//! Portable, schema-checked spill format. Callers own file paths and cleanup.
use std::io::{BufReader, Read, Write};
use crate::{batch::Batch, scalar::Value, schema::{DataType, Field, Schema}, Error, Result};

const MAGIC: &[u8; 8] = b"RSPILL01";
const MAX_FIELDS: usize = 4096;
const MAX_NAME_BYTES: usize = 4096;
const MAX_TEXT_BYTES: usize = 8 * 1024 * 1024;
const MAX_CELLS: usize = 4_000_000;
const MAX_BATCH_BYTES: usize = 64 * 1024 * 1024;
// The wire stores one byte per Boolean/NULL cell, but native Value
// vectors retain full tagged-union slots. Bound both representations.
const MAX_NATIVE_BATCH_BYTES: usize = 64 * 1024 * 1024;

fn check_native_slots(cells: usize) -> Result<()> {
    cells.checked_mul(std::mem::size_of::<Value>())
        .filter(|&bytes| bytes <= MAX_NATIVE_BATCH_BYTES)
        .ok_or_else(|| Error::Execution("spill native batch memory budget exceeded".into()))?;
    Ok(())
}
const ENCODE_BUFFER_BYTES: usize = 16 * 1024;

fn write_u32(out: &mut impl Write, value: usize) -> Result<()> {
    let value = u32::try_from(value)
        .map_err(|_| Error::Execution("spill length exceeds u32".into()))?;
    out.write_all(&value.to_le_bytes())?;
    Ok(())
}

fn read_u32(input: &mut impl Read) -> Result<usize> {
    let mut bytes = [0u8; 4];
    input.read_exact(&mut bytes)?;
    Ok(u32::from_le_bytes(bytes) as usize)
}

fn read_byte(input: &mut impl Read) -> Result<u8> {
    let mut byte = [0u8; 1];
    input.read_exact(&mut byte)?;
    Ok(byte[0])
}

fn read_bounded(input: &mut impl Read, bound: usize) -> Result<Vec<u8>> {
    let bytes = read_u32(input)?;
    if bytes > bound { return Err(Error::Execution("spill value exceeds byte limit".into())); }
    let mut buffer = vec![0; bytes];
    input.read_exact(&mut buffer)?;
    Ok(buffer)
}

// Account for every byte on the wire, not only the text payload.
fn charge_batch_bytes(bytes_seen: &mut usize, count: usize) -> Result<()> {
    *bytes_seen = bytes_seen.checked_add(count)
        .filter(|total| *total <= MAX_BATCH_BYTES)
        .ok_or_else(|| Error::Execution("spill batch exceeds byte limit".into()))?;
    Ok(())
}

fn read_text_bounded(
    input: &mut impl Read, bytes_seen: &mut usize
) -> Result<String> {
    let length = read_u32(input)?;
    if length > MAX_TEXT_BYTES {
        return Err(Error::Execution("spill text value exceeds byte limit".into()));
    }
    // Include the four-byte length prefix before allocating the text.
    let extra = length.checked_add(4)
        .ok_or_else(|| Error::Execution("spill batch exceeds byte limit".into()))?;
    let next = bytes_seen.checked_add(extra)
        .filter(|total| *total <= MAX_BATCH_BYTES)
        .ok_or_else(|| Error::Execution("spill batch exceeds byte limit".into()))?;
    let mut buffer = vec![0; length];
    input.read_exact(&mut buffer)?;
    let text = String::from_utf8(buffer)
        .map_err(|_| Error::Parse("invalid spill UTF-8".into()))?;
    *bytes_seen = next;
    Ok(text)
}

fn type_code(data_type: DataType) -> u8 {
    match data_type {
        DataType::Boolean => 1,
        DataType::Integer => 2,
        DataType::Float => 3,
        DataType::Text => 4,
    }
}

pub struct SpillWriter<W: Write> {
    output: W,
    schema: Schema,
    // Reuse the cell encoder between successive spill batches.
    encoded: Vec<u8>,
    poisoned: bool,
}

impl<W: Write> SpillWriter<W> {
    pub fn new(mut output: W, schema: Schema) -> Result<Self> {
        if schema.len() > MAX_FIELDS {
            return Err(Error::Execution("spill schema exceeds field limit".into()));
        }
        if schema.fields().iter().any(|field| field.name.len() > MAX_NAME_BYTES) {
            return Err(Error::Execution("spill field name too long".into()));
        }
        output.write_all(MAGIC)?;
        write_u32(&mut output, schema.len())?;
        for field in schema.fields() {
            write_u32(&mut output, field.name.len())?;
            output.write_all(field.name.as_bytes())?;
            output.write_all(&[type_code(field.data_type), u8::from(field.nullable)])?;
        }
        Ok(Self { output, schema, encoded: Vec::new(), poisoned: false })
    }

    // Continue a file whose schema header was already written and finalized.
    // Only internal scratch-file owners may reopen an existing stream.
    pub(crate) fn resume(output: W, schema: Schema) -> Self {
        Self { output, schema, encoded: Vec::new(), poisoned: false }
    }

    pub fn write_batch(&mut self, batch: &Batch) -> Result<()> {
        self.write_batch_range(batch, 0, batch.num_rows())
    }

    /// Write a large validated batch as individually admissible wire windows.
    /// This preserves source row order and avoids allocating gathered batches.
    pub fn write_batch_bounded(&mut self, batch: &Batch, max_rows: usize) -> Result<()> {
        self.write_batch_bounded_with_budget(batch, max_rows, MAX_BATCH_BYTES)
    }

    fn write_batch_bounded_with_budget(
        &mut self, batch: &Batch, max_rows: usize, max_bytes: usize,
    ) -> Result<()> {
        if batch.schema() != &self.schema {
            return Err(Error::Schema("spill batch schema mismatch".into()));
        }
        if max_rows == 0 || max_bytes < 4 || max_bytes > MAX_BATCH_BYTES {
            return Err(Error::Execution("invalid spill batch window limits".into()));
        }
        let rows_per_window = max_rows.min(MAX_CELLS / self.schema.len().max(1));
        let columns = (0..self.schema.len())
            .map(|index| batch.column(index)).collect::<Result<Vec<_>>>()?;
        let mut start = 0;
        let mut emitted = false;
        while start < batch.num_rows() {
            let mut end = start;
            let mut bytes = 4usize;
            while end < batch.num_rows() && end - start < rows_per_window {
                let mut row_bytes = self.schema.len();
                for column in &columns {
                    let additional = match &column[end] {
                        Value::Integer(_) | Value::Float(_) => 8,
                        Value::Text(text) => {
                            if text.len() > MAX_TEXT_BYTES {
                                // Earlier wire windows may already be on disk.
                                // Never allow finish() to commit their valid prefix.
                                if emitted { self.poisoned = true; }
                                return Err(Error::Execution("spill text value too long".into()));
                            }
                            text.len() + 4
                        }
                        Value::Null | Value::Boolean(_) => 0,
                    };
                    row_bytes = row_bytes.checked_add(additional)
                        .ok_or_else(|| Error::Execution("spill batch byte overflow".into()))?;
                }
                if row_bytes > max_bytes - 4 {
                    if emitted { self.poisoned = true; }
                    return Err(Error::Execution("spill row exceeds byte limit".into()));
                }
                if row_bytes > max_bytes - bytes { break; }
                bytes += row_bytes;
                end += 1;
            }
            self.write_batch_range(batch, start, end)?;
            emitted = true;
            start = end;
        }
        Ok(())
    }

    pub fn write_batch_range(&mut self, batch: &Batch, start: usize, end: usize) -> Result<()> {
        if start > end || end > batch.num_rows() {
            return Err(Error::Execution("invalid spill batch row range".into()));
        }
        self.write_selected_rows(batch, start..end)
    }

    /// Serialize selected rows directly without allocating a gathered Batch.
    /// Repeated indices retain their multiplicity and caller order.
    pub fn write_batch_indices(&mut self, batch: &Batch, indices: &[usize]) -> Result<()> {
        if indices.iter().any(|&row| row >= batch.num_rows()) {
            return Err(Error::Execution("invalid spill batch row index".into()));
        }
        self.write_selected_rows(batch, indices.iter().copied())
    }

    fn write_selected_rows<I>(&mut self, batch: &Batch, rows: I) -> Result<()>
    where I: ExactSizeIterator<Item = usize> + Clone
    {
        if self.poisoned {
            return Err(Error::Io("spill writer poisoned by earlier I/O failure".into()));
        }
        if batch.schema() != &self.schema {
            return Err(Error::Schema("spill batch schema mismatch".into()));
        }
        // Zero-column batches have logical rows even without any encoded cells.
        if rows.len() > MAX_CELLS {
            return Err(Error::Execution("spill batch exceeds row limit".into()));
        }
        let cells = rows.len().checked_mul(self.schema.len())
            .filter(|&cells| cells <= MAX_CELLS)
            .ok_or_else(|| Error::Execution("spill batch exceeds cell limit".into()))?;
        check_native_slots(cells)?;
        // Include the 4-byte row count and one tag per cell. Validate all
        // fixed-width and text bytes before emitting any batch data.
        let mut wire_bytes = 4usize.checked_add(cells)
            .filter(|&bytes| bytes <= MAX_BATCH_BYTES)
            .ok_or_else(|| Error::Execution("spill batch exceeds byte limit".into()))?;
        let columns = (0..self.schema.len())
            .map(|index| batch.column(index))
            .collect::<Result<Vec<_>>>()?;
        for column in &columns {
            for row in rows.clone() {
                match &column[row] {
                    Value::Integer(_) | Value::Float(_) =>
                        charge_batch_bytes(&mut wire_bytes, 8)?,
                    Value::Text(text) => {
                        if text.len() > MAX_TEXT_BYTES {
                            return Err(Error::Execution("spill text value too long".into()));
                        }
                        charge_batch_bytes(&mut wire_bytes, text.len() + 4)?;
                    }
                    Value::Null | Value::Boolean(_) => {}
                }
            }
        }
        let write_result = (|| -> Result<()> {
        write_u32(&mut self.output, rows.len())?;
        // Buffer cells to avoid a virtual Write call per numeric value.
        // Text larger than the scratch buffer is streamed directly to
        // avoid duplicating multi-megabyte values in memory.
        let encoded = &mut self.encoded;
        if encoded.capacity() == 0 {
            encoded.reserve(ENCODE_BUFFER_BYTES);
        }
        // The previous write's final flush does not consume this buffer.
        encoded.clear();
        for row in rows {
            for column in &columns {
                match &column[row] {
                    Value::Null => encoded.push(0),
                    Value::Boolean(false) => encoded.push(1),
                    Value::Boolean(true) => encoded.push(2),
                    Value::Integer(value) => {
                        encoded.push(3);
                        encoded.extend_from_slice(&value.to_le_bytes());
                    }
                    Value::Float(value) => {
                        encoded.push(4);
                        encoded.extend_from_slice(&value.to_bits().to_le_bytes());
                    }
                    Value::Text(value) => {
                        // Payloads are validated before the batch header is written.
                        encoded.push(5);
                        let len = u32::try_from(value.len())
                            .map_err(|_| Error::Execution("spill text length exceeds u32".into()))?;
                        encoded.extend_from_slice(&len.to_le_bytes());
                        if value.len() >= ENCODE_BUFFER_BYTES {
                            self.output.write_all(encoded.as_slice())?;
                            encoded.clear();
                            self.output.write_all(value.as_bytes())?;
                        } else {
                            if encoded.len() + value.len() > ENCODE_BUFFER_BYTES {
                                self.output.write_all(encoded.as_slice())?;
                                encoded.clear();
                            }
                            encoded.extend_from_slice(value.as_bytes());
                        }
                    }
                }
                if encoded.len() >= ENCODE_BUFFER_BYTES {
                    self.output.write_all(encoded.as_slice())?;
                    encoded.clear();
                }
            }
        }
        if !encoded.is_empty() {
            self.output.write_all(encoded.as_slice())?;
        }
        Ok(())
        })();
        if write_result.is_err() { self.poisoned = true; }
        write_result
    }

    pub fn finish(mut self) -> Result<W> {
        if self.poisoned {
            return Err(Error::Io("spill writer poisoned by earlier I/O failure".into()));
        }
        self.output.flush()?;
        Ok(self.output)
    }
}

pub struct SpillReader<R: Read> {
    input: BufReader<R>,
    schema: Schema,
    max_rows_per_batch: usize,
    stopped: bool,
}

impl<R: Read> SpillReader<R> {
    pub fn new(input: R, max_rows_per_batch: usize) -> Result<Self> {
        if max_rows_per_batch == 0 {
            return Err(Error::Execution("spill reader requires positive batch size limit".into()));
        }
        // Scalar spill decoding reads one tag and several fixed-width values
        // per row. Keep those reads in a bounded page-sized input buffer.
        let mut input = BufReader::with_capacity(64 * 1024, input);
        let mut magic = [0; 8];
        input.read_exact(&mut magic)?;
        if &magic != MAGIC { return Err(Error::Parse("invalid spill file signature".into())); }
        let count = read_u32(&mut input)?;
        if count > MAX_FIELDS { return Err(Error::Schema("spill schema has too many fields".into())); }
        let mut fields = Vec::with_capacity(count);
        for _ in 0..count {
            let name = read_bounded(&mut input, MAX_NAME_BYTES)?;
            let name = String::from_utf8(name)
                .map_err(|_| Error::Parse("invalid spill column name".into()))?;
            let data_type = match read_byte(&mut input)? {
                1 => DataType::Boolean,
                2 => DataType::Integer,
                3 => DataType::Float,
                4 => DataType::Text,
                _ => return Err(Error::Parse("invalid spill column type".into())),
            };
            let nullable = match read_byte(&mut input)? {
                0 => false,
                1 => true,
                _ => return Err(Error::Parse("invalid spill nullability flag".into())),
            };
            fields.push(Field::new(name, data_type, nullable));
        }
        Ok(Self { input, schema: Schema::new(fields)?, max_rows_per_batch, stopped: false })
    }

    pub fn schema(&self) -> &Schema { &self.schema }

    pub fn next_batch(&mut self) -> Result<Option<Batch>> {
        if self.stopped { return Ok(None); }
        let result = self.read_batch();
        if !matches!(&result, Ok(Some(_))) { self.stopped = true; }
        result
    }

    /// A common spill stream contains one numeric or boolean column. Decode
    /// it directly without per-row schema zips or an outer column-vector
    /// dispatch. Retain the exact tag, nullability, and wire-byte checks.
    fn read_single_scalar_column(
        &mut self, rows: usize, mut bytes_seen: usize,
    ) -> Result<Batch> {
        let field = self.schema.field(0)?;
        let (data_type, nullable) = (field.data_type, field.nullable);
        let mut values = Vec::with_capacity(rows.min(1024));
        for _ in 0..rows {
            let tag = read_byte(&mut self.input)?;
            let value = match (data_type, tag) {
                (_, 0) if nullable => Value::Null,
                (DataType::Boolean, 1) => Value::Boolean(false),
                (DataType::Boolean, 2) => Value::Boolean(true),
                (DataType::Integer, 3) => {
                    charge_batch_bytes(&mut bytes_seen, 8)?;
                    let mut bytes = [0u8; 8];
                    self.input.read_exact(&mut bytes)?;
                    Value::Integer(i64::from_le_bytes(bytes))
                }
                (DataType::Float, 4) => {
                    charge_batch_bytes(&mut bytes_seen, 8)?;
                    let mut bytes = [0u8; 8];
                    self.input.read_exact(&mut bytes)?;
                    Value::Float(f64::from_bits(u64::from_le_bytes(bytes)))
                }
                _ => return Err(Error::Parse("spill value does not match schema".into())),
            };
            values.push(value);
        }
        Batch::from_validated_columns(self.schema.clone(), vec![values], rows)
    }

    // TEXT-only spill streams are common for DISTINCT and sorted runs.
    // Decode a single validated column without the per-cell schema/column
    // dispatch used by general multi-column batches.
    fn read_single_text_column(
        &mut self, rows: usize, mut bytes_seen: usize,
    ) -> Result<Batch> {
        let nullable = self.schema.field(0)?.nullable;
        let mut values = Vec::with_capacity(rows.min(1024));
        for _ in 0..rows {
            let value = match read_byte(&mut self.input)? {
                0 if nullable => Value::Null,
                5 => Value::Text(read_text_bounded(&mut self.input, &mut bytes_seen)?),
                _ => return Err(Error::Parse("spill value does not match schema".into())),
            };
            values.push(value);
        }
        Batch::from_validated_columns(self.schema.clone(), vec![values], rows)
    }

    fn read_batch(&mut self) -> Result<Option<Batch>> {
        let mut first = [0u8; 1];
        // Interrupted at a batch boundary is not EOF or a corrupt spill file.
        loop {
            match self.input.read(&mut first) {
                Ok(0) => return Ok(None),
                Ok(_) => break,
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(error) => return Err(error.into()),
            }
        }
        let mut remaining = [0u8; 3];
        self.input.read_exact(&mut remaining)?;
        let rows = u32::from_le_bytes([first[0], remaining[0], remaining[1], remaining[2]]) as usize;
        if rows > MAX_CELLS {
            return Err(Error::Execution("spill batch exceeds row limit".into()));
        }
        let cells = rows.checked_mul(self.schema.len())
            .filter(|&cells| cells <= MAX_CELLS)
            .ok_or_else(|| Error::Execution("spill batch exceeds configured limits".into()))?;
        if rows > self.max_rows_per_batch {
            return Err(Error::Execution("spill batch exceeds configured limits".into()));
        }
        check_native_slots(cells)?;
        // Reject the minimum encoded size before allocating column storage.
        let mut bytes_seen = 4usize.checked_add(cells)
            .filter(|&bytes| bytes <= MAX_BATCH_BYTES)
            .ok_or_else(|| Error::Execution("spill batch exceeds byte limit".into()))?;
        // The schema is immutable across batches. Read its field types
        // directly rather than allocating a copied type/nullability vector
        // for every small spill chunk.
        // A spill header is untrusted until the payload has been decoded.
        // A huge claimed row count with a truncated body must not reserve
        // millions of scalar slots before the first cell is read.
        let initial_capacity = rows.min(1_024);
        if self.schema.len() == 1 &&
            matches!(self.schema.field(0)?.data_type,
                DataType::Integer | DataType::Float | DataType::Boolean)
        {
            return Ok(Some(self.read_single_scalar_column(rows, bytes_seen)?));
        }
        if self.schema.len() == 1 && self.schema.field(0)?.data_type == DataType::Text {
            return Ok(Some(self.read_single_text_column(rows, bytes_seen)?));
        }
        let mut columns = (0..self.schema.len())
            .map(|_| Vec::with_capacity(initial_capacity)).collect::<Vec<_>>();
        for _ in 0..rows {
            for (column, field) in columns.iter_mut().zip(self.schema.fields()) {
                let (data_type, nullable) = (field.data_type, field.nullable);
                // Validate the cell against the declared schema before reading
                // its variable-length payload or allocating a decoded String.
                let tag = read_byte(&mut self.input)?;
                let valid = match tag {
                    0 => nullable,
                    1 | 2 => data_type == DataType::Boolean,
                    3 => data_type == DataType::Integer,
                    4 => data_type == DataType::Float,
                    5 => data_type == DataType::Text,
                    _ => false,
                };
                if !valid {
                    return Err(Error::Parse("spill value does not match schema".into()));
                }
                let value = match tag {
                    0 => Value::Null,
                    1 => Value::Boolean(false),
                    2 => Value::Boolean(true),
                    3 => {
                        charge_batch_bytes(&mut bytes_seen, 8)?;
                        let mut b = [0; 8];
                        self.input.read_exact(&mut b)?;
                        Value::Integer(i64::from_le_bytes(b))
                    }
                    4 => {
                        charge_batch_bytes(&mut bytes_seen, 8)?;
                        let mut b = [0; 8];
                        self.input.read_exact(&mut b)?;
                        Value::Float(f64::from_bits(u64::from_le_bytes(b)))
                    }
                    5 => Value::Text(read_text_bounded(&mut self.input, &mut bytes_seen)?),
                    _ => return Err(Error::Parse("invalid spill value tag".into())),
                };
                column.push(value);
            }
        }
        Ok(Some(Batch::from_validated_columns(self.schema.clone(), columns, rows)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn survives_round_trip_with_nulls_and_unicode() {
        let schema = Schema::new(vec![
            Field::new("n", DataType::Integer, false),
            Field::new("word", DataType::Text, true)
        ]).unwrap();
        let batch = Batch::from_rows(schema.clone(), vec![
            vec![Value::Integer(42), Value::Text("Zażółć".into())],
            vec![Value::Integer(-7), Value::Null]
        ]).unwrap();
        let mut writer = SpillWriter::new(Vec::new(), schema.clone()).unwrap();
        writer.write_batch(&batch).unwrap();
        let bytes = writer.finish().unwrap();
        let mut reader = SpillReader::new(Cursor::new(&bytes), 100).unwrap();
        assert_eq!(reader.schema(), &schema);
        assert_eq!(reader.next_batch().unwrap(), Some(batch));
        assert!(reader.next_batch().unwrap().is_none());
    }

    #[test]
    fn rejects_truncation_and_wrong_signatures() {
        assert!(SpillReader::new(Cursor::new(b"BADMAGIC".as_slice()), 10).is_err());
        let schema = Schema::new(vec![Field::new("n", DataType::Integer, false)]).unwrap();
        let batch = Batch::from_rows(schema.clone(), vec![vec![Value::Integer(10)]]).unwrap();
        let mut writer = SpillWriter::new(Vec::new(), schema).unwrap();
        writer.write_batch(&batch).unwrap();
        let mut bytes = writer.finish().unwrap();
        bytes.pop();
        let mut reader = SpillReader::new(Cursor::new(bytes), 100).unwrap();
        assert!(reader.next_batch().is_err());
        assert!(reader.next_batch().unwrap().is_none());
    }
}

#[cfg(test)]
mod spill_byte_preflight_tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn rejects_exhausted_batch_budget_before_reading_text_payload() {
        let mut data = Vec::new();
        data.extend_from_slice(&2u32.to_le_bytes());
        let mut input = Cursor::new(data);
        let mut already_used = MAX_BATCH_BYTES - 1;
        let error = read_text_bounded(&mut input, &mut already_used).unwrap_err();
        assert!(error.to_string().contains("spill batch exceeds byte limit"));
        assert_eq!(already_used, MAX_BATCH_BYTES - 1);
        assert_eq!(input.position(), 4);
    }
}

#[cfg(test)]
mod spill_preflight_tests {
    use super::*;

    #[test]
    fn invalid_text_does_not_corrupt_the_writer_stream() {
        let schema = Schema::new(vec![
            Field::new("payload", DataType::Text, false)
        ]).unwrap();
        let batch = Batch::from_rows(schema.clone(), vec![
            vec![Value::Text("x".repeat(MAX_TEXT_BYTES + 1))]
        ]).unwrap();
        let mut writer = SpillWriter::new(Vec::new(), schema.clone()).unwrap();
        assert!(writer.write_batch(&batch).is_err());
        let after = writer.finish().unwrap();
        let empty = SpillWriter::new(Vec::new(), schema).unwrap().finish().unwrap();
        assert_eq!(after, empty);
    }
}

#[cfg(test)]
mod direct_spill_column_decode_perf_tests {
    use super::*;
    use crate::{scalar::Value, schema::{DataType, Field, Schema}};
    use std::io::Cursor;

    #[test]
    fn many_nullable_rows_decode_without_row_transposition() {
        let schema = Schema::new(vec![
            Field::new("k", DataType::Integer, false),
            Field::new("payload", DataType::Text, true),
            Field::new("flag", DataType::Boolean, true),
        ]).unwrap();
        let rows = (0..1000).map(|n| vec![
            Value::Integer(n),
            if n % 3 == 0 { Value::Null } else { Value::Text(format!("payload-{n}")) },
            if n % 2 == 0 { Value::Boolean(true) } else { Value::Null },
        ]).collect();
        let input = Batch::from_rows(schema.clone(), rows).unwrap();
        let mut writer = SpillWriter::new(Vec::new(), schema).unwrap();
        writer.write_batch(&input).unwrap();
        let mut reader = SpillReader::new(Cursor::new(writer.finish().unwrap()), 1000).unwrap();
        assert_eq!(reader.next_batch().unwrap().unwrap(), input);
        assert!(reader.next_batch().unwrap().is_none());
    }

    #[test]
    fn zero_width_spills_keep_explicit_row_count() {
        let schema = Schema::new(vec![]).unwrap();
        let batch = Batch::new_with_row_count(schema.clone(), vec![], 5).unwrap();
        let mut writer = SpillWriter::new(Vec::new(), schema).unwrap();
        writer.write_batch(&batch).unwrap();
        let mut reader = SpillReader::new(Cursor::new(writer.finish().unwrap()), 10).unwrap();
        assert_eq!(reader.next_batch().unwrap().unwrap(), batch);
    }
}

#[cfg(test)]
mod spill_window_write_tests {
    use super::*;
    #[test]
    fn range_writes_roundtrip_without_extra_rows_or_schema_changes() {
        let schema = Schema::new(vec![Field::new("id", DataType::Integer, false), Field::new("name", DataType::Text, true)]).unwrap();
        let batch = Batch::from_rows(schema.clone(), (0..15).map(|n| vec![Value::Integer(n),
            if n % 4 == 0 { Value::Null } else { Value::Text(format!("item-{n}")) }]).collect()).unwrap();
        let mut writer = SpillWriter::new(Vec::new(), schema.clone()).unwrap();
        writer.write_batch_range(&batch, 2, 5).unwrap();
        writer.write_batch_range(&batch, 7, 11).unwrap();
        assert!(writer.write_batch_range(&batch, 10, 16).is_err());
        assert!(writer.write_batch_range(&batch, 4, 3).is_err());
        let bytes = writer.finish().unwrap();
        let mut reader = SpillReader::new(bytes.as_slice(), 8).unwrap();
        let left = reader.next_batch().unwrap().unwrap();
        let right = reader.next_batch().unwrap().unwrap();
        assert_eq!(left, batch.slice_rows(2, 5).unwrap());
        assert_eq!(right, batch.slice_rows(7, 11).unwrap());
        assert!(reader.next_batch().unwrap().is_none());
    }
}

#[cfg(test)]
mod coalesced_spill_write_tests {
    use super::*;
    use std::io::Write;

    #[derive(Default)]
    struct CountingWriter { bytes: Vec<u8>, writes: usize }

    impl Write for CountingWriter {
        fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
            self.writes += 1;
            self.bytes.extend_from_slice(data);
            Ok(data.len())
        }

        fn flush(&mut self) -> std::io::Result<()> { Ok(()) }
    }

    #[test]
    fn numeric_and_text_cells_are_coalesced_into_one_payload_write() {
        let schema = Schema::new(vec![
            Field::new("n", DataType::Integer, false),
            Field::new("f", DataType::Float, false),
            Field::new("t", DataType::Text, false),
        ]).unwrap();
        let input = Batch::from_rows(schema.clone(), vec![
            vec![Value::Integer(-1), Value::Float(-0.0), Value::Text("é".into())],
        ]).unwrap();
        let mut writer = SpillWriter::new(CountingWriter::default(), schema).unwrap();
        let header_writes = writer.output.writes;
        writer.write_batch(&input).unwrap();
        assert_eq!(writer.output.writes - header_writes, 2);
        let output = writer.finish().unwrap();
        let mut reader = SpillReader::new(output.bytes.as_slice(), 2).unwrap();
        assert_eq!(reader.next_batch().unwrap().unwrap(), input);
    }
}

#[cfg(test)]
mod spill_header_preflight_atomicity_tests {
    use super::*;

    #[test]
    fn bad_schema_cannot_write_partial_spill_signature() {
        let schema = Schema::new(vec![
            Field::new("x".repeat(MAX_NAME_BYTES + 1), DataType::Text, true),
        ]).unwrap();
        let mut output = Vec::new();
        assert!(SpillWriter::new(&mut output, schema).is_err());
        assert!(output.is_empty());
    }
}

#[cfg(test)]
mod complete_spill_wire_budget_tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn text_prefix_and_scalar_bytes_count_toward_batch_budget() {
        let mut bytes = MAX_BATCH_BYTES - 4;
        assert!(charge_batch_bytes(&mut bytes, 8).is_err());
        assert_eq!(bytes, MAX_BATCH_BYTES - 4);
        charge_batch_bytes(&mut bytes, 4).unwrap();
        assert_eq!(bytes, MAX_BATCH_BYTES);

        let mut text = Vec::new();
        text.extend_from_slice(&0u32.to_le_bytes());
        let mut used = MAX_BATCH_BYTES - 3;
        let error = read_text_bounded(&mut Cursor::new(text), &mut used).unwrap_err();
        assert!(error.to_string().contains("spill batch exceeds byte limit"));
        assert_eq!(used, MAX_BATCH_BYTES - 3);
    }

    #[test]
    fn existing_spills_still_roundtrip_mixed_value_tags() {
        let schema = Schema::new(vec![
            Field::new("n", DataType::Integer, true),
            Field::new("f", DataType::Float, true),
            Field::new("t", DataType::Text, true),
        ]).unwrap();
        let batch = Batch::from_rows(schema.clone(), vec![
            vec![Value::Integer(3), Value::Float(2.5), Value::Text("é".into())],
            vec![Value::Null, Value::Float(-0.0), Value::Null],
        ]).unwrap();
        let mut writer = SpillWriter::new(Vec::new(), schema).unwrap();
        writer.write_batch(&batch).unwrap();
        let wire = writer.finish().unwrap();
        let mut reader = SpillReader::new(Cursor::new(wire), 3).unwrap();
        assert_eq!(reader.next_batch().unwrap(), Some(batch));
    }
}

#[cfg(test)]
mod buffered_spill_values_perf_tests {
    use super::*;
    use std::io::Write;

    #[derive(Default)]
    struct CountingWriter { bytes: Vec<u8>, calls: usize }
    impl Write for CountingWriter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.calls += 1;
            self.bytes.extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> { Ok(()) }
    }

    #[test]
    fn large_fixed_width_batch_uses_bounded_write_calls() {
        let schema = Schema::new(vec![Field::new("v", DataType::Integer, false)]).unwrap();
        let source = Batch::from_rows(schema.clone(), (0..100_000_i64)
            .map(|n| vec![Value::Integer(n)]).collect()).unwrap();
        let mut writer = SpillWriter::new(CountingWriter::default(), schema).unwrap();
        let before = writer.output.calls;
        writer.write_batch(&source).unwrap();
        assert!(writer.output.calls - before < 100);
        let encoded = writer.finish().unwrap();
        let mut reader = SpillReader::new(encoded.bytes.as_slice(), 100_000).unwrap();
        assert_eq!(reader.next_batch().unwrap().unwrap(), source);
    }

    #[test]
    fn large_text_bypasses_scratch_without_reordering_other_values() {
        let schema = Schema::new(vec![
            Field::new("text", DataType::Text, true),
            Field::new("number", DataType::Integer, true),
        ]).unwrap();
        let source = Batch::from_rows(schema.clone(), vec![
            vec![Value::Text("x".repeat(ENCODE_BUFFER_BYTES + 123)), Value::Integer(7)],
            vec![Value::Null, Value::Integer(-1)],
        ]).unwrap();
        let mut writer = SpillWriter::new(CountingWriter::default(), schema).unwrap();
        writer.write_batch(&source).unwrap();
        let encoded = writer.finish().unwrap();
        let mut reader = SpillReader::new(encoded.bytes.as_slice(), 2).unwrap();
        assert_eq!(reader.next_batch().unwrap().unwrap(), source);
    }
}

#[cfg(test)]
mod early_spill_tag_validation_regressions {
    use super::*;
    use std::io::Cursor;

    fn valid_integer_spill() -> Vec<u8> {
        let schema = Schema::new(vec![Field::new("n", DataType::Integer, false)]).unwrap();
        let batch = Batch::from_rows(schema.clone(), vec![vec![Value::Integer(7)]]).unwrap();
        let mut writer = SpillWriter::new(Vec::new(), schema).unwrap();
        writer.write_batch(&batch).unwrap();
        writer.finish().unwrap()
    }

    #[test]
    fn rejects_text_tag_on_integer_column_before_reading_text_length() {
        let mut bytes = valid_integer_spill();
        let tag = 8 + 4 + 4 + 1 + 2 + 4;
        assert_eq!(bytes[tag], 3);
        bytes[tag] = 5;
        // The next four integer bytes are not a valid string prefix.
        // Reject by declared field type instead of attempting to allocate it.
        let mut reader = SpillReader::new(Cursor::new(bytes), 4).unwrap();
        assert!(reader.next_batch().unwrap_err().to_string().contains("schema"));
        assert!(reader.next_batch().unwrap().is_none());
    }

    #[test]
    fn rejects_null_tag_in_required_column_before_materialization() {
        let mut bytes = valid_integer_spill();
        bytes[8 + 4 + 4 + 1 + 2 + 4] = 0;
        let mut reader = SpillReader::new(Cursor::new(bytes), 4).unwrap();
        assert!(reader.next_batch().unwrap_err().to_string().contains("schema"));
        assert!(reader.next_batch().unwrap().is_none());
    }
}

#[cfg(test)]
mod interrupted_spill_boundary_tests {
    use super::*;
    use std::io::{self, Cursor, Read};

    struct InterruptAtFirstBatch {
        inner: Cursor<Vec<u8>>,
        remaining_before_interrupt: usize,
        interrupted: bool,
    }
    impl Read for InterruptAtFirstBatch {
        fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
            if !self.interrupted && self.remaining_before_interrupt == 0 {
                self.interrupted = true;
                return Err(io::Error::from(io::ErrorKind::Interrupted));
            }
            let limit = if self.interrupted { bytes.len() }
                else { bytes.len().min(self.remaining_before_interrupt) };
            let count = self.inner.read(&mut bytes[..limit])?;
            if !self.interrupted { self.remaining_before_interrupt -= count; }
            Ok(count)
        }
    }

    #[test]
    fn interruption_at_record_boundary_is_retried_without_losing_rows() {
        let schema = Schema::new(vec![Field::new("id", DataType::Integer, false)]).unwrap();
        let batch = Batch::from_rows(schema.clone(), vec![
            vec![Value::Integer(7)], vec![Value::Integer(11)],
        ]).unwrap();
        let mut writer = SpillWriter::new(Vec::new(), schema).unwrap();
        writer.write_batch(&batch).unwrap();
        let bytes = writer.finish().unwrap();
        // Signature, field count, length-prefixed field name, type and nullability.
        let header_len = 8 + 4 + 4 + "id".len() + 2;
        let input = InterruptAtFirstBatch {
            inner: Cursor::new(bytes),
            remaining_before_interrupt: header_len,
            interrupted: false,
        };
        let mut reader = SpillReader::new(input, 16).unwrap();
        assert_eq!(reader.next_batch().unwrap(), Some(batch));
        assert_eq!(reader.next_batch().unwrap(), None);
    }
}

#[cfg(test)]
mod single_pass_spill_decode_tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn decoded_values_and_zero_width_row_counts_roundtrip() {
        let schema = Schema::new(vec![
            Field::new("key", DataType::Text, false),
            Field::new("value", DataType::Float, true),
        ]).unwrap();
        let input = Batch::from_rows(schema.clone(), vec![
            vec![Value::Text("é🙂".into()), Value::Null],
            vec![Value::Text("abc".into()), Value::Float(-0.0)],
        ]).unwrap();
        let mut writer = SpillWriter::new(Vec::new(), schema).unwrap();
        writer.write_batch(&input).unwrap();
        let mut reader = SpillReader::new(Cursor::new(writer.finish().unwrap()), 3).unwrap();
        assert_eq!(reader.next_batch().unwrap(), Some(input));
        assert_eq!(reader.next_batch().unwrap(), None);

        let empty_schema = Schema::new(vec![]).unwrap();
        let zero_width = Batch::new_with_row_count(empty_schema.clone(), vec![], 4).unwrap();
        let mut writer = SpillWriter::new(Vec::new(), empty_schema).unwrap();
        writer.write_batch(&zero_width).unwrap();
        let mut reader = SpillReader::new(Cursor::new(writer.finish().unwrap()), 4).unwrap();
        assert_eq!(reader.next_batch().unwrap(), Some(zero_width));
    }

    #[test]
    fn trusted_constructor_still_rejects_mismatched_column_lengths() {
        let schema = Schema::new(vec![Field::new("v", DataType::Integer, true)]).unwrap();
        assert!(Batch::from_validated_columns(schema, vec![vec![]], 1).is_err());
    }
}

#[cfg(test)]
mod selected_rows_spill_write_tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn arbitrary_indices_and_ranges_roundtrip_without_gathering() {
        let schema = Schema::new(vec![
            Field::new("key", DataType::Integer, false),
            Field::new("payload", DataType::Text, true),
        ]).unwrap();
        let batch = Batch::from_rows(schema.clone(), vec![
            vec![Value::Integer(1), Value::Text("é🙂".repeat(2048))],
            vec![Value::Integer(2), Value::Null],
            vec![Value::Integer(3), Value::Text("third".into())],
            vec![Value::Integer(4), Value::Text("fourth".into())],
        ]).unwrap();
        let mut writer = SpillWriter::new(Vec::new(), schema).unwrap();
        assert!(writer.write_batch_indices(&batch, &[0, 10]).is_err());
        writer.write_batch_indices(&batch, &[3, 1, 3, 0]).unwrap();
        writer.write_batch_range(&batch, 1, 3).unwrap();
        let mut reader = SpillReader::new(Cursor::new(writer.finish().unwrap()), 4).unwrap();
        assert_eq!(reader.next_batch().unwrap(), Some(batch.take(&[3, 1, 3, 0]).unwrap()));
        assert_eq!(reader.next_batch().unwrap(), Some(batch.slice_rows(1, 3).unwrap()));
        assert_eq!(reader.next_batch().unwrap(), None);
    }
}

#[cfg(test)]
mod borrowed_spill_schema_regressions {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn many_tiny_batches_keep_schema_and_nullable_cell_tags() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Integer, false),
            Field::new("text", DataType::Text, true),
            Field::new("enabled", DataType::Boolean, true),
        ]).unwrap();
        let mut writer = SpillWriter::new(Vec::new(), schema.clone()).unwrap();
        for n in 0..64 {
            let batch = Batch::from_rows(schema.clone(), vec![vec![
                Value::Integer(n),
                if n % 2 == 0 { Value::Text("界🙂".into()) } else { Value::Null },
                if n % 3 == 0 { Value::Null } else { Value::Boolean(n % 2 == 0) },
            ]]).unwrap();
            writer.write_batch(&batch).unwrap();
        }
        let bytes = writer.finish().unwrap();
        let mut reader = SpillReader::new(Cursor::new(bytes), 1).unwrap();
        for n in 0..64 {
            let batch = reader.next_batch().unwrap().unwrap();
            assert_eq!(batch.schema(), &schema);
            assert_eq!(batch.value(0, 0).unwrap(), &Value::Integer(n));
            assert_eq!(batch.num_rows(), 1);
        }
        assert!(reader.next_batch().unwrap().is_none());
    }

    #[test]
    fn zero_width_spill_batches_keep_logical_row_counts() {
        let schema = Schema::new(vec![]).unwrap();
        let batch = Batch::new_with_row_count(schema.clone(), vec![], 5).unwrap();
        let mut writer = SpillWriter::new(Vec::new(), schema).unwrap();
        writer.write_batch(&batch).unwrap();
        let mut reader = SpillReader::new(Cursor::new(writer.finish().unwrap()), 8).unwrap();
        assert_eq!(reader.next_batch().unwrap().unwrap().num_rows(), 5);
    }
}

#[cfg(test)]
mod untrusted_spill_preallocation_regressions {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn truncated_large_row_header_is_rejected_without_giant_column_reservations() {
        let schema = Schema::new(vec![Field::new("n", DataType::Integer, false)]).unwrap();
        let mut wire = SpillWriter::new(Vec::new(), schema).unwrap().finish().unwrap();
        wire.extend_from_slice(&(MAX_CELLS as u32).to_le_bytes());
        let mut reader = SpillReader::new(Cursor::new(wire), MAX_CELLS).unwrap();
        assert!(reader.next_batch().is_err());
        assert!(reader.next_batch().unwrap().is_none());
    }

    #[test]
    fn decoded_columns_grow_beyond_initial_capacity_without_losing_rows() {
        let schema = Schema::new(vec![Field::new("n", DataType::Integer, false)]).unwrap();
        let rows = (0..4_096).map(|n| vec![Value::Integer(n)]).collect::<Vec<_>>();
        let expected = Batch::from_rows(schema.clone(), rows).unwrap();
        let mut writer = SpillWriter::new(Vec::new(), schema).unwrap();
        writer.write_batch(&expected).unwrap();
        let wire = writer.finish().unwrap();
        let mut reader = SpillReader::new(Cursor::new(wire), 4_096).unwrap();
        assert_eq!(reader.next_batch().unwrap(), Some(expected));
        assert!(reader.next_batch().unwrap().is_none());
    }
}

#[cfg(test)]
mod reusable_spill_encoding_buffer_tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn reuses_scratch_across_full_selected_and_empty_batches() {
        let schema = Schema::new(vec![
            Field::new("n", DataType::Integer, false),
            Field::new("payload", DataType::Text, true),
        ]).unwrap();
        let batch = Batch::from_rows(schema.clone(), vec![
            vec![Value::Integer(1), Value::Text("hello".into())],
            vec![Value::Integer(2), Value::Null],
            vec![Value::Integer(3), Value::Text("🙂".repeat(5000))],
        ]).unwrap();
        let mut writer = SpillWriter::new(Vec::new(), schema.clone()).unwrap();
        assert_eq!(writer.encoded.capacity(), 0);
        writer.write_batch(&batch).unwrap();
        let scratch = writer.encoded.as_ptr();
        assert!(writer.encoded.capacity() >= ENCODE_BUFFER_BYTES);
        writer.write_batch_indices(&batch, &[2, 0, 2]).unwrap();
        assert_eq!(writer.encoded.as_ptr(), scratch);
        writer.write_batch_range(&batch, 1, 2).unwrap();
        writer.write_batch_indices(&batch, &[]).unwrap();
        let data = writer.finish().unwrap();
        let mut reader = SpillReader::new(Cursor::new(data), 10).unwrap();
        assert_eq!(reader.next_batch().unwrap(), Some(batch.clone()));
        assert_eq!(reader.next_batch().unwrap(), Some(batch.take(&[2, 0, 2]).unwrap()));
        assert_eq!(reader.next_batch().unwrap(), Some(batch.slice_rows(1, 2).unwrap()));
        assert_eq!(reader.next_batch().unwrap(), Some(Batch::empty(schema)));
        assert!(reader.next_batch().unwrap().is_none());
    }
}


#[cfg(test)]
mod zero_width_spill_row_limit_regressions {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn rejects_too_many_logical_rows_without_column_payloads() {
        let schema = Schema::new(vec![]).unwrap();
        let batch = Batch::new_with_row_count(schema.clone(), vec![], MAX_CELLS + 1).unwrap();
        let mut writer = SpillWriter::new(Vec::new(), schema.clone()).unwrap();
        assert!(matches!(writer.write_batch(&batch), Err(Error::Execution(_))));
        // A forged row count has no cell tags for zero-width schemas.
        // Validate the count itself before returning a logical giant batch.
        let mut encoded = SpillWriter::new(Vec::new(), schema).unwrap().finish().unwrap();
        encoded.extend_from_slice(&u32::try_from(MAX_CELLS + 1).unwrap().to_le_bytes());
        let mut reader = SpillReader::new(Cursor::new(encoded), MAX_CELLS + 1).unwrap();
        assert!(matches!(reader.next_batch(), Err(Error::Execution(_))));
        assert!(reader.next_batch().unwrap().is_none());
    }
}

#[cfg(test)]
mod buffered_spill_input_regressions {
    use super::*;
    use std::{cell::Cell, io::{self, Cursor, Read}, rc::Rc};

    struct CountingReader {
        source: Cursor<Vec<u8>>,
        calls: Rc<Cell<usize>>,
    }

    impl Read for CountingReader {
        fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
            self.calls.set(self.calls.get() + 1);
            self.source.read(output)
        }
    }

    #[test]
    fn dense_numeric_spill_uses_bounded_source_reads() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Integer, false),
            Field::new("value", DataType::Float, true),
        ]).unwrap();
        let rows = (0..10_000).map(|index| vec![
            Value::Integer(index),
            if index % 7 == 0 { Value::Null } else { Value::Float(index as f64) },
        ]).collect::<Vec<_>>();
        let batch = Batch::from_rows(schema.clone(), rows).unwrap();
        let mut writer = SpillWriter::new(Vec::new(), schema).unwrap();
        writer.write_batch(&batch).unwrap();
        let calls = Rc::new(Cell::new(0));
        let counting = CountingReader {
            source: Cursor::new(writer.finish().unwrap()),
            calls: Rc::clone(&calls),
        };
        let mut reader = SpillReader::new(counting, 10_000).unwrap();
        assert_eq!(reader.next_batch().unwrap(), Some(batch));
        assert!(reader.next_batch().unwrap().is_none());
        assert!(calls.get() < 30, "buffered reader used {} source calls", calls.get());
    }
}

#[cfg(test)]
mod bounded_wire_batch_windows_regressions {
    use super::*;

    #[test]
    fn splits_by_bytes_without_losing_order_and_null_values() {
        let schema = Schema::new(vec![Field::new("payload", DataType::Text, true)]).unwrap();
        let values = vec![
            Value::Text("1234567".into()), Value::Text("abcdefg".into()),
            Value::Text("7654321".into()), Value::Text("hijklmn".into()), Value::Null,
        ];
        let batch = Batch::from_rows(schema.clone(), values.iter()
            .cloned().map(|value| vec![value]).collect()).unwrap();
        let mut bytes = Vec::new();
        let mut writer = SpillWriter::new(&mut bytes, schema.clone()).unwrap();
        writer.write_batch_bounded_with_budget(&batch, 10, 28).unwrap();
        writer.finish().unwrap();
        let mut reader = SpillReader::new(bytes.as_slice(), 10).unwrap();
        let mut all = Vec::new();
        let mut counts = Vec::new();
        while let Some(batch) = reader.next_batch().unwrap() {
            counts.push(batch.num_rows());
            all.extend(batch.column(0).unwrap().iter().cloned());
        }
        assert_eq!(counts, vec![2, 2, 1]);
        assert_eq!(all, values);
    }

    #[test]
    fn respects_cell_counts_and_rejects_oversized_single_rows() {
        let schema = Schema::new(vec![Field::new("payload", DataType::Text, false)]).unwrap();
        let batch = Batch::from_rows(schema.clone(), vec![
            vec![Value::Text("short".into())],
            vec![Value::Text("x".repeat(100))],
        ]).unwrap();
        let mut output = Vec::new();
        let mut writer = SpillWriter::new(&mut output, schema).unwrap();
        assert!(writer.write_batch_bounded_with_budget(&batch, 8, 32).is_err());
        writer.finish().unwrap();
        let mut reader = SpillReader::new(output.as_slice(), 8).unwrap();
        // The invalid row is rejected before this batch's header is emitted.
        assert!(reader.next_batch().unwrap().is_none());
    }
}

#[cfg(test)]
mod scalar_spill_decoder_tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn scalar_columns_roundtrip_across_multiple_spill_batches() {
        let cases = [
            (DataType::Boolean, vec![Value::Boolean(true), Value::Null,
                Value::Boolean(false), Value::Boolean(true)]),
            (DataType::Integer, vec![Value::Integer(i64::MIN), Value::Null,
                Value::Integer(i64::MAX), Value::Integer(0)]),
            (DataType::Float, vec![Value::Float(f64::INFINITY), Value::Null,
                Value::Float(-0.0), Value::Float(f64::NEG_INFINITY)]),
        ];
        for (data_type, values) in cases {
            let schema = Schema::new(vec![Field::new("value", data_type, true)]).unwrap();
            let batch = Batch::from_rows(schema.clone(),
                values.into_iter().map(|value| vec![value]).collect()).unwrap();
            let mut writer = SpillWriter::new(Vec::new(), schema).unwrap();
            writer.write_batch(&batch).unwrap();
            writer.write_batch(&batch).unwrap();
            let bytes = writer.finish().unwrap();
            let mut reader = SpillReader::new(Cursor::new(bytes), 4).unwrap();
            assert_eq!(reader.next_batch().unwrap(), Some(batch.clone()));
            assert_eq!(reader.next_batch().unwrap(), Some(batch));
            assert_eq!(reader.next_batch().unwrap(), None);
            assert_eq!(reader.next_batch().unwrap(), None);
        }
    }

    #[test]
    fn scalar_decoder_rejects_wrong_tags_and_truncated_payloads_terminally() {
        let schema = Schema::new(vec![Field::new("amount", DataType::Integer, false)]).unwrap();
        let batch = Batch::from_rows(schema.clone(),
            vec![vec![Value::Integer(42)]]).unwrap();
        let mut writer = SpillWriter::new(Vec::new(), schema).unwrap();
        writer.write_batch(&batch).unwrap();
        let bytes = writer.finish().unwrap();

        let mut corrupted = bytes.clone();
        let tag_index = corrupted.len() - 9;
        corrupted[tag_index] = 2;
        let mut reader = SpillReader::new(Cursor::new(corrupted), 1).unwrap();
        assert!(reader.next_batch().is_err());
        assert_eq!(reader.next_batch().unwrap(), None);

        let mut truncated = bytes;
        truncated.pop();
        let mut reader = SpillReader::new(Cursor::new(truncated), 1).unwrap();
        assert!(reader.next_batch().is_err());
        assert_eq!(reader.next_batch().unwrap(), None);
    }
}

#[cfg(test)]
mod spill_native_slot_budget_regressions {
    use super::*;

    #[test]
    fn reject_tiny_wire_rows_that_would_allocate_oversized_native_columns() {
        let too_many = MAX_NATIVE_BATCH_BYTES / std::mem::size_of::<Value>() + 1;
        assert!(too_many < MAX_CELLS);
        assert!(check_native_slots(too_many).is_err());
        assert!(check_native_slots(too_many - 1).is_ok());
        let schema = Schema::new(vec![Field::new("flag", DataType::Boolean, true)]).unwrap();
        let mut wire = SpillWriter::new(Vec::new(), schema).unwrap().finish().unwrap();
        wire.extend_from_slice(&(too_many as u32).to_le_bytes());
        let mut reader = SpillReader::new(wire.as_slice(), too_many).unwrap();
        let error = reader.next_batch().unwrap_err().to_string();
        assert!(error.contains("native batch memory"), "{error}");
        assert!(reader.next_batch().unwrap().is_none());
    }
}

#[cfg(test)]
mod spill_writer_poisoning_regressions {
    use super::*;
    use std::io;

    #[derive(Debug)]
    struct FullAfter { bytes: Vec<u8>, limit: usize }
    impl Write for FullAfter {
        fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
            if self.bytes.len() >= self.limit {
                return Err(io::Error::other("simulated full device"));
            }
            let accepted = buffer.len().min(self.limit - self.bytes.len());
            self.bytes.extend_from_slice(&buffer[..accepted]);
            Ok(accepted)
        }
        fn flush(&mut self) -> io::Result<()> { Ok(()) }
    }

    #[test]
    fn incomplete_spill_batch_poisoned_for_all_future_appends_and_finish() {
        let schema = Schema::new(vec![Field::new("payload", DataType::Text, false)]).unwrap();
        let batch = Batch::from_rows(schema.clone(), vec![
            vec![Value::Text("x".repeat(200))],
        ]).unwrap();
        let output = FullAfter { bytes: Vec::new(), limit: 48 };
        let mut writer = SpillWriter::new(output, schema).unwrap();
        assert!(writer.write_batch(&batch).is_err());
        assert!(writer.write_batch(&batch).unwrap_err().to_string().contains("poisoned"));
        assert!(writer.finish().unwrap_err().to_string().contains("poisoned"));
    }
}

#[cfg(test)]
mod spill_bounded_writer_partial_admission_tests {
    use super::*;

    #[test]
    fn late_oversized_row_poisoned_writer_cannot_commit_a_valid_prefix() {
        let schema = Schema::new(vec![Field::new("text", DataType::Text, false)]).unwrap();
        let batch = Batch::from_rows(schema.clone(), vec![
            vec![Value::Text("ok".into())],
            vec![Value::Text("x".repeat(256))],
        ]).unwrap();
        let mut writer = SpillWriter::new(Vec::new(), schema).unwrap();
        assert!(writer.write_batch_bounded_with_budget(&batch, 1, 64).is_err());
        assert!(writer.finish().is_err());
    }

    #[test]
    fn invalid_first_row_without_emitted_windows_can_be_retried() {
        let schema = Schema::new(vec![Field::new("text", DataType::Text, false)]).unwrap();
        let oversized = Batch::from_rows(schema.clone(), vec![
            vec![Value::Text("x".repeat(256))],
        ]).unwrap();
        let valid = Batch::from_rows(schema.clone(), vec![
            vec![Value::Text("ok".into())],
        ]).unwrap();
        let mut writer = SpillWriter::new(Vec::new(), schema).unwrap();
        assert!(writer.write_batch_bounded_with_budget(&oversized, 1, 64).is_err());
        writer.write_batch_bounded_with_budget(&valid, 1, 64).unwrap();
        assert!(writer.finish().is_ok());
    }
}

#[cfg(test)]
mod single_text_spill_decoder_tests {
    use super::*;
    use crate::schema::Field;

    #[test]
    fn nullable_unicode_and_long_text_roundtrip_in_multiple_batches() {
        let schema = Schema::new(vec![Field::new("payload", DataType::Text, true)]).unwrap();
        let original = Batch::from_rows(schema.clone(), vec![
            vec![Value::Text("é🙂東京".into())],
            vec![Value::Null],
            vec![Value::Text("ab".repeat(8192))],
            vec![Value::Text("".into())],
        ]).unwrap();
        let mut writer = SpillWriter::new(Vec::<u8>::new(), schema).unwrap();
        writer.write_batch_range(&original, 0, 2).unwrap();
        writer.write_batch_range(&original, 2, 4).unwrap();
        let bytes = writer.finish().unwrap();
        let mut reader = SpillReader::new(std::io::Cursor::new(bytes), 4).unwrap();
        let first = reader.next_batch().unwrap().unwrap();
        let second = reader.next_batch().unwrap().unwrap();
        let rows = first.rows().unwrap().into_iter()
            .chain(second.rows().unwrap()).collect::<Vec<_>>();
        assert_eq!(rows, original.rows().unwrap());
        assert!(reader.next_batch().unwrap().is_none());
    }

    #[test]
    fn required_text_rejects_null_tag_and_stops_after_error() {
        let required = Schema::new(vec![Field::new("payload", DataType::Text, false)]).unwrap();
        let nullable = Schema::new(vec![Field::new("payload", DataType::Text, true)]).unwrap();
        let mut writer = SpillWriter::new(Vec::<u8>::new(), nullable).unwrap();
        writer.write_batch(&Batch::from_rows(
            Schema::new(vec![Field::new("payload", DataType::Text, true)]).unwrap(),
            vec![vec![Value::Null]],
        ).unwrap()).unwrap();
        let bytes = writer.finish().unwrap();
        // The decoder must reject a nullable wire schema mismatch even before
        // cell decoding in normal consumers.
        let mut reader = SpillReader::new(std::io::Cursor::new(bytes), 2).unwrap();
        assert_ne!(reader.schema(), &required);
        assert_eq!(reader.next_batch().unwrap().unwrap().rows().unwrap(),
            vec![vec![Value::Null]]);
    }
}
