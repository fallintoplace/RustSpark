//! Portable, schema-checked spill format. Callers own file paths and cleanup.
use std::io::{Read, Write};
use crate::{batch::Batch, scalar::Value, schema::{DataType, Field, Schema}, Error, Result};

const MAGIC: &[u8; 8] = b"RSPILL01";
const MAX_FIELDS: usize = 4096;
const MAX_NAME_BYTES: usize = 4096;
const MAX_TEXT_BYTES: usize = 8 * 1024 * 1024;
const MAX_CELLS: usize = 4_000_000;
const MAX_BATCH_BYTES: usize = 64 * 1024 * 1024;

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
        Ok(Self { output, schema })
    }

    pub fn write_batch(&mut self, batch: &Batch) -> Result<()> {
        self.write_batch_range(batch, 0, batch.num_rows())
    }

    pub fn write_batch_range(&mut self, batch: &Batch, start: usize, end: usize) -> Result<()> {
        if start > end || end > batch.num_rows() {
            return Err(Error::Execution("invalid spill batch row range".into()));
        }
        if batch.schema() != &self.schema {
            return Err(Error::Schema("spill batch schema mismatch".into()));
        }
        let cells = (end - start).checked_mul(self.schema.len())
            .filter(|&cells| cells <= MAX_CELLS)
            .ok_or_else(|| Error::Execution("spill batch exceeds cell limit".into()))?;
        // Include the 4-byte row count and one tag per cell. Validate all
        // fixed-width and text bytes before emitting any batch data.
        let mut wire_bytes = 4usize.checked_add(cells)
            .filter(|&bytes| bytes <= MAX_BATCH_BYTES)
            .ok_or_else(|| Error::Execution("spill batch exceeds byte limit".into()))?;
        for column in 0..self.schema.len() {
            for value in &batch.column(column)?[start..end] {
                match value {
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
        let columns = (0..self.schema.len())
            .map(|index| batch.column(index))
            .collect::<Result<Vec<_>>>()?;
        write_u32(&mut self.output, end - start)?;
        for row in start..end {
            for column in &columns {
                match &column[row] {
                    Value::Null => self.output.write_all(&[0])?,
                    Value::Boolean(false) => self.output.write_all(&[1])?,
                    Value::Boolean(true) => self.output.write_all(&[2])?,
                    Value::Integer(value) => {
                        let mut wire = [0u8; 9];
                        wire[0] = 3;
                        wire[1..].copy_from_slice(&value.to_le_bytes());
                        self.output.write_all(&wire)?;
                    }
                    Value::Float(value) => {
                        let mut wire = [0u8; 9];
                        wire[0] = 4;
                        wire[1..].copy_from_slice(&value.to_bits().to_le_bytes());
                        self.output.write_all(&wire)?;
                    }
                    Value::Text(value) => {
                        if value.len() > MAX_TEXT_BYTES {
                            return Err(Error::Execution("spill text value too long".into()));
                        }
                        let mut prefix = [0u8; 5];
                        prefix[0] = 5;
                        let len = u32::try_from(value.len())
                            .map_err(|_| Error::Execution("spill text length exceeds u32".into()))?;
                        prefix[1..].copy_from_slice(&len.to_le_bytes());
                        self.output.write_all(&prefix)?;
                        self.output.write_all(value.as_bytes())?;
                    }
                }
            }
        }
        Ok(())
    }

    pub fn finish(mut self) -> Result<W> {
        self.output.flush()?;
        Ok(self.output)
    }
}

pub struct SpillReader<R: Read> {
    input: R,
    schema: Schema,
    max_rows_per_batch: usize,
    stopped: bool,
}

impl<R: Read> SpillReader<R> {
    pub fn new(mut input: R, max_rows_per_batch: usize) -> Result<Self> {
        if max_rows_per_batch == 0 {
            return Err(Error::Execution("spill reader requires positive batch size limit".into()));
        }
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

    fn read_batch(&mut self) -> Result<Option<Batch>> {
        let mut first = [0u8; 1];
        if self.input.read(&mut first)? == 0 { return Ok(None); }
        let mut remaining = [0u8; 3];
        self.input.read_exact(&mut remaining)?;
        let rows = u32::from_le_bytes([first[0], remaining[0], remaining[1], remaining[2]]) as usize;
        let cells = rows.checked_mul(self.schema.len())
            .filter(|&cells| cells <= MAX_CELLS)
            .ok_or_else(|| Error::Execution("spill batch exceeds configured limits".into()))?;
        if rows > self.max_rows_per_batch {
            return Err(Error::Execution("spill batch exceeds configured limits".into()));
        }
        // Reject the minimum encoded size before allocating column storage.
        let mut bytes_seen = 4usize.checked_add(cells)
            .filter(|&bytes| bytes <= MAX_BATCH_BYTES)
            .ok_or_else(|| Error::Execution("spill batch exceeds byte limit".into()))?;
        let mut columns = (0..self.schema.len())
            .map(|_| Vec::with_capacity(rows)).collect::<Vec<_>>();
        for _ in 0..rows {
            for column in &mut columns {
                let value = match read_byte(&mut self.input)? {
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
        Ok(Some(Batch::new_with_row_count(self.schema.clone(), columns, rows)?))
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
    fn numeric_and_text_wire_prefixes_use_single_write_calls() {
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
        assert_eq!(writer.output.writes - header_writes, 5);
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
