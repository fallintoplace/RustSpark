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

fn read_text_bounded(
    input: &mut impl Read, bytes_seen: &mut usize
) -> Result<String> {
    let length = read_u32(input)?;
    if length > MAX_TEXT_BYTES {
        return Err(Error::Execution("spill text value exceeds byte limit".into()));
    }
    let next = bytes_seen.checked_add(length)
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
        output.write_all(MAGIC)?;
        write_u32(&mut output, schema.len())?;
        for field in schema.fields() {
            if field.name.len() > MAX_NAME_BYTES {
                return Err(Error::Execution("spill field name too long".into()));
            }
            write_u32(&mut output, field.name.len())?;
            output.write_all(field.name.as_bytes())?;
            output.write_all(&[type_code(field.data_type), u8::from(field.nullable)])?;
        }
        Ok(Self { output, schema })
    }

    pub fn write_batch(&mut self, batch: &Batch) -> Result<()> {
        if batch.schema() != &self.schema {
            return Err(Error::Schema("spill batch schema mismatch".into()));
        }
        if batch.num_rows().checked_mul(self.schema.len()).is_none_or(|cells| cells > MAX_CELLS) {
            return Err(Error::Execution("spill batch exceeds cell limit".into()));
        }
        write_u32(&mut self.output, batch.num_rows())?;
        for row in 0..batch.num_rows() {
            for column in 0..self.schema.len() {
                match batch.value(row, column)? {
                    Value::Null => self.output.write_all(&[0])?,
                    Value::Boolean(false) => self.output.write_all(&[1])?,
                    Value::Boolean(true) => self.output.write_all(&[2])?,
                    Value::Integer(value) => {
                        self.output.write_all(&[3])?;
                        self.output.write_all(&value.to_le_bytes())?;
                    }
                    Value::Float(value) => {
                        self.output.write_all(&[4])?;
                        self.output.write_all(&value.to_bits().to_le_bytes())?;
                    }
                    Value::Text(value) => {
                        if value.len() > MAX_TEXT_BYTES {
                            return Err(Error::Execution("spill text value too long".into()));
                        }
                        self.output.write_all(&[5])?;
                        write_u32(&mut self.output, value.len())?;
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
        Ok(Self { input, schema: Schema::new(fields)?, max_rows_per_batch })
    }

    pub fn schema(&self) -> &Schema { &self.schema }

    pub fn next_batch(&mut self) -> Result<Option<Batch>> {
        let mut first = [0u8; 1];
        if self.input.read(&mut first)? == 0 { return Ok(None); }
        let mut remaining = [0u8; 3];
        self.input.read_exact(&mut remaining)?;
        let rows = u32::from_le_bytes([first[0], remaining[0], remaining[1], remaining[2]]) as usize;
        if rows > self.max_rows_per_batch ||
            rows.checked_mul(self.schema.len()).is_none_or(|cells| cells > MAX_CELLS) {
            return Err(Error::Execution("spill batch exceeds configured limits".into()));
        }
        let mut bytes_seen = 0usize;
        let mut output = Vec::with_capacity(rows);
        for _ in 0..rows {
            let mut row = Vec::with_capacity(self.schema.len());
            for _ in self.schema.fields() {
                let value = match read_byte(&mut self.input)? {
                    0 => Value::Null,
                    1 => Value::Boolean(false),
                    2 => Value::Boolean(true),
                    3 => {
                        let mut b = [0; 8];
                        self.input.read_exact(&mut b)?;
                        Value::Integer(i64::from_le_bytes(b))
                    }
                    4 => {
                        let mut b = [0; 8];
                        self.input.read_exact(&mut b)?;
                        Value::Float(f64::from_bits(u64::from_le_bytes(b)))
                    }
                    5 => Value::Text(read_text_bounded(&mut self.input, &mut bytes_seen)?),
                    _ => return Err(Error::Parse("invalid spill value tag".into())),
                };
                row.push(value);
            }
            output.push(row);
        }
        Ok(Some(Batch::from_rows(self.schema.clone(), output)?))
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
