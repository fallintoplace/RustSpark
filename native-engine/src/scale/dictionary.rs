//! Lossless bounded dictionary encoding for repetitive native text columns.
use std::collections::HashMap;
use crate::{batch::Batch, scalar::Value, schema::DataType, Error, Result};

#[derive(Clone, Debug)]
pub struct DictionaryColumn {
    dictionary: Vec<String>,
    indices: Vec<Option<u32>>,
    encoded_string_bytes: usize,
}

impl DictionaryColumn {
    pub fn from_batch(
        batch: &Batch, column: &str, max_distinct: usize,
        max_dictionary_bytes: usize, max_rows: usize,
    ) -> Result<Self> {
        let index = batch.schema().column(column)?;
        if batch.schema().field(index)?.data_type != DataType::Text {
            return Err(Error::Type("dictionary encoding requires a text column".into()));
        }
        if batch.num_rows() > max_rows {
            return Err(Error::Execution("dictionary row limit exceeded".into()));
        }
        let mut seen = HashMap::<String, u32>::new();
        let mut dictionary = Vec::new();
        let mut indices = Vec::with_capacity(batch.num_rows());
        let mut bytes = 0usize;
        for value in batch.column(index)? {
            match value {
                Value::Null => indices.push(None),
                Value::Text(text) => {
                    let id = if let Some(id) = seen.get(text) {
                        *id
                    } else {
                        if dictionary.len() >= max_distinct {
                            return Err(Error::Execution("dictionary cardinality limit exceeded".into()));
                        }
                        bytes = bytes.checked_add(text.len())
                            .filter(|total| *total <= max_dictionary_bytes)
                            .ok_or_else(|| Error::Execution("dictionary byte limit exceeded".into()))?;
                        let id = u32::try_from(dictionary.len())
                            .map_err(|_| Error::Execution("dictionary ID overflow".into()))?;
                        dictionary.push(text.clone());
                        seen.insert(text.clone(), id);
                        id
                    };
                    indices.push(Some(id));
                }
                _ => return Err(Error::Type("dictionary value is not text".into())),
            }
        }
        Ok(Self { dictionary, indices, encoded_string_bytes: bytes })
    }

    pub fn distinct_count(&self) -> usize { self.dictionary.len() }
    pub fn row_count(&self) -> usize { self.indices.len() }
    pub fn encoded_string_bytes(&self) -> usize { self.encoded_string_bytes }
    pub fn dictionary(&self) -> &[String] { &self.dictionary }

    pub fn value(&self, index: usize) -> Result<Value> {
        match self.indices.get(index) {
            Some(None) => Ok(Value::Null),
            Some(Some(id)) => self.dictionary.get(*id as usize)
                .cloned().map(Value::Text)
                .ok_or_else(|| Error::Execution("invalid dictionary code".into())),
            None => Err(Error::Execution("dictionary index out of bounds".into())),
        }
    }

    pub fn decode(&self) -> Result<Vec<Value>> {
        (0..self.row_count()).map(|index| self.value(index)).collect()
    }

    pub fn estimated_bytes(&self) -> usize {
        self.encoded_string_bytes
            .saturating_add(self.indices.len().saturating_mul(std::mem::size_of::<Option<u32>>()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{Schema, Field};

    #[test]
    fn repeated_text_uses_one_dictionary_entry() {
        let schema = Schema::new(vec![Field::new("event", DataType::Text, true)]).unwrap();
        let values = vec![
            Value::Text("page_view".into()), Value::Text("page_view".into()),
            Value::Null, Value::Text("purchase".into()),
        ];
        let batch = Batch::from_rows(schema, values.iter()
            .cloned().map(|value| vec![value]).collect()).unwrap();
        let encoded = DictionaryColumn::from_batch(&batch, "event", 2, 30, 4).unwrap();
        assert_eq!(encoded.distinct_count(), 2);
        assert_eq!(encoded.decode().unwrap(), values);
        assert!(DictionaryColumn::from_batch(&batch, "event", 1, 30, 4).is_err());
        assert!(DictionaryColumn::from_batch(&batch, "event", 2, 3, 4).is_err());
    }
}
