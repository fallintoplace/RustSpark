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

 
impl DictionaryColumn {
    /// Concatenate dictionary-coded columns while unifying their string tables.
    pub fn concatenate(
        left: &Self, right: &Self,
        max_distinct: usize, max_dictionary_bytes: usize, max_rows: usize,
    ) -> Result<Self> {
        let row_count = left.row_count().checked_add(right.row_count())
            .filter(|rows| *rows <= max_rows)
            .ok_or_else(|| Error::Execution("dictionary combined row limit exceeded".into()))?;
        if left.dictionary.len() > max_distinct || left.encoded_string_bytes > max_dictionary_bytes {
            return Err(Error::Execution("left dictionary exceeds merge budget".into()));
        }
        let mut dictionary = left.dictionary.clone();
        let mut seen: HashMap<String, u32> = dictionary.iter().enumerate()
            .map(|(index, text)| (text.clone(), index as u32)).collect();
        let mut bytes = left.encoded_string_bytes;
        let mut mapping = Vec::with_capacity(right.dictionary.len());
        for text in &right.dictionary {
            let code = if let Some(&existing) = seen.get(text) {
                existing
            } else {
                if dictionary.len() >= max_distinct {
                    return Err(Error::Execution("dictionary combined cardinality limit exceeded".into()));
                }
                bytes = bytes.checked_add(text.len()).filter(|value| *value <= max_dictionary_bytes)
                    .ok_or_else(|| Error::Execution("dictionary combined byte limit exceeded".into()))?;
                let next = u32::try_from(dictionary.len())
                    .map_err(|_| Error::Execution("dictionary combined ID overflow".into()))?;
                dictionary.push(text.clone());
                seen.insert(text.clone(), next);
                next
            };
            mapping.push(code);
        }
        let mut indices = Vec::with_capacity(row_count);
        indices.extend_from_slice(&left.indices);
        for code in &right.indices {
            indices.push(match code {
                Some(index) => Some(*mapping.get(*index as usize)
                    .ok_or_else(|| Error::Execution("invalid dictionary remap code".into()))?),
                None => None,
            });
        }
        Ok(Self { dictionary, indices, encoded_string_bytes: bytes })
    }
}

#[cfg(test)]
mod dictionary_concat_feature_tests {
    use super::*;
    use crate::schema::{DataType, Field, Schema};

    #[test]
    fn merges_overlapping_dictionaries_without_losing_nulls() {
        let schema = Schema::new(vec![Field::new("s", DataType::Text, true)]).unwrap();
        let a = Batch::from_rows(schema.clone(), vec![
            vec![Value::Text("apple".into())], vec![Value::Null]
        ]).unwrap();
        let b = Batch::from_rows(schema, vec![
            vec![Value::Text("banana".into())], vec![Value::Text("apple".into())],
        ]).unwrap();
        let da = DictionaryColumn::from_batch(&a, "s", 4, 40, 5).unwrap();
        let db = DictionaryColumn::from_batch(&b, "s", 4, 40, 5).unwrap();
        let merged = DictionaryColumn::concatenate(&da, &db, 3, 40, 5).unwrap();
        assert_eq!(merged.distinct_count(), 2);
        assert_eq!(merged.decode().unwrap(), vec![
            Value::Text("apple".into()), Value::Null,
            Value::Text("banana".into()), Value::Text("apple".into()),
        ]);
        assert!(DictionaryColumn::concatenate(&da, &db, 1, 40, 5).is_err());
    }
}

