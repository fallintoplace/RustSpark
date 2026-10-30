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
        let mut seen = HashMap::<&str, u32>::new();
        let mut dictionary = Vec::new();
        let mut indices = Vec::with_capacity(batch.num_rows());
        let mut bytes = 0usize;
        for value in batch.column(index)? {
            match value {
                Value::Null => indices.push(None),
                Value::Text(text) => {
                    let id = if let Some(id) = seen.get(text.as_str()) {
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
                        seen.insert(text.as_str(), id);
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

    fn lookup_code(&self, code: u32) -> Result<&str> {
        self.dictionary.get(code as usize).map(String::as_str)
            .ok_or_else(|| Error::Execution("invalid dictionary code".into()))
    }

    pub fn value(&self, index: usize) -> Result<Value> {
        match self.indices.get(index) {
            Some(None) => Ok(Value::Null),
            Some(Some(id)) => Ok(Value::Text(self.lookup_code(*id)?.to_owned())),
            None => Err(Error::Execution("dictionary index out of bounds".into())),
        }
    }

    pub fn decode(&self) -> Result<Vec<Value>> {
        self.decode_with_budget(64 * 1024 * 1024)
    }

    /// Refuse to expand a compact dictionary beyond the caller's memory budget.
    pub fn decode_with_budget(&self, max_bytes: usize) -> Result<Vec<Value>> {
        let mut used = self.indices.len()
            .checked_mul(std::mem::size_of::<Value>())
            .ok_or_else(|| Error::Execution("dictionary decoded size overflow".into()))?;
        if used > max_bytes {
            return Err(Error::Execution("dictionary decode budget exceeded".into()));
        }
        // Fail before allocating any decoded strings when a compact column
        // expands past its caller's budget.
        for id in self.indices.iter().flatten() {
            let text = self.lookup_code(*id)?;
            used = used.checked_add(text.len())
                .filter(|bytes| *bytes <= max_bytes)
                .ok_or_else(|| Error::Execution("dictionary decode budget exceeded".into()))?;
        }
        let mut rows = Vec::with_capacity(self.indices.len());
        for code in &self.indices {
            let value = match code {
                None => Value::Null,
                Some(id) => Value::Text(self.lookup_code(*id)?.to_owned()),
            };
            rows.push(value);
        }
        Ok(rows)
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
        let mut seen: HashMap<&str, u32> = left.dictionary.iter().enumerate()
            .map(|(index, text)| (text.as_str(), index as u32)).collect();
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
                seen.insert(text.as_str(), next);
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


#[cfg(test)]
mod bounded_dictionary_decode_tests {
    use super::*;
    use crate::schema::{Field, Schema};

    #[test]
    fn repeated_text_is_accounted_per_decoded_row() {
        let schema = Schema::new(vec![Field::new("tag", DataType::Text, false)]).unwrap();
        let batch = Batch::from_rows(schema, vec![
            vec![Value::Text("aaaa".into())],
            vec![Value::Text("aaaa".into())],
        ]).unwrap();
        let dictionary = DictionaryColumn::from_batch(&batch, "tag", 1, 4, 2).unwrap();
        let overhead = 2 * std::mem::size_of::<Value>();
        assert!(dictionary.decode_with_budget(overhead + 7).is_err());
        assert_eq!(dictionary.decode_with_budget(overhead + 8).unwrap(),
            vec![Value::Text("aaaa".into()), Value::Text("aaaa".into())]);
    }
}

#[cfg(test)]
mod borrowed_dictionary_interning_perf_tests {
    use super::*;
    use crate::{schema::{DataType, Field, Schema}, scalar::Value};

    #[test]
    fn long_duplicate_strings_and_nulls_round_trip() {
        let schema = Schema::new(vec![Field::new("s", DataType::Text, true)]).unwrap();
        let alpha = "alpha-".repeat(256);
        let beta = "beta-".repeat(256);
        let values = vec![
            Value::Text(alpha.clone()), Value::Text(beta),
            Value::Text(alpha.clone()), Value::Null,
            Value::Text(alpha),
        ];
        let batch = Batch::from_rows(schema, values.iter().cloned().map(|value| vec![value]).collect()).unwrap();
        let coded = DictionaryColumn::from_batch(&batch, "s", 2, 4096, 5).unwrap();
        assert_eq!(coded.distinct_count(), 2);
        assert_eq!(coded.decode().unwrap(), values);
    }
}

#[cfg(test)]
mod budget_before_decode_allocation_tests {
    use super::*;
    use crate::schema::{Field, Schema};
    #[test]
    fn rejects_expansion_while_retaining_exact_valid_decoding() {
        let large = "z".repeat(4_096);
        let schema = Schema::new(vec![Field::new("v", DataType::Text, true)]).unwrap();
        let input = Batch::from_rows(schema, vec![vec![Value::Text(large.clone())], vec![Value::Null], vec![Value::Text(large.clone())]]).unwrap();
        let dict = DictionaryColumn::from_batch(&input, "v", 1, 4_096, 3).unwrap();
        let baseline = 3 * std::mem::size_of::<Value>();
        assert!(dict.decode_with_budget(baseline + 8_191).is_err());
        assert_eq!(dict.decode_with_budget(baseline + 8_192).unwrap(), vec![Value::Text(large.clone()), Value::Null, Value::Text(large)]);
    }
}

#[cfg(test)]
mod dictionary_preflight_rejects_late_overflow_tests {
    use super::*;
    use crate::schema::{Field, Schema};
    #[test]
    fn budget_rejection_preserves_existing_dictionary_for_retry() {
        let schema = Schema::new(vec![Field::new("text", DataType::Text, false)]).unwrap();
        let batch = Batch::from_rows(schema, vec![vec![Value::Text("abc".into())]; 4096]).unwrap();
        let encoded = DictionaryColumn::from_batch(&batch, "text", 1, 3, 4096).unwrap();
        let base = 4096 * std::mem::size_of::<Value>();
        assert!(encoded.decode_with_budget(base + 4096 * 3 - 1).is_err());
        assert_eq!(encoded.decode_with_budget(base + 4096 * 3).unwrap().len(), 4096);
    }
}

#[cfg(test)]
mod checked_dictionary_decode_tests {
    use super::*;
    use crate::schema::{Field, Schema};

    #[test]
    fn decoded_rows_match_single_key_lookups() {
        let schema = Schema::new(vec![Field::new("label", DataType::Text, true)]).unwrap();
        let batch = Batch::from_rows(schema, vec![
            vec![Value::Text("東京".into())], vec![Value::Null],
            vec![Value::Text("東京".into())],
        ]).unwrap();
        let dictionary = DictionaryColumn::from_batch(&batch, "label", 4, 1024, 4).unwrap();
        let decoded = dictionary.decode_with_budget(1024).unwrap();
        for (index, value) in decoded.iter().enumerate() {
            assert_eq!(value, &dictionary.value(index).unwrap());
        }
    }
}
