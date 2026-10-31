//! Lossless bounded dictionary encoding for repetitive native text columns.
use std::{collections::HashMap, sync::Arc};
use crate::{batch::Batch, scalar::Value, schema::DataType, Error, Result};

#[derive(Clone, Debug)]
pub struct DictionaryColumn {
    dictionary: Arc<Vec<String>>,
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
        Ok(Self { dictionary: Arc::new(dictionary), indices, encoded_string_bytes: bytes })
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
        // Shuffle partitions often retain the same dictionary across batches.
        // Equal tables have identical codes, so no hash map, remapping, or
        // copying of potentially large immutable strings is needed.
        if Arc::ptr_eq(&left.dictionary, &right.dictionary) ||
            left.dictionary == right.dictionary {
            if right.indices.iter().flatten().any(|&code| code as usize >= right.dictionary.len()) {
                return Err(Error::Execution("invalid dictionary remap code".into()));
            }
            let mut indices = Vec::with_capacity(row_count);
            indices.extend_from_slice(&left.indices);
            indices.extend_from_slice(&right.indices);
            return Ok(Self {
                dictionary: Arc::clone(&left.dictionary),
                indices,
                encoded_string_bytes: left.encoded_string_bytes,
            });
        }
        // Keep the left table shared until the first genuinely new key.
        // Subset merges need only remapped codes, not cloned strings.
        let mut dictionary: Option<Vec<String>> = None;
        let mut seen: HashMap<&str, u32> = left.dictionary.iter().enumerate()
            .map(|(index, text)| (text.as_str(), index as u32)).collect();
        let mut bytes = left.encoded_string_bytes;
        let mut mapping = Vec::with_capacity(right.dictionary.len());
        for text in &right.dictionary {
            let code = if let Some(&existing) = seen.get(text) {
                existing
            } else {
                let distinct = dictionary.as_ref().map_or(left.dictionary.len(), Vec::len);
                if distinct >= max_distinct {
                    return Err(Error::Execution("dictionary combined cardinality limit exceeded".into()));
                }
                bytes = bytes.checked_add(text.len()).filter(|value| *value <= max_dictionary_bytes)
                    .ok_or_else(|| Error::Execution("dictionary combined byte limit exceeded".into()))?;
                let next = u32::try_from(distinct)
                    .map_err(|_| Error::Execution("dictionary combined ID overflow".into()))?;
                dictionary.get_or_insert_with(|| left.dictionary.as_ref().clone()).push(text.clone());
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
        Ok(Self {
            dictionary: dictionary.map(Arc::new).unwrap_or_else(|| Arc::clone(&left.dictionary)),
            indices, encoded_string_bytes: bytes,
        })
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

#[cfg(test)]
mod identical_dictionary_merge_perf_tests {
    use super::*;
    use crate::schema::{Field, Schema};

    #[test]
    fn equal_tables_reuse_codes_without_losing_nulls_or_budgets() {
        let schema = Schema::new(vec![Field::new("key", DataType::Text, true)]).unwrap();
        let source = Batch::from_rows(schema, vec![
            vec![Value::Text("key".repeat(512))], vec![Value::Null],
            vec![Value::Text("value".into())],
        ]).unwrap();
        let dictionary = DictionaryColumn::from_batch(&source, "key", 2, 4096, 3).unwrap();
        let merged = DictionaryColumn::concatenate(&dictionary, &dictionary, 2, 4096, 6).unwrap();
        let mut expected = dictionary.decode().unwrap();
        expected.extend(dictionary.decode().unwrap());
        assert_eq!(merged.decode().unwrap(), expected);
        assert_eq!(merged.dictionary(), dictionary.dictionary());
        assert_eq!(merged.encoded_string_bytes(), dictionary.encoded_string_bytes());
        assert!(DictionaryColumn::concatenate(&dictionary, &dictionary, 2, 4096, 5).is_err());
        let mut corrupted = dictionary.clone();
        corrupted.indices[0] = Some(300);
        assert!(DictionaryColumn::concatenate(&dictionary, &corrupted, 2, 4096, 6).is_err());
    }
}

#[cfg(test)]
mod shared_dictionary_table_regressions {
    use super::*;
    use crate::schema::{Field, Schema};

    #[test]
    fn repeated_identical_merges_share_dictionary_storage() {
        let schema = Schema::new(vec![Field::new("label", DataType::Text, true)]).unwrap();
        let source = Batch::from_rows(schema,
            (0..128).map(|index| vec![
                Value::Text(format!("label-{}-{}", index % 8, "x".repeat(2048)))
            ]).collect()).unwrap();
        let dictionary = DictionaryColumn::from_batch(&source, "label", 8, 32_768, 128).unwrap();
        let first = DictionaryColumn::concatenate(&dictionary, &dictionary, 8, 32_768, 256).unwrap();
        let second = DictionaryColumn::concatenate(&first, &dictionary, 8, 32_768, 384).unwrap();
        assert!(Arc::ptr_eq(&dictionary.dictionary, &first.dictionary));
        assert!(Arc::ptr_eq(&first.dictionary, &second.dictionary));
        assert_eq!(second.row_count(), 384);
        assert_eq!(second.decode_with_budget(900_000).unwrap().len(), 384);
        assert!(DictionaryColumn::concatenate(&first, &dictionary, 7, 32_768, 384).is_err());
    }

    #[test]
    fn distinct_tables_keep_owned_merged_dictionary_with_same_values() {
        let schema = Schema::new(vec![Field::new("s", DataType::Text, false)]).unwrap();
        let left = Batch::from_rows(schema.clone(), vec![vec![Value::Text("a".into())]]).unwrap();
        let right = Batch::from_rows(schema, vec![vec![Value::Text("b".into())]]).unwrap();
        let a = DictionaryColumn::from_batch(&left, "s", 2, 10, 1).unwrap();
        let b = DictionaryColumn::from_batch(&right, "s", 2, 10, 1).unwrap();
        let combined = DictionaryColumn::concatenate(&a, &b, 2, 10, 2).unwrap();
        assert!(!Arc::ptr_eq(&a.dictionary, &combined.dictionary));
        assert_eq!(combined.decode().unwrap(), vec![Value::Text("a".into()), Value::Text("b".into())]);
    }
}

#[cfg(test)]
mod lazy_dictionary_subset_merge_regressions {
    use super::*;
    use crate::schema::{DataType, Field, Schema};

    #[test]
    fn subset_right_keys_reuse_table_and_new_keys_copy_on_write() {
        let schema = Schema::new(vec![Field::new("key", DataType::Text, true)]).unwrap();
        let make = |rows: Vec<Value>| {
            let batch = Batch::from_rows(schema.clone(),
                rows.into_iter().map(|value| vec![value]).collect()).unwrap();
            DictionaryColumn::from_batch(&batch, "key", 10, 100_000, 100).unwrap()
        };
        let left = make(vec![
            Value::Text("alpha".repeat(1000)),
            Value::Text("βeta".into()),
            Value::Text("γamma".into()),
            Value::Null,
        ]);
        let right = make(vec![Value::Text("γamma".into()),
            Value::Text("alpha".repeat(1000)), Value::Null]);
        let joined = DictionaryColumn::concatenate(&left, &right, 10, 100_000, 100).unwrap();
        assert!(Arc::ptr_eq(&left.dictionary, &joined.dictionary));
        let expected = left.decode().unwrap().into_iter()
            .chain(right.decode().unwrap()).collect::<Vec<_>>();
        assert_eq!(joined.decode().unwrap(), expected);
        let fresh = make(vec![Value::Text("new".into())]);
        let grown = DictionaryColumn::concatenate(&joined, &fresh, 10, 100_000, 100).unwrap();
        assert!(!Arc::ptr_eq(&left.dictionary, &grown.dictionary));
        assert_eq!(grown.distinct_count(), left.distinct_count() + 1);
        let mut expected = expected;
        expected.push(Value::Text("new".into()));
        assert_eq!(grown.decode().unwrap(), expected);
    }

    #[test]
    fn subset_merge_still_rejects_corrupted_right_codes() {
        let schema = Schema::new(vec![Field::new("key", DataType::Text, false)]).unwrap();
        let make = |value: &str| DictionaryColumn::from_batch(
            &Batch::from_rows(schema.clone(), vec![vec![Value::Text(value.into())]]).unwrap(),
            "key", 10, 100, 10,
        ).unwrap();
        let left = make("alpha");
        let mut right = make("alpha");
        right.indices[0] = Some(42);
        assert!(DictionaryColumn::concatenate(&left, &right, 10, 100, 10).is_err());
    }
}
