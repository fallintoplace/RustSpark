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
        // Repeated adjacent values are common after SORT and partitioning.
        // Remember the borrowed last value to avoid hashing it once per row.
        let mut previous: Option<(&str, u32)> = None;
        let mut dictionary = Vec::new();
        let mut indices = Vec::with_capacity(batch.num_rows());
        let mut bytes = 0usize;
        for value in batch.column(index)? {
            match value {
                Value::Null => {
                    indices.push(None);
                    previous = None;
                }
                Value::Text(text) => {
                    let key = text.as_str();
                    let id = if let Some((last, code)) = previous {
                        if last == key {
                            code
                        } else if let Some(&code) = seen.get(key) {
                            code
                        } else {
                            if dictionary.len() >= max_distinct {
                                return Err(Error::Execution("dictionary cardinality limit exceeded".into()));
                            }
                            bytes = bytes.checked_add(text.len())
                                .filter(|total| *total <= max_dictionary_bytes)
                                .ok_or_else(|| Error::Execution("dictionary byte limit exceeded".into()))?;
                            let code = u32::try_from(dictionary.len())
                                .map_err(|_| Error::Execution("dictionary ID overflow".into()))?;
                            dictionary.push(text.clone());
                            seen.insert(key, code);
                            code
                        }
                    } else if let Some(&code) = seen.get(key) {
                        code
                    } else {
                        if dictionary.len() >= max_distinct {
                            return Err(Error::Execution("dictionary cardinality limit exceeded".into()));
                        }
                        bytes = bytes.checked_add(text.len())
                            .filter(|total| *total <= max_dictionary_bytes)
                            .ok_or_else(|| Error::Execution("dictionary byte limit exceeded".into()))?;
                        let code = u32::try_from(dictionary.len())
                            .map_err(|_| Error::Execution("dictionary ID overflow".into()))?;
                        dictionary.push(text.clone());
                        seen.insert(key, code);
                        code
                    };
                    previous = Some((key, id));
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
        if self.dictionary.len() == 1 {
            // Repetitive columns can account for expansion with one multiply.
            // Validate codes even in this fast path.
            let mut count = 0usize;
            for &id in self.indices.iter().flatten() {
                if id != 0 {
                    return Err(Error::Execution("invalid dictionary code".into()));
                }
                count += 1;
            }
            let expanded = count.checked_mul(self.dictionary[0].len())
                .ok_or_else(|| Error::Execution("dictionary decode budget exceeded".into()))?;
            used.checked_add(expanded)
                .filter(|bytes| *bytes <= max_bytes)
                .ok_or_else(|| Error::Execution("dictionary decode budget exceeded".into()))?;
        } else {
            for id in self.indices.iter().flatten() {
                let text = self.lookup_code(*id)?;
                used = used.checked_add(text.len())
                    .filter(|bytes| *bytes <= max_bytes)
                    .ok_or_else(|| Error::Execution("dictionary decode budget exceeded".into()))?;
            }
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

    /// Decode and consume a dictionary, moving a TEXT allocation on its
    /// final use when this is the last owner of the dictionary table.
    /// Earlier repeated uses still require independent TEXT allocations.
    pub fn into_decoded_with_budget(self, max_bytes: usize) -> Result<Vec<Value>> {
        // Keep the same admission and invalid-code checks as borrowed decode,
        // without allocating output strings on the failing path.
        let mut used = self.indices.len()
            .checked_mul(std::mem::size_of::<Value>())
            .ok_or_else(|| Error::Execution("dictionary decoded size overflow".into()))?;
        if used > max_bytes {
            return Err(Error::Execution("dictionary decode budget exceeded".into()));
        }
        let mut references = vec![0usize; self.dictionary.len()];
        for index in self.indices.iter().flatten() {
            let code = usize::try_from(*index)
                .map_err(|_| Error::Execution("invalid dictionary code".into()))?;
            let value = self.dictionary.get(code)
                .ok_or_else(|| Error::Execution("invalid dictionary code".into()))?;
            references[code] += 1;
            used = used.checked_add(value.len())
                .filter(|bytes| *bytes <= max_bytes)
                .ok_or_else(|| Error::Execution("dictionary decode budget exceeded".into()))?;
        }
        if Arc::strong_count(&self.dictionary) != 1 {
            return self.decode_with_budget(max_bytes);
        }
        let Self { dictionary, indices, .. } = self;
        let table = Arc::try_unwrap(dictionary)
            .expect("single-owned dictionary Arc can be unwrapped");
        let mut available = table.into_iter().map(Some).collect::<Vec<_>>();
        let mut output = Vec::with_capacity(indices.len());
        for index in indices {
            match index {
                None => output.push(Value::Null),
                Some(index) => {
                    let code = index as usize;
                    references[code] -= 1;
                    let text = if references[code] == 0 {
                        available[code].take().expect("final dictionary reference")
                    } else {
                        available[code].as_ref()
                            .expect("live dictionary reference").clone()
                    };
                    output.push(Value::Text(text));
                }
            }
        }
        Ok(output)
    }

    pub fn into_decoded(self) -> Result<Vec<Value>> {
        self.into_decoded_with_budget(64 * 1024 * 1024)
    }

    pub fn estimated_bytes(&self) -> usize {
        // Retained heap memory includes the index allocation, each String
        // header and its backing bytes, and the shared Arc<Vec<String>>.
        // Charge the entire shared table per column as a conservative upper
        // bound when callers account for independently held columns.
        let strings = self.dictionary.iter().fold(0usize, |bytes, text| {
            bytes.saturating_add(text.capacity())
        });
        self.indices.capacity().saturating_mul(std::mem::size_of::<Option<u32>>())
            .saturating_add(self.dictionary.capacity()
                .saturating_mul(std::mem::size_of::<String>()))
            .saturating_add(strings)
            .saturating_add(std::mem::size_of::<Vec<String>>() +
                2 * std::mem::size_of::<usize>())
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
        // The left indices are copied unchanged in both merge paths. Validate
        // them before publishing an invalid combined dictionary.
        if left.indices.iter().flatten().any(|&code| code as usize >= left.dictionary.len()) {
            return Err(Error::Execution("invalid left dictionary code".into()));
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
        for text in right.dictionary.iter() {
            let code = if let Some(&existing) = seen.get(text.as_str()) {
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

#[cfg(test)]
mod single_key_decode_accounting_tests {
    use super::*;
    use crate::schema::{Field, Schema};

    #[test]
    fn single_key_fast_path_respects_exact_budget_and_invalid_codes() {
        let schema = Schema::new(vec![Field::new("event", DataType::Text, true)]).unwrap();
        let values = (0..100).map(|i| if i % 5 == 0 {
            vec![Value::Null]
        } else {
            vec![Value::Text("event-tag".into())]
        }).collect::<Vec<_>>();
        let batch = Batch::from_rows(schema, values).unwrap();
        let dictionary = DictionaryColumn::from_batch(&batch, "event", 1, 32, 100).unwrap();
        let required = 100 * std::mem::size_of::<Value>() + 80 * "event-tag".len();
        assert!(dictionary.decode_with_budget(required - 1).is_err());
        assert_eq!(dictionary.decode_with_budget(required).unwrap().len(), 100);

        let mut corrupted = dictionary.clone();
        corrupted.indices[2] = Some(15);
        assert!(corrupted.decode_with_budget(required + 100).is_err());
    }
}

#[cfg(test)]
mod invalid_left_dictionary_merge_regressions {
    use super::*;
    use crate::schema::{Field, Schema};

    #[test]
    fn rejects_corrupt_left_codes_in_shared_and_remapped_tables() {
        let schema = Schema::new(vec![Field::new("text", DataType::Text, true)]).unwrap();
        let build = |text: &str| {
            DictionaryColumn::from_batch(&Batch::from_rows(
                schema.clone(), vec![vec![Value::Text(text.into())]],
            ).unwrap(), "text", 8, 4096, 8).unwrap()
        };
        let mut corrupted = build("left");
        corrupted.indices[0] = Some(17);
        let same = build("left");
        let different = build("right");
        assert!(DictionaryColumn::concatenate(&corrupted, &same, 8, 4096, 8).is_err());
        assert!(DictionaryColumn::concatenate(&corrupted, &different, 8, 4096, 8).is_err());
        let merged = DictionaryColumn::concatenate(&same, &different, 8, 4096, 8).unwrap();
        assert_eq!(merged.decode().unwrap(), vec![
            Value::Text("left".into()), Value::Text("right".into())
        ]);
    }
}

#[cfg(test)]
mod consuming_dictionary_decode_regressions {
    use super::*;
    use crate::schema::{Field, Schema};

    #[test]
    fn moves_last_owned_text_references_but_clones_repeated_values() {
        let schema = Schema::new(vec![Field::new("tag", DataType::Text, true)]).unwrap();
        let large = "é🙂".repeat(1024);
        let input = Batch::from_rows(schema, vec![
            vec![Value::Text(large.clone())], vec![Value::Null],
            vec![Value::Text("once".repeat(1024))],
            vec![Value::Text(large.clone())],
        ]).unwrap();
        let dictionary = DictionaryColumn::from_batch(&input, "tag", 4, 100_000, 4).unwrap();
        let ptr_first = dictionary.dictionary()[0].as_ptr();
        let ptr_second = dictionary.dictionary()[1].as_ptr();
        let expected = dictionary.decode().unwrap();
        let decoded = dictionary.into_decoded().unwrap();
        assert_eq!(decoded, expected);
        assert_eq!(match &decoded[3] { Value::Text(text) => text.as_ptr(), _ => unreachable!() },
            ptr_first);
        assert_eq!(match &decoded[2] { Value::Text(text) => text.as_ptr(), _ => unreachable!() },
            ptr_second);
    }

    #[test]
    fn shared_dictionary_and_decode_budget_match_borrowed_path() {
        let schema = Schema::new(vec![Field::new("v", DataType::Text, true)]).unwrap();
        let input = Batch::from_rows(schema, vec![
            vec![Value::Text("abc".repeat(16))], vec![Value::Null],
            vec![Value::Text("abc".repeat(16))],
        ]).unwrap();
        let encoded = DictionaryColumn::from_batch(&input, "v", 2, 256, 3).unwrap();
        let required = 3 * std::mem::size_of::<Value>() + 96;
        assert!(encoded.clone().into_decoded_with_budget(required - 1).is_err());
        assert_eq!(encoded.clone().into_decoded_with_budget(required).unwrap(),
            encoded.decode_with_budget(required).unwrap());
        assert_eq!(encoded.into_decoded().unwrap(), input.column(0).unwrap());
    }

    #[test]
    fn zero_rows_and_null_only_columns_decode_without_panics() {
        let schema = Schema::new(vec![Field::new("v", DataType::Text, true)]).unwrap();
        let empty = Batch::empty(schema.clone());
        let nulls = Batch::from_rows(schema,
            vec![vec![Value::Null], vec![Value::Null]]).unwrap();
        assert!(DictionaryColumn::from_batch(&empty, "v", 0, 0, 0)
            .unwrap().into_decoded_with_budget(0).unwrap().is_empty());
        assert_eq!(DictionaryColumn::from_batch(&nulls, "v", 0, 0, 2)
            .unwrap().into_decoded().unwrap(), vec![Value::Null, Value::Null]);
    }
}

#[cfg(test)]
mod run_dictionary_encoding_tests {
    use super::*;
    use crate::schema::{Field, Schema};

    #[test]
    fn repeated_wide_keys_with_nulls_and_changes_roundtrip() {
        let a = "one 🦀".repeat(2048);
        let b = "two 🦀".repeat(2048);
        let mut values = Vec::new();
        for _ in 0..200 { values.push(Value::Text(a.clone())); }
        values.push(Value::Null);
        for _ in 0..200 { values.push(Value::Text(b.clone())); }
        for _ in 0..200 { values.push(Value::Text(a.clone())); }
        let schema = Schema::new(vec![Field::new("value", DataType::Text, true)]).unwrap();
        let batch = Batch::from_rows(schema, values.iter().cloned().map(|v| vec![v]).collect()).unwrap();
        let encoded = DictionaryColumn::from_batch(
            &batch, "value", 2, a.len() + b.len(), values.len()
        ).unwrap();
        assert_eq!(encoded.distinct_count(), 2);
        assert_eq!(encoded.encoded_string_bytes(), a.len() + b.len());
        assert_eq!(encoded.decode().unwrap(), values);
        assert!(DictionaryColumn::from_batch(
            &batch, "value", 1, a.len() + b.len(), values.len()
        ).is_err());
    }

    #[test]
    fn alternating_runs_do_not_assign_different_codes_to_equal_keys() {
        let values = ["a", "a", "b", "a", "b", "b", "a"];
        let schema = Schema::new(vec![Field::new("key", DataType::Text, false)]).unwrap();
        let batch = Batch::from_rows(schema, values.iter()
            .map(|value| vec![Value::Text((*value).to_owned())]).collect()).unwrap();
        let encoded = DictionaryColumn::from_batch(&batch, "key", 2, 2, 7).unwrap();
        assert_eq!(encoded.indices, vec![Some(0), Some(0), Some(1), Some(0), Some(1), Some(1), Some(0)]);
    }
}

#[cfg(test)]
mod dictionary_retained_heap_accounting_regressions {
    use super::*;
    use crate::schema::{Field, Schema};

    #[test]
    fn estimate_includes_dictionary_headers_capacity_and_index_allocation() {
        let schema = Schema::new(vec![Field::new("label", DataType::Text, true)]).unwrap();
        let input = Batch::from_rows(schema, vec![
            vec![Value::Text("alpha".into())], vec![Value::Text("beta".into())],
            vec![Value::Text("alpha".into())], vec![Value::Null],
        ]).unwrap();
        let encoded = DictionaryColumn::from_batch(&input, "label", 2, 9, 4).unwrap();
        let payload_only = encoded.encoded_string_bytes()
            + encoded.row_count() * std::mem::size_of::<Option<u32>>();
        assert_eq!(encoded.encoded_string_bytes(), 9);
        assert!(encoded.estimated_bytes() >= payload_only + 2 * std::mem::size_of::<String>());
        assert_eq!(encoded.decode().unwrap(), input.column(0).unwrap());
    }
}
