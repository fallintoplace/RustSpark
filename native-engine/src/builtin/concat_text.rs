use super::Function;
use crate::{scalar::Value, schema::DataType, Error, Result};

fn output(types: &[DataType]) -> Result<DataType> { super::text_type(types) }

// Single and multi-argument CONCAT paths reserve with the same error contract.
fn reserved_output(length: usize) -> Result<String> {
    let mut output = String::new();
    output.try_reserve_exact(length)
        .map_err(|_| Error::Execution("CONCAT output allocation failed".into()))?;
    Ok(output)
}

fn concat(args: &[Value]) -> Result<Value> {
    if let [Value::Text(text)] = args {
        if text.len() > 16 * 1024 * 1024 {
            return Err(Error::Execution("CONCAT output exceeds 16 MiB".into()));
        }
        let mut output = reserved_output(text.len())?;
        output.push_str(text);
        return Ok(Value::Text(output));
    }
    let mut length = 0_usize;
    for value in args {
        let Some(text) = super::string(value)? else { return Ok(Value::Null); };
        length = length.checked_add(text.len())
            .ok_or_else(|| Error::Execution("CONCAT output length overflow".into()))?;
        if length > 16 * 1024 * 1024 {
            return Err(Error::Execution("CONCAT output exceeds 16 MiB".into()));
        }
    }
    // Type and size were checked above; avoid intermediate strings or joins.
    let mut output = reserved_output(length)?;
    for value in args {
        if let Value::Text(text) = value {
            output.push_str(text);
        }
    }
    Ok(Value::Text(output))
}

pub(super) const FUNCTIONS: &[Function] = &[Function {
    name: "concat", min_args: 1, max_args: 128,
    result_type: output, call: concat,
}];

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{batch::Batch, catalog::Catalog, executor, schema::{Field, Schema}, sql};

    #[test]
    fn joins_unicode_text_and_propagates_nulls() {
        assert_eq!(concat(&[Value::Text("café".into()), Value::Text("🙂".into())]).unwrap(),
            Value::Text("café🙂".into()));
        assert_eq!(concat(&[Value::Text("".into())]).unwrap(), Value::Text("".into()));
        assert_eq!(concat(&[Value::Text("x".into()), Value::Null]).unwrap(), Value::Null);
        assert!(concat(&[Value::Text("x".repeat(16 * 1024 * 1024)),
            Value::Text("y".into())]).is_err());
        assert!(output(&[DataType::Text, DataType::Integer]).is_err());
    }

    #[test]
    fn executes_concat_through_native_sql() {
        let schema = Schema::new(vec![Field::new("city", DataType::Text, true)]).unwrap();
        let mut catalog = Catalog::new();
        catalog.register("t", Batch::from_rows(schema,
            vec![vec![Value::Text("Berlin".into())], vec![Value::Null]]).unwrap()).unwrap();
        let plan = sql::parse_sql("SELECT CONCAT(city, '-DE') AS value FROM t").unwrap();
        let rows = executor::execute(&plan, &catalog).unwrap().rows().unwrap();
        assert_eq!(rows, vec![vec![Value::Text("Berlin-DE".into())], vec![Value::Null]]);
    }
}

#[cfg(test)]
mod single_value_concat_tests {
    use super::*;
    #[test]
    fn single_argument_fast_path_preserves_null_and_size_bound() {
        let content = "café🙂".repeat(1000);
        assert_eq!(concat(&[Value::Text(content.clone())]).unwrap(), Value::Text(content));
        assert_eq!(concat(&[Value::Null]).unwrap(), Value::Null);
        assert!(concat(&[Value::Text("x".repeat(16 * 1024 * 1024 + 1))]).is_err());
    }
}

#[cfg(test)]
mod fallible_concat_reservation_tests {
    use super::*;
    #[test]
    fn exact_result_lengths_with_single_and_multiple_unicode_values() {
        let one = concat(&[Value::Text("é🙂".into())]).unwrap();
        assert_eq!(one, Value::Text("é🙂".into()));
        let multiple = concat(&[Value::Text("é".into()), Value::Text("🙂".into())]).unwrap();
        assert_eq!(multiple, one);
        assert_eq!(concat(&[Value::Text("a".into()), Value::Null]).unwrap(), Value::Null);
    }
}

#[cfg(test)]
mod shared_concat_reservation_tests {
    use super::*;

    #[test]
    fn same_size_inputs_agree_across_argument_counts() {
        let long = "x".repeat(1024);
        for text in ["", "a", "é🙂", long.as_str()] {
            let single = concat(&[Value::Text(text.into())]).unwrap();
            let split = concat(&[
                Value::Text(String::new()), Value::Text(text.into()),
            ]).unwrap();
            assert_eq!(single, split);
        }
    }
}
