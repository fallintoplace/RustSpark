use super::{Function, regex_support};
use crate::{scalar::Value, schema::DataType, Result};

fn output(types: &[DataType]) -> Result<DataType> {
    super::text_type(types)?;
    Ok(DataType::Boolean)
}

fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null); };
    let Some(pattern) = super::string(&args[1])? else { return Ok(Value::Null); };
    regex_support::check_input(text)?;
    if pattern.is_empty() { return Ok(Value::Boolean(true)); }
    Ok(Value::Boolean(regex_support::compile(pattern)?.is_match(text)))
}


fn regexp_escape_type(types: &[DataType]) -> Result<DataType> { super::text_type(types) }
fn regexp_escape(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null); };
    if text.len() > 4096 {
        return Err(crate::Error::Execution("REGEXP_ESCAPE input exceeds pattern budget".into()));
    }
    let escaped = regex::escape(text);
    if escaped.len() > 4096 {
        return Err(crate::Error::Execution("REGEXP_ESCAPE output exceeds pattern budget".into()));
    }
    Ok(Value::Text(escaped))
}

pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "regexp_escape", min_args: 1, max_args: 1, result_type: regexp_escape_type, call: regexp_escape },Function {
    name: "regexp_like", min_args: 2, max_args: 2,
    result_type: output, call: evaluate,
}];

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{batch::Batch, catalog::Catalog, executor, schema::{Field, Schema}, sql};
    #[test]
    fn matches_unicode_strings_through_native_sql() {
        let mut catalog = Catalog::new();
        let schema = Schema::new(vec![Field::new("name", DataType::Text, true)]).unwrap();
        catalog.register("t", Batch::from_rows(schema, vec![
            vec![Value::Text("Berlin".into())], vec![Value::Text("Łódź".into())],
        ]).unwrap()).unwrap();
        let rows = executor::execute(&sql::parse_sql(
            "SELECT REGEXP_LIKE(name, '^B.*') AS match_name FROM t"
        ).unwrap(), &catalog).unwrap().rows().unwrap();
        assert_eq!(rows, vec![vec![Value::Boolean(true)], vec![Value::Boolean(false)]]);
    }
}

#[cfg(test)]
mod regexp_escape_extension_tests {
    use super::*;
    #[test]
    fn preserves_unicode_and_null_semantics() {
        assert!(crate::builtin::supports("regexp_escape"));
        let escaped = regexp_escape(&[Value::Text("a+b?".into())]).unwrap();
        let Value::Text(pattern) = escaped else { panic!("expected text"); };
        assert!(regex_support::compile(&pattern).unwrap().is_match("a+b?"));
        assert!(!regex_support::compile(&pattern).unwrap().is_match("abbb"));
        assert_eq!(regexp_escape(&[Value::Null]).unwrap(), Value::Null);
        assert!(regexp_escape(&[Value::Text("(".repeat(4097))]).is_err());
    }
}

#[cfg(test)]
mod empty_regex_like_perf_tests {
    use super::*;
    #[test]
    fn empty_pattern_matches_every_nonnull_input() {
        for text in ["", "hello", "é🙂"] {
            assert_eq!(evaluate(&[Value::Text(text.into()), Value::Text("".into())])
                .unwrap(), Value::Boolean(true));
        }
        assert_eq!(evaluate(&[Value::Null, Value::Text("".into())]).unwrap(), Value::Null);
    }
}
