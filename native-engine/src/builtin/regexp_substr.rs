use super::{Function, regex_support};
use crate::{scalar::Value, schema::DataType, Error, Result};

fn output(types: &[DataType]) -> Result<DataType> {
    if types.len() == 2 && types.iter().all(|kind| *kind == DataType::Text) { Ok(DataType::Text) }
    else { Err(Error::Type("REGEXP_SUBSTR has incompatible SQL argument types".into())) }
}

fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null); };
    let Some(pattern) = super::string(&args[1])? else { return Ok(Value::Null); };
    regex_support::check_input(text)?;
    let compiled = regex_support::compile(pattern)?;
    Ok(compiled.find(text).map_or(Value::Null,
        |matched| Value::Text(matched.as_str().to_string())))
}

pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "regexp_substr", min_args: 2, max_args: 2, result_type: output, call: evaluate },
];

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn spark_sql_semantics() {
        assert_eq!(evaluate(&[Value::Text("Steven Jones".into()), Value::Text("Ste(v|ph)en".into())]).unwrap(), Value::Text("Steven".into()));
        assert_eq!(evaluate(&[Value::Text("abc".into()), Value::Text("z+".into())]).unwrap(), Value::Null);
        assert!(evaluate(&[Value::Text("abc".into()), Value::Text("(".into())]).is_err());
    }
}
