use super::Function;
use crate::{scalar::Value, schema::DataType, Error, Result};

fn output(types: &[DataType]) -> Result<DataType> {
    if types.len() == 1 && types[0] == DataType::Text { Ok(DataType::Text) }
    else { Err(Error::Type("INITCAP has incompatible SQL argument types".into())) }
}

fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null); };
    let mut output = String::with_capacity(text.len());
    let mut start = true;
    for ch in text.chars() {
        if ch.is_whitespace() { start = true; output.push(ch); }
        else if start { output.extend(ch.to_uppercase()); start = false; }
        else { output.extend(ch.to_lowercase()); }
    }
    Ok(Value::Text(output))
}

pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "initcap", min_args: 1, max_args: 1, result_type: output, call: evaluate },
];

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn spark_sql_semantics() {
        assert_eq!(evaluate(&[Value::Text("sPark  sqL ŁÓDŹ".into())]).unwrap(),
            Value::Text("Spark  Sql Łódź".into()));
        assert_eq!(evaluate(&[Value::Null]).unwrap(), Value::Null);
    }
}
