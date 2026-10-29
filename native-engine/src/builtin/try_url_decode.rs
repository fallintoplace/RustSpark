use super::Function;
use crate::{scalar::Value, schema::DataType, Error, Result};

fn output(types: &[DataType]) -> Result<DataType> {
    if types.len() == 1 && types[0] == DataType::Text { Ok(DataType::Text) }
    else { Err(Error::Type("TRY_URL_DECODE requires a text argument".into())) }
}

fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null); };
    Ok(super::url_decode::decode(text).map(Value::Text).unwrap_or(Value::Null))
}

pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "try_url_decode", min_args: 1, max_args: 1, result_type: output, call: evaluate },
];

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn spark_sql_semantics() {
        assert_eq!(evaluate(&[Value::Text("x+%C3%A9".into())]).unwrap(), Value::Text("x é".into()));
        assert_eq!(evaluate(&[Value::Text("%GG".into())]).unwrap(), Value::Null);
        assert_eq!(evaluate(&[Value::Null]).unwrap(), Value::Null);
    }
}
