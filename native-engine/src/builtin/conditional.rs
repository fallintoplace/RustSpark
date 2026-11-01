use super::{same_type, Function};
use crate::{scalar::Value, schema::DataType, Error, Result};

fn output(types: &[DataType]) -> Result<DataType> {
    if types[0] != DataType::Boolean {
        return Err(Error::Type("IF condition must be boolean".into()));
    }
    same_type(&types[1..])
}
fn evaluate(args: &[Value]) -> Result<Value> {
    let condition = args[0].as_boolean()?;
    Ok(args[if condition == Some(true) { 1 } else { 2 }].clone())
}
fn assert_true_type(types: &[DataType]) -> Result<DataType> {
    if types[0] != DataType::Boolean ||
        (types.len() == 2 && types[1] != DataType::Text) {
        return Err(Error::Type("ASSERT_TRUE expects boolean and optional text message".into()));
    }
    // Spark's successful result is an untyped NULL.
    Ok(DataType::Text)
}

fn assert_true(args: &[Value]) -> Result<Value> {
    match &args[0] {
        Value::Boolean(true) => Ok(Value::Null),
        Value::Boolean(false) | Value::Null => {
            let message = match args.get(1) {
                Some(Value::Text(message)) => message.as_str(),
                _ => "assert_true failed",
            };
            if message.len() > 4096 {
                return Err(Error::Execution("ASSERT_TRUE message exceeds 4096 bytes".into()));
            }
            Err(Error::Execution(message.to_owned()))
        }
        _ => Err(Error::Type("ASSERT_TRUE expects boolean".into())),
    }
}

fn raise_error_type(types: &[DataType]) -> Result<DataType> {
    if types[0] != DataType::Text {
        return Err(Error::Type("RAISE_ERROR expects a text message".into()));
    }
    Ok(DataType::Text)
}

fn raise_error(args: &[Value]) -> Result<Value> {
    let message = match &args[0] {
        Value::Text(message) => message.as_str(),
        Value::Null => "NULL",
        _ => return Err(Error::Type("RAISE_ERROR expects text".into())),
    };
    if message.len() > 4096 {
        return Err(Error::Execution("RAISE_ERROR message exceeds 4096 bytes".into()));
    }
    Err(Error::Execution(message.to_owned()))
}

pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "assert_true", min_args: 1, max_args: 2, result_type: assert_true_type, call: assert_true },
    Function { name: "raise_error", min_args: 1, max_args: 1, result_type: raise_error_type, call: raise_error },
    Function {
    name: "if", min_args: 3, max_args: 3, result_type: output, call: evaluate,
}];

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{batch::Batch, catalog::Catalog, executor, schema::{Field, Schema}, sql};
    #[test]
    fn conditionals_short_circuit_across_native_sql() {
        let mut catalog = Catalog::new();
        let batch = Batch::from_rows(Schema::new(vec![
            Field::new("condition", DataType::Boolean, true)
        ]).unwrap(), vec![vec![Value::Boolean(true)], vec![Value::Null]]).unwrap();
        catalog.register("t", batch).unwrap();
        let query = "SELECT IF(condition, 5, 9223372036854775807 + 1) AS n FROM t";
        let plan = sql::parse_sql(query).unwrap();
        let rows = executor::execute(&plan, &catalog).unwrap_err();
        assert!(rows.to_string().contains("overflow"));
        let query = "SELECT IF(condition, 5, 7) AS n FROM t";
        let rows = executor::execute(&sql::parse_sql(query).unwrap(), &catalog)
            .unwrap().rows().unwrap();
        assert_eq!(rows, vec![vec![Value::Integer(5)], vec![Value::Integer(7)]]);
    }
}

#[cfg(test)]
mod spark_assert_raise_tests {
    use super::*;
    use crate::{catalog::Catalog, executor, sql};
    #[test]
    fn asserted_conditions_and_user_errors() {
        assert!(crate::builtin::supports("assert_true"));
        assert!(crate::builtin::supports("raise_error"));
        assert_eq!(assert_true(&[Value::Boolean(true)]).unwrap(), Value::Null);
        assert!(assert_true(&[Value::Boolean(false)]).is_err());
        assert!(assert_true(&[Value::Null]).is_err());
        assert!(assert_true(&[Value::Boolean(false), Value::Text("failure".into())])
            .unwrap_err().to_string().contains("failure"));
        assert!(raise_error(&[Value::Text("oops".into())])
            .unwrap_err().to_string().contains("oops"));
        let good = sql::parse_sql("SELECT ASSERT_TRUE(1 = 1)").unwrap();
        assert_eq!(executor::execute(&good, &Catalog::new()).unwrap().rows().unwrap(),
            vec![vec![Value::Null]]);
        for query in ["SELECT ASSERT_TRUE(1 = 2)", "SELECT RAISE_ERROR('oops')"] {
            let plan = sql::parse_sql(query).unwrap();
            assert!(executor::execute(&plan, &Catalog::new()).is_err());
        }
    }
}
