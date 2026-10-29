use super::Function;
use crate::{scalar::Value, schema::DataType, Error, Result};

fn output(types: &[DataType]) -> Result<DataType> {
    super::float_type(types)?;
    Ok(DataType::Integer)
}

fn evaluate(args: &[Value]) -> Result<Value> {
    if args.iter().any(Value::is_null) {
        return Ok(Value::Null);
    }
    match (&args[0], &args[1]) {
        (Value::Integer(numerator), Value::Integer(denominator)) => {
            if *denominator == 0 {
                return Ok(Value::Null);
            }
            numerator.checked_div(*denominator).map(Value::Integer)
                .ok_or_else(|| Error::Execution("DIV integer overflow".into()))
        }
        _ => {
            let numerator = super::number(&args[0])?.ok_or_else(||
                Error::Type("DIV requires numeric values".into()))?;
            let denominator = super::number(&args[1])?.ok_or_else(||
                Error::Type("DIV requires numeric values".into()))?;
            if denominator == 0.0 {
                return Ok(Value::Null);
            }
            let result = (numerator / denominator).trunc();
            if !result.is_finite() || result < i64::MIN as f64
                || result >= 9_223_372_036_854_775_808.0
            {
                return Err(Error::Execution("DIV result outside signed 64-bit range".into()));
            }
            Ok(Value::Integer(result as i64))
        }
    }
}

pub(super) const FUNCTIONS: &[Function] = &[Function {
    name: "div", min_args: 2, max_args: 2, result_type: output, call: evaluate,
}];

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{batch::Batch, catalog::Catalog, executor, schema::{Field, Schema}, sql};

    #[test]
    fn div_truncates_to_integer_and_returns_null_for_zero() {
        assert_eq!(evaluate(&[Value::Integer(7), Value::Integer(2)]).unwrap(), Value::Integer(3));
        assert_eq!(evaluate(&[Value::Integer(-7), Value::Integer(2)]).unwrap(), Value::Integer(-3));
        assert_eq!(evaluate(&[Value::Float(-7.5), Value::Float(2.0)]).unwrap(), Value::Integer(-3));
        assert_eq!(evaluate(&[Value::Integer(7), Value::Integer(0)]).unwrap(), Value::Null);
        assert_eq!(evaluate(&[Value::Integer(7), Value::Null]).unwrap(), Value::Null);
        assert!(evaluate(&[Value::Integer(i64::MIN), Value::Integer(-1)]).is_err());
    }

    #[test]
    fn spark_div_operator_executes_through_native_sql() {
        let mut catalog = Catalog::new();
        let schema = Schema::new(vec![Field::new("x", DataType::Integer, false)]).unwrap();
        catalog.register("t", Batch::from_rows(schema, vec![vec![Value::Integer(1)]]).unwrap()).unwrap();
        let plan = sql::parse_sql("SELECT 7 DIV 2 AS quotient FROM t").unwrap();
        let result = executor::execute(&plan, &catalog).unwrap().rows().unwrap();
        assert_eq!(result, vec![vec![Value::Integer(3)]]);
    }
}
