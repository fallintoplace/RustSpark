use super::Function;
use crate::{scalar::Value, schema::DataType, Error, Result};

fn output(types: &[DataType]) -> Result<DataType> {
    if types == [DataType::Integer, DataType::Integer] {
        Ok(DataType::Integer)
    } else {
        Err(Error::Type("DIV requires integral operands".into()))
    }
}

fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(left) = super::integer(&args[0])? else { return Ok(Value::Null); };
    let Some(right) = super::integer(&args[1])? else { return Ok(Value::Null); };
    // Spark integral division is nullable for zero divisors.
    if right == 0 { return Ok(Value::Null); }
    left.checked_div(right).map(Value::Integer)
        .ok_or_else(|| Error::Execution("DIV integer overflow".into()))
}

fn try_divide_output(types: &[DataType]) -> Result<DataType> {
    if types.len() != 2 || !types.iter().all(|kind|
        matches!(kind, DataType::Integer | DataType::Float)) {
        return Err(Error::Type("TRY_DIVIDE requires numeric operands".into()));
    }
    Ok(DataType::Float)
}

fn try_divide(args: &[Value]) -> Result<Value> {
    let Some(numerator) = super::number(&args[0])? else { return Ok(Value::Null); };
    let Some(denominator) = super::number(&args[1])? else { return Ok(Value::Null); };
    if denominator == 0.0 { return Ok(Value::Null); }
    let value = numerator / denominator;
    Ok(if value.is_finite() { Value::Float(value) } else { Value::Null })
}

pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "div", min_args: 2, max_args: 2, result_type: output, call: evaluate },
    Function { name: "try_divide", min_args: 2, max_args: 2,
        result_type: try_divide_output, call: try_divide },
];

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{batch::Batch, catalog::Catalog, executor, schema::{Field, Schema}, sql};

    #[test]
    fn truncates_toward_zero_and_protects_overflow_and_zero_divisors() {
        let integer = Value::Integer;
        assert_eq!(evaluate(&[integer(7), integer(3)]).unwrap(), integer(2));
        assert_eq!(evaluate(&[integer(-7), integer(3)]).unwrap(), integer(-2));
        assert_eq!(evaluate(&[integer(7), integer(-3)]).unwrap(), integer(-2));
        assert_eq!(evaluate(&[integer(7), integer(0)]).unwrap(), Value::Null);
        assert_eq!(evaluate(&[Value::Null, integer(3)]).unwrap(), Value::Null);
        assert!(evaluate(&[integer(i64::MIN), integer(-1)]).is_err());
        assert!(output(&[DataType::Float, DataType::Integer]).is_err());
    }

    #[test]
    fn sql_div_precedence_and_nullable_results() {
        let schema = Schema::new(vec![Field::new("n", DataType::Integer, true)]).unwrap();
        let mut catalog = Catalog::new();
        catalog.register("t", Batch::from_rows(schema, vec![
            vec![Value::Integer(-9)], vec![Value::Integer(0)], vec![Value::Null]
        ]).unwrap()).unwrap();
        let plan = sql::parse_sql("SELECT n DIV 2 + 1 AS result, n DIV 0 AS zero FROM t").unwrap();
        let rows = executor::execute(&plan, &catalog).unwrap().rows().unwrap();
        assert_eq!(rows, vec![
            vec![Value::Integer(-3), Value::Null],
            vec![Value::Integer(1), Value::Null],
            vec![Value::Null, Value::Null],
        ]);
    }
}

#[cfg(test)]
mod spark_try_divide_native_tests {
    use super::*;
    use crate::{catalog::Catalog, executor, sql};

    #[test]
    fn numeric_division_returns_double_or_null_instead_of_failing() {
        assert_eq!(try_divide(&[Value::Integer(3), Value::Integer(2)]).unwrap(),
            Value::Float(1.5));
        assert_eq!(try_divide(&[Value::Integer(1), Value::Integer(0)]).unwrap(),
            Value::Null);
        assert_eq!(try_divide(&[Value::Integer(i64::MIN), Value::Integer(-1)]).unwrap(),
            Value::Float(9_223_372_036_854_775_808.0));
        assert_eq!(try_divide(&[Value::Null, Value::Integer(2)]).unwrap(),
            Value::Null);
        assert!(try_divide_output(&[DataType::Text, DataType::Integer]).is_err());
        assert_eq!(try_divide_output(&[DataType::Integer, DataType::Integer]).unwrap(),
            DataType::Float);
    }

    #[test]
    fn runs_as_native_sql() {
        let query = sql::parse_sql("SELECT TRY_DIVIDE(3, 2), TRY_DIVIDE(1, 0)").unwrap();
        assert_eq!(executor::execute(&query, &Catalog::new()).unwrap().rows().unwrap(),
            vec![vec![Value::Float(1.5), Value::Null]]);
    }
}
