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

pub(super) const FUNCTIONS: &[Function] = &[Function {
    name: "div", min_args: 2, max_args: 2, result_type: output, call: evaluate,
}];

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
