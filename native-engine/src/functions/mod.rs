//! Native scalar functions. Each function is typed and evaluated without DataFusion.
use crate::{batch::Batch, expr::Expr, scalar::Value, schema::{DataType, Schema}, Error, Result};

mod coalesce;

mod nullif;

mod if_expr;

mod abs;

mod round;

mod pmod;

mod modulo;

mod try_divide;

mod try_add;

mod try_subtract;

mod try_multiply;

mod try_mod;

mod nanvl;

mod nullifzero;

mod zeroifnull;

mod least;

mod width_bucket;

mod bit_count;

pub(crate) fn check_arity(name: &str, args: &[Expr], min: usize, max: usize) -> Result<()> {
    if (min..=max).contains(&args.len()) {
        Ok(())
    } else {
        Err(Error::Parse(format!("{name} expects {min}..={max} arguments")))
    }
}

pub(crate) fn unified_type(args: &[Expr], schema: &Schema) -> Result<DataType> {
    let mut result = None;
    for arg in args {
        if matches!(arg, Expr::Literal(Value::Null)) { continue; }
        let next = arg.output_type(schema)?;
        if result.is_some_and(|previous| previous != next) {
            return Err(Error::Type("scalar function arguments must share a type".into()));
        }
        result = Some(next);
    }
    Ok(result.unwrap_or(DataType::Text))
}

pub(crate) fn numeric_arg(expr: &Expr, schema: &Schema) -> Result<()> {
    if matches!(expr, Expr::Literal(Value::Null)) {
        return Ok(());
    }
    match expr.output_type(schema)? {
        DataType::Integer | DataType::Float => Ok(()),
        _ => Err(Error::Type("function requires numeric argument".into())),
    }
}

pub(crate) fn text_arg(expr: &Expr, schema: &Schema) -> Result<()> {
    if matches!(expr, Expr::Literal(Value::Null)) {
        return Ok(());
    }
    if expr.output_type(schema)? == DataType::Text {
        Ok(())
    } else {
        Err(Error::Type("function requires text argument".into()))
    }
}

pub(crate) fn numeric_result_type(args: &[Expr], schema: &Schema) -> Result<DataType> {
    let mut float = false;
    for arg in args {
        numeric_arg(arg, schema)?;
        if !matches!(arg, Expr::Literal(Value::Null))
            && arg.output_type(schema)? == DataType::Float {
            float = true;
        }
    }
    Ok(if float { DataType::Float } else { DataType::Integer })
}

pub(crate) fn as_f64(value: &Value) -> Result<f64> {
    match value {
        Value::Integer(n) => Ok(*n as f64),
        Value::Float(n) => Ok(*n),
        _ => Err(Error::Type("function requires numeric values".into())),
    }
}

pub(crate) fn output_type(name: &str, args: &[Expr], schema: &Schema) -> Result<DataType> {
    match name.to_ascii_uppercase().as_str() {
        "COALESCE" => coalesce::output_type(args, schema),
        "NULLIF" => nullif::output_type(args, schema),
        "IF" => if_expr::output_type(args, schema),
        "ABS" => abs::output_type(args, schema),
        "ROUND" => round::output_type(args, schema),
        "PMOD" => pmod::output_type(args, schema),
        "MOD" => modulo::output_type(args, schema),
        "TRY_DIVIDE" => try_divide::output_type(args, schema),
        "TRY_ADD" => try_add::output_type(args, schema),
        "TRY_SUBTRACT" => try_subtract::output_type(args, schema),
        "TRY_MULTIPLY" => try_multiply::output_type(args, schema),
        "TRY_MOD" => try_mod::output_type(args, schema),
        "NANVL" => nanvl::output_type(args, schema),
        "NULLIFZERO" => nullifzero::output_type(args, schema),
        "ZEROIFNULL" => zeroifnull::output_type(args, schema),
        "LEAST" => least::output_type(args, schema),
        "WIDTH_BUCKET" => width_bucket::output_type(args, schema),
        "BIT_COUNT" => bit_count::output_type(args, schema),
        _ => Err(Error::Unsupported(format!("unknown native scalar function: {name}"))),
    }
}

pub(crate) fn evaluate(name: &str, args: &[Expr], batch: &Batch, row: usize) -> Result<Value> {
    match name.to_ascii_uppercase().as_str() {
        "COALESCE" => coalesce::eval(args, batch, row),
        "NULLIF" => nullif::eval(args, batch, row),
        "IF" => if_expr::eval(args, batch, row),
        "ABS" => abs::eval(args, batch, row),
        "ROUND" => round::eval(args, batch, row),
        "PMOD" => pmod::eval(args, batch, row),
        "MOD" => modulo::eval(args, batch, row),
        "TRY_DIVIDE" => try_divide::eval(args, batch, row),
        "TRY_ADD" => try_add::eval(args, batch, row),
        "TRY_SUBTRACT" => try_subtract::eval(args, batch, row),
        "TRY_MULTIPLY" => try_multiply::eval(args, batch, row),
        "TRY_MOD" => try_mod::eval(args, batch, row),
        "NANVL" => nanvl::eval(args, batch, row),
        "NULLIFZERO" => nullifzero::eval(args, batch, row),
        "ZEROIFNULL" => zeroifnull::eval(args, batch, row),
        "LEAST" => least::eval(args, batch, row),
        "WIDTH_BUCKET" => width_bucket::eval(args, batch, row),
        "BIT_COUNT" => bit_count::eval(args, batch, row),
        _ => Err(Error::Unsupported(format!("unknown native scalar function: {name}"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{catalog::Catalog, executor, sql, schema::Field};

    fn run(query: &str) -> Result<Vec<Vec<Value>>> {
        let mut catalog = Catalog::new();
        let schema = Schema::new(vec![
            Field::new("x", DataType::Integer, true),
            Field::new("name", DataType::Text, true),
            Field::new("y", DataType::Float, true),
        ])?;
        let rows = vec![
            vec![Value::Null, Value::Text("Berlin".into()), Value::Float(9.0)],
            vec![Value::Integer(5), Value::Text("Paris".into()), Value::Float(16.0)],
            vec![Value::Integer(-4), Value::Null, Value::Null],
        ];
        catalog.register("t", crate::batch::Batch::from_rows(schema, rows)?)?;
        executor::execute(&sql::parse_sql(query)?, &catalog)?.rows()
    }

    #[test]
    fn nullif_works_in_native_sql() {
        assert_eq!(run("SELECT NULLIF(x, 5) AS n FROM t").unwrap(), vec![
            vec![Value::Null], vec![Value::Null], vec![Value::Integer(-4)],
        ]);
    }

    #[test]
    fn if_expr_works_in_native_sql() {
        assert_eq!(run("SELECT IF(x > 0, 10, 0) AS n FROM t").unwrap(), vec![
            vec![Value::Integer(0)], vec![Value::Integer(10)], vec![Value::Integer(0)],
        ]);
    }

    #[test]
    fn abs_works_in_native_sql() {
        assert_eq!(run("SELECT ABS(x) AS n FROM t").unwrap(), vec![
            vec![Value::Null], vec![Value::Integer(5)], vec![Value::Integer(4)],
        ]);
    }

    #[test]
    fn round_works_in_native_sql() {
        assert_eq!(run("SELECT ROUND(3.14159, 2) AS n FROM t").unwrap()[0],
            vec![Value::Float(3.14)]);
    }

    #[test]
    fn pmod_works_in_native_sql() {
        assert_eq!(run("SELECT PMOD(x, 3) FROM t").unwrap(), vec![
            vec![Value::Null], vec![Value::Integer(2)], vec![Value::Integer(2)],
        ]);
    }

    #[test]
    fn modulo_works_in_native_sql() {
        assert_eq!(run("SELECT MOD(x, 3) FROM t").unwrap(), vec![
            vec![Value::Null], vec![Value::Integer(2)], vec![Value::Integer(-1)],
        ]);
    }

    #[test]
    fn try_divide_works_in_native_sql() {
        assert_eq!(run("SELECT TRY_DIVIDE(10, x) FROM t").unwrap(), vec![
            vec![Value::Null], vec![Value::Float(2.0)], vec![Value::Float(-2.5)],
        ]);
        assert_eq!(run("SELECT TRY_DIVIDE(1, 0) FROM t").unwrap()[0], vec![Value::Null]);
    }

    #[test]
    fn try_add_handles_overflow_and_nulls() {
        assert_eq!(run("SELECT TRY_ADD(x, 2) FROM t").unwrap()[0], vec![Value::Null]);
        assert_eq!(run("SELECT TRY_ADD(x, 2) FROM t").unwrap()[1], vec![Value::Integer(7)]);
        assert_eq!(run("SELECT TRY_ADD(9223372036854775807, 1) FROM t").unwrap()[0], vec![Value::Null]);
    }

    #[test]
    fn try_subtract_handles_overflow_and_nulls() {
        assert_eq!(run("SELECT TRY_SUBTRACT(x, 2) FROM t").unwrap()[0], vec![Value::Null]);
        assert_eq!(run("SELECT TRY_SUBTRACT(x, 2) FROM t").unwrap()[1], vec![Value::Integer(3)]);
        assert_eq!(run("SELECT TRY_SUBTRACT(-9223372036854775807, 2) FROM t").unwrap()[0], vec![Value::Null]);
    }

    #[test]
    fn try_multiply_handles_overflow_and_nulls() {
        assert_eq!(run("SELECT TRY_MULTIPLY(x, 3) FROM t").unwrap()[0], vec![Value::Null]);
        assert_eq!(run("SELECT TRY_MULTIPLY(x, 3) FROM t").unwrap()[1], vec![Value::Integer(15)]);
        assert_eq!(run("SELECT TRY_MULTIPLY(9223372036854775807, 2) FROM t").unwrap()[0], vec![Value::Null]);
    }

    #[test]
    fn try_mod_has_native_sql_semantics() {
        assert_eq!(run("SELECT TRY_MOD(x, 3) FROM t").unwrap(), vec![
            vec![Value::Null], vec![Value::Integer(2)], vec![Value::Integer(-1)],
        ]);
        assert_eq!(run("SELECT TRY_MOD(1, 0) FROM t").unwrap()[0], vec![Value::Null]);
    }

    #[test]
    fn nanvl_has_native_sql_semantics() {
        assert_eq!(run("SELECT NANVL(y, 0) FROM t").unwrap(), vec![
            vec![Value::Float(9.0)], vec![Value::Float(16.0)], vec![Value::Null],
        ]);
    }

    #[test]
    fn nullifzero_has_native_sql_semantics() {
        assert_eq!(run("SELECT NULLIFZERO(x) FROM t").unwrap(), vec![
            vec![Value::Null], vec![Value::Integer(5)], vec![Value::Integer(-4)],
        ]);
        assert_eq!(run("SELECT NULLIFZERO(0) FROM t").unwrap()[0], vec![Value::Null]);
    }

    #[test]
    fn zeroifnull_has_native_sql_semantics() {
        assert_eq!(run("SELECT ZEROIFNULL(x) FROM t").unwrap(), vec![
            vec![Value::Integer(0)], vec![Value::Integer(5)], vec![Value::Integer(-4)],
        ]);
    }

    #[test]
    fn least_native_sql_regression() {
        assert_eq!(run("SELECT LEAST(x, 0) FROM t").unwrap(), vec![
            vec![Value::Integer(0)], vec![Value::Integer(0)], vec![Value::Integer(-4)],
        ]);
    }

    #[test]
    fn width_bucket_native_sql_regression() {
        assert_eq!(run("SELECT WIDTH_BUCKET(x, 0, 10, 5) FROM t").unwrap(), vec![
            vec![Value::Null], vec![Value::Integer(3)], vec![Value::Integer(0)],
        ]);
    }

    #[test]
    fn bit_count_native_sql_regression() {
        assert_eq!(run("SELECT BIT_COUNT(x) FROM t").unwrap(), vec![
            vec![Value::Null], vec![Value::Integer(2)], vec![Value::Integer(62)],
        ]);
    }

    #[test]
    fn coalesce_uses_first_non_null_and_preserves_types() {
        assert_eq!(run("SELECT COALESCE(x, 7) AS n FROM t").unwrap(), vec![
            vec![Value::Integer(7)],
            vec![Value::Integer(5)],
            vec![Value::Integer(-4)],
        ]);
    }
}
