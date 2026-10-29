//! Native scalar functions. Each function is typed and evaluated without DataFusion.
use crate::{batch::Batch, expr::Expr, scalar::Value, schema::{DataType, Schema}, Error, Result};

mod coalesce;

mod nullif;

mod if_expr;

mod abs;

mod round;

mod lower;

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

pub(crate) fn output_type(name: &str, args: &[Expr], schema: &Schema) -> Result<DataType> {
    match name.to_ascii_uppercase().as_str() {
        "COALESCE" => coalesce::output_type(args, schema),
        "NULLIF" => nullif::output_type(args, schema),
        "IF" => if_expr::output_type(args, schema),
        "ABS" => abs::output_type(args, schema),
        "ROUND" => round::output_type(args, schema),
        "LOWER" => lower::output_type(args, schema),
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
        "LOWER" => lower::eval(args, batch, row),
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
    fn lower_works_in_native_sql() {
        assert_eq!(run("SELECT LOWER(name) AS n FROM t").unwrap(), vec![
            vec![Value::Text("berlin".into())],
            vec![Value::Text("paris".into())],
            vec![Value::Null],
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
