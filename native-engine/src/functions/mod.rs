//! Native scalar functions. Each function is typed and evaluated without DataFusion.
use std::borrow::Cow;
use crate::{batch::Batch, expr::Expr, scalar::Value, schema::{DataType, Schema}, Error, Result};

mod coalesce;

mod nullif;

mod if_expr;

mod nvl2;

mod equal_null;

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

mod extrema;

mod least;

mod greatest;

mod width_bucket;

mod bit_count;

mod bit_get;

// Function names are often uppercase already. Avoid allocating a new String
// for every evaluated row and for repeated output-type checks.
fn uppercase_name(name: &str) -> Cow<'_, str> {
    if name.bytes().any(|byte| byte.is_ascii_lowercase()) {
        Cow::Owned(name.to_ascii_uppercase())
    } else {
        Cow::Borrowed(name)
    }
}

pub(crate) fn check_arity(name: &str, args: &[Expr], min: usize, max: usize) -> Result<()> {
    if (min..=max).contains(&args.len()) {
        Ok(())
    } else {
        Err(Error::Parse(format!("{name} expects {min}..={max} arguments")))
    }
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

/// Apply TRY arithmetic with the same NULL and overflow handling for each operator.
fn try_numeric_binary(
    args: &[Expr], batch: &Batch, row: usize,
    integer: fn(i64, i64) -> Option<i64>,
    float: fn(f64, f64) -> f64,
) -> Result<Value> {
    let left = args[0].evaluate(batch, row)?;
    let right = args[1].evaluate(batch, row)?;
    if left.is_null() || right.is_null() { return Ok(Value::Null); }
    if let (Value::Integer(a), Value::Integer(b)) = (&left, &right) {
        return Ok(integer(*a, *b).map(Value::Integer).unwrap_or(Value::Null));
    }
    let result = float(as_f64(&left)?, as_f64(&right)?);
    Ok(if result.is_finite() { Value::Float(result) } else { Value::Null })
}

pub(crate) fn output_type(name: &str, args: &[Expr], schema: &Schema) -> Result<DataType> {
    match uppercase_name(name).as_ref() {
        "COALESCE" => coalesce::output_type(args, schema),
        "NVL" | "IFNULL" => {
            check_arity(name, args, 2, 2)?;
            coalesce::output_type(args, schema)
        },
        "NULLIF" => nullif::output_type(args, schema),
        "IF" => if_expr::output_type(args, schema),
        "NVL2" => nvl2::output_type(args, schema),
        "EQUAL_NULL" => equal_null::output_type(args, schema),
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
        "GREATEST" => greatest::output_type(args, schema),
        "WIDTH_BUCKET" => width_bucket::output_type(args, schema),
        "BIT_COUNT" => bit_count::output_type(args, schema),
        "BIT_GET" | "GETBIT" => bit_get::output_type(args, schema),
        "TYPEOF" => {
            check_arity(name, args, 1, 1)?;
            if !matches!(&args[0], Expr::Literal(Value::Null)) {
                args[0].output_type(schema)?;
            }
            Ok(DataType::Text)
        },
        _ => {
            if crate::builtin::supports(name) {
                crate::builtin::output_type(name, args, schema)
            } else {
                crate::builtin_extra::return_type(name, args, schema)
            }
        },
    }
}

pub(crate) fn evaluate(name: &str, args: &[Expr], batch: &Batch, row: usize) -> Result<Value> {
    match uppercase_name(name).as_ref() {
        "COALESCE" => coalesce::eval(args, batch, row),
        "NVL" | "IFNULL" => coalesce::eval(args, batch, row),
        "NULLIF" => nullif::eval(args, batch, row),
        "IF" => if_expr::eval(args, batch, row),
        "NVL2" => nvl2::eval(args, batch, row),
        "EQUAL_NULL" => equal_null::eval(args, batch, row),
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
        "GREATEST" => greatest::eval(args, batch, row),
        "WIDTH_BUCKET" => width_bucket::eval(args, batch, row),
        "BIT_COUNT" => bit_count::eval(args, batch, row),
        "BIT_GET" | "GETBIT" => bit_get::eval(args, batch, row),
        "TYPEOF" => {
            let kind = match &args[0] {
                Expr::Literal(Value::Null) => "void",
                _ => match args[0].output_type(batch.schema())? {
                    DataType::Boolean => "boolean",
                    DataType::Integer => "bigint",
                    DataType::Float => "double",
                    DataType::Text => "string",
                },
            };
            Ok(Value::Text(kind.into()))
        },
        _ => {
            if crate::builtin::supports(name) {
                crate::builtin::evaluate(name, args, batch, row)
            } else {
                // Most remaining SQL builtins have at most three arguments.
                // Avoid an allocation for each row in that common path.
                if args.len() <= 4 {
                    let mut values: [Value; 4] = std::array::from_fn(|_| Value::Null);
                    for (slot, arg) in values.iter_mut().zip(args) {
                        *slot = arg.evaluate(batch, row)?;
                    }
                    return crate::builtin_extra::evaluate(name, &values[..args.len()]);
                }
                let values = args.iter().map(|arg| arg.evaluate(batch, row))
                    .collect::<Result<Vec<_>>>()?;
                crate::builtin_extra::evaluate(name, &values)
            }
        },
    }
}

#[cfg(test)]
mod uppercase_dispatch_perf_tests {
    use super::*;

    #[test]
    fn borrows_uppercase_names_and_normalizes_lower_and_mixed_case() {
        assert!(matches!(uppercase_name("COALESCE"), Cow::Borrowed(_)));
        assert_eq!(uppercase_name("coalesce").as_ref(), "COALESCE");
        assert_eq!(uppercase_name("CoAlEsCe").as_ref(), "COALESCE");
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
    fn builtin_extra_evaluates_small_and_large_arity_calls() {
        assert_eq!(run("SELECT UPPER(name) FROM t").unwrap()[0][0],
            Value::Text("BERLIN".into()));
        assert_eq!(run("SELECT CONCAT(name, '-', name) FROM t").unwrap()[0][0],
            Value::Text("Berlin-Berlin".into()));
        assert_eq!(run("SELECT CONCAT(name, '-', name, '-', name) FROM t").unwrap()[0][0],
            Value::Text("Berlin-Berlin-Berlin".into()));
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
    fn bit_get_native_sql_regression() {
        assert_eq!(run("SELECT BIT_GET(x, 2) FROM t").unwrap(), vec![
            vec![Value::Null], vec![Value::Integer(1)], vec![Value::Integer(1)],
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

#[cfg(test)]
mod spark_nvl_alias_tests {
    use crate::{batch::Batch, catalog::Catalog, executor, scalar::Value, schema::{DataType, Field, Schema}, sql};
    #[test]
    fn nvl_ifnull_are_lazy_sql_coalesce_aliases() {
        let schema = Schema::new(vec![Field::new("n", DataType::Integer, true)]).unwrap();
        let mut catalog = Catalog::new();
        catalog.register("t", Batch::from_rows(schema, vec![vec![Value::Null], vec![Value::Integer(7)]]).unwrap()).unwrap();
        let query = sql::parse_sql("SELECT NVL(n, 3) AS first, IFNULL(5, 9223372036854775807 + 1) AS second FROM t").unwrap();
        let rows = executor::execute(&query, &catalog).unwrap().rows().unwrap();
        assert_eq!(rows, vec![vec![Value::Integer(3), Value::Integer(5)], vec![Value::Integer(7), Value::Integer(5)]]);
    }
}
