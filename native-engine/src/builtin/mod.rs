//! Native scalar function registry. Neither planning nor evaluation uses DataFusion.
//! Functions validate arity and input types before a scan is executed.
use crate::{batch::Batch, expr::Expr, scalar::Value, schema::{DataType, Schema}, Error, Result};

mod nullif;
mod conditional;
mod abs_sign;
mod floor_ceil;
mod round;
mod roots;
mod power;
mod exponential;
mod logarithms;
mod trigonometry;
// MODULE_IMPORTS

#[derive(Clone, Copy)]
pub struct Function {
    pub name: &'static str,
    pub min_args: usize,
    pub max_args: usize,
    pub result_type: fn(&[DataType]) -> Result<DataType>,
    pub call: fn(&[Value]) -> Result<Value>,
}

fn coalesce_type(types: &[DataType]) -> Result<DataType> {
    same_type(types)
}

fn coalesce(values: &[Value]) -> Result<Value> {
    Ok(values.iter().find(|v| !v.is_null()).cloned().unwrap_or(Value::Null))
}

const CORE: &[Function] = &[
    Function { name: "coalesce", min_args: 1, max_args: 128, result_type: coalesce_type, call: coalesce },
];

const MODULES: &[&[Function]] = &[
    &nullif::FUNCTIONS,
    &conditional::FUNCTIONS,
    &abs_sign::FUNCTIONS,
    &floor_ceil::FUNCTIONS,
    &round::FUNCTIONS,
    &roots::FUNCTIONS,
    &power::FUNCTIONS,
    &exponential::FUNCTIONS,
    &logarithms::FUNCTIONS,
    &trigonometry::FUNCTIONS,
    // FUNCTION_LIST
];

fn lookup(name: &str) -> Result<Function> {
    CORE.iter().chain(MODULES.iter().flat_map(|functions| functions.iter()))
        .copied().find(|function| function.name.eq_ignore_ascii_case(name))
        .ok_or_else(|| Error::Unsupported(format!("unsupported native SQL function: {name}")))
}

fn check_arity(function: Function, count: usize) -> Result<()> {
    if (function.min_args..=function.max_args).contains(&count) { Ok(()) }
    else { Err(Error::Parse(format!("{} expects {}..={} arguments, received {count}",
        function.name, function.min_args, function.max_args))) }
}

/// All variants are statically typed; NULLs are allowed at evaluation time.
pub fn same_type(types: &[DataType]) -> Result<DataType> {
    let Some(&first) = types.first() else {
        return Err(Error::Type("function needs at least one typed argument".into()));
    };
    if types.iter().all(|kind| *kind == first) { Ok(first) }
    else { Err(Error::Type("native SQL function has incompatible argument types".into())) }
}

pub fn numeric(kind: DataType) -> Result<()> {
    if matches!(kind, DataType::Integer | DataType::Float) { Ok(()) }
    else { Err(Error::Type("numeric SQL function requires a number".into())) }
}
pub fn float_type(types: &[DataType]) -> Result<DataType> {
    if types.iter().copied().all(|kind| numeric(kind).is_ok()) { Ok(DataType::Float) }
    else { Err(Error::Type("numeric SQL function requires numbers".into())) }
}
pub fn text_type(types: &[DataType]) -> Result<DataType> {
    if types.iter().all(|kind| *kind == DataType::Text) { Ok(DataType::Text) }
    else { Err(Error::Type("text SQL function requires text".into())) }
}
pub fn integer_result(_types: &[DataType]) -> Result<DataType> { Ok(DataType::Integer) }
pub fn boolean_result(_types: &[DataType]) -> Result<DataType> { Ok(DataType::Boolean) }
pub fn float_result(_types: &[DataType]) -> Result<DataType> { Ok(DataType::Float) }
pub fn text_result(_types: &[DataType]) -> Result<DataType> { Ok(DataType::Text) }

pub fn number(value: &Value) -> Result<Option<f64>> {
    match value {
        Value::Integer(v) => Ok(Some(*v as f64)),
        Value::Float(v) => Ok(Some(*v)),
        Value::Null => Ok(None),
        _ => Err(Error::Type("expected numeric SQL argument".into())),
    }
}
pub fn integer(value: &Value) -> Result<Option<i64>> {
    match value {
        Value::Integer(v) => Ok(Some(*v)),
        Value::Null => Ok(None),
        _ => Err(Error::Type("expected integer SQL argument".into())),
    }
}
pub fn string(value: &Value) -> Result<Option<&str>> {
    match value {
        Value::Text(v) => Ok(Some(v)),
        Value::Null => Ok(None),
        _ => Err(Error::Type("expected text SQL argument".into())),
    }
}
pub fn finite(value: f64) -> Result<Value> {
    if value.is_finite() { Ok(Value::Float(value)) }
    else { Err(Error::Execution("SQL function produced nonfinite number".into())) }
}

pub fn output_type(name: &str, arguments: &[Expr], schema: &Schema) -> Result<DataType> {
    let function = lookup(name)?;
    check_arity(function, arguments.len())?;
    let types = arguments.iter().map(|expression| expression.output_type(schema))
        .collect::<Result<Vec<_>>>()?;
    if name.eq_ignore_ascii_case("coalesce") {
        let not_null = arguments.iter().zip(&types).filter_map(|(expr, kind)| {
            if matches!(expr, Expr::Literal(Value::Null)) { None } else { Some(*kind) }
        }).collect::<Vec<_>>();
        // SQL NULL has no intrinsic type; all-NULL remains a nullable text slot.
        return if not_null.is_empty() { Ok(DataType::Text) }
            else { same_type(&not_null) };
    }
    (function.result_type)(&types)
}

pub fn evaluate(name: &str, arguments: &[Expr], batch: &Batch, row: usize) -> Result<Value> {
    let function = lookup(name)?;
    check_arity(function, arguments.len())?;
    // COALESCE must short-circuit. Evaluating unused arguments could throw.
    if name.eq_ignore_ascii_case("if") {
        let condition = arguments[0].evaluate(batch, row)?.as_boolean()?;
        return arguments[if condition == Some(true) { 1 } else { 2 }].evaluate(batch, row);
    }
    if name.eq_ignore_ascii_case("coalesce") {
        for argument in arguments {
            let value = argument.evaluate(batch, row)?;
            if !value.is_null() { return Ok(value); }
        }
        return Ok(Value::Null);
    }
    let evaluated = arguments.iter().map(|arg| arg.evaluate(batch, row))
        .collect::<Result<Vec<_>>>()?;
    (function.call)(&evaluated)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{catalog::Catalog, executor, schema::Field, sql};
    #[test]
    fn coalesce_executes_native_sql_and_short_circuits_invalid_branches() {
        let schema = Schema::new(vec![Field::new("n", DataType::Integer, true)]).unwrap();
        let data = Batch::from_rows(schema, vec![vec![Value::Null], vec![Value::Integer(8)]]).unwrap();
        let mut catalog = Catalog::new();
        catalog.register("t", data).unwrap();
        let plan = sql::parse_sql("SELECT COALESCE(n, 3) AS fallback FROM t").unwrap();
        let rows = executor::execute(&plan, &catalog).unwrap().rows().unwrap();
        assert_eq!(rows, vec![vec![Value::Integer(3)], vec![Value::Integer(8)]]);
        let plan = sql::parse_sql("SELECT COALESCE(9, 9223372036854775807 + 1) AS safe FROM t").unwrap();
        assert_eq!(executor::execute(&plan, &catalog).unwrap().row(0).unwrap()[0], Value::Integer(9));
        assert!(sql::parse_sql("SELECT COALESCE(n,) FROM t").is_err());
    }
}
