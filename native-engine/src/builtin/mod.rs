//! Native scalar function registry. Neither planning nor evaluation uses DataFusion.
//! Functions validate arity and input types before a scan is executed.
use std::{collections::HashMap, sync::OnceLock};
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
mod inverse_trig;
mod hyperbolic;
mod inverse_hyperbolic;
mod angles;
mod modular;
mod bitwise;
mod bitshift;
mod bit_count;
mod width_bucket;
mod factorial;
mod hypot;
mod log1p;
mod bankers_round;
mod nan_semantics;
mod padding;
mod ascii_chr;
mod hex_utf8;
mod base64_utf8;
mod string_search;
mod text_predicates;
mod json_path;
mod json_valid;
mod json_type;
mod json_value;
mod json_query;
mod json_array_length;
mod json_object_keys;
mod json_exists;
mod json_extract;
mod regex_support;
mod regexp_extract;
mod regexp_count;
mod concat_ws;
mod concat_text;
mod trim_chars;
mod repeat_text;
mod translate_text;
mod left_right;
mod overlay_text;
mod levenshtein;
mod char_length;
mod octet_length;
mod bit_length;
mod lcase;
mod ucase;
mod initcap;
mod space;
mod substring_index;
mod find_in_set;
mod elt;
mod regexp_instr;
mod regexp_substr;
mod regexp_like;
mod regexp_replace;
mod crc32;
mod url_encode;
mod url_decode;
mod try_url_decode;
mod bin;
mod format_number;
mod conv;
mod null_predicates;
mod len;
mod soundex;
mod luhn_check;
mod quote;
mod cot;
mod csc;
mod sec;
mod asind;
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
    &inverse_trig::FUNCTIONS,
    &hyperbolic::FUNCTIONS,
    &inverse_hyperbolic::FUNCTIONS,
    &angles::FUNCTIONS,
    &modular::FUNCTIONS,
    &bitwise::FUNCTIONS,
    &bitshift::FUNCTIONS,
    &bit_count::FUNCTIONS,
    &width_bucket::FUNCTIONS,
    &factorial::FUNCTIONS,
    &hypot::FUNCTIONS,
    &log1p::FUNCTIONS,
    &bankers_round::FUNCTIONS,
    &nan_semantics::FUNCTIONS,
    &padding::FUNCTIONS,
    &ascii_chr::FUNCTIONS,
    &hex_utf8::FUNCTIONS,
    &base64_utf8::FUNCTIONS,
    &string_search::FUNCTIONS,
    &text_predicates::FUNCTIONS,
    &json_valid::FUNCTIONS,
    &json_type::FUNCTIONS,
    &json_value::FUNCTIONS,
    &json_query::FUNCTIONS,
    &json_array_length::FUNCTIONS,
    &json_object_keys::FUNCTIONS,
    &json_exists::FUNCTIONS,
    &json_extract::FUNCTIONS,
    &regexp_extract::FUNCTIONS,
    &regexp_count::FUNCTIONS,
    &concat_ws::FUNCTIONS,
    &concat_text::FUNCTIONS,
    &trim_chars::FUNCTIONS,
    &repeat_text::FUNCTIONS,
    &translate_text::FUNCTIONS,
    &left_right::FUNCTIONS,
    &overlay_text::FUNCTIONS,
    &levenshtein::FUNCTIONS,
    &char_length::FUNCTIONS,
    &octet_length::FUNCTIONS,
    &bit_length::FUNCTIONS,
    &lcase::FUNCTIONS,
    &ucase::FUNCTIONS,
    &initcap::FUNCTIONS,
    &space::FUNCTIONS,
    &substring_index::FUNCTIONS,
    &find_in_set::FUNCTIONS,
    &elt::FUNCTIONS,
    &regexp_instr::FUNCTIONS,
    &regexp_substr::FUNCTIONS,
    &regexp_like::FUNCTIONS,
    &regexp_replace::FUNCTIONS,
    &crc32::FUNCTIONS,
    &url_encode::FUNCTIONS,
    &url_decode::FUNCTIONS,
    &try_url_decode::FUNCTIONS,
    &bin::FUNCTIONS,
    &format_number::FUNCTIONS,
    &conv::FUNCTIONS,
    &null_predicates::FUNCTIONS,
    &len::FUNCTIONS,
    &soundex::FUNCTIONS,
    &luhn_check::FUNCTIONS,
    &quote::FUNCTIONS,
    &cot::FUNCTIONS,
    &csc::FUNCTIONS,
    &sec::FUNCTIONS,
    &md5_digest::FUNCTIONS,
    &asind::FUNCTIONS,
    // FUNCTION_LIST
];

fn lookup(name: &str) -> Result<Function> {
    // SQL evaluators resolve functions per row. Build the index once rather than
    // scanning every module on each scalar evaluation.
    static FUNCTIONS_BY_NAME: OnceLock<HashMap<&'static str, Function>> = OnceLock::new();
    let functions = FUNCTIONS_BY_NAME.get_or_init(|| {
        let mut index = HashMap::new();
        for function in CORE.iter().chain(MODULES.iter().flat_map(|module| module.iter())) {
            // Preserve the first registration if modules ever define an alias twice.
            index.entry(function.name).or_insert(*function);
        }
        index
    });
    let function = functions.get(name).or_else(|| {
        // Native SQL function names are registered in lowercase. Avoid allocating
        // a lowercase String for the common already-normalized query.
        name.bytes().any(|byte| byte.is_ascii_uppercase())
            .then(|| name.to_ascii_lowercase())
            .and_then(|lower| functions.get(lower.as_str()))
    });
    function.copied()
        .ok_or_else(|| Error::Unsupported(format!("unsupported native SQL function: {name}")))
}

pub(crate) fn supports(name: &str) -> bool {
    lookup(name).is_ok()
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
    if name.eq_ignore_ascii_case("elt") {
        let index = arguments[0].evaluate(batch, row)?;
        let Some(index) = integer(&index)? else { return Ok(Value::Null); };
        let selected = index.checked_sub(1).and_then(|n| usize::try_from(n).ok())
            .and_then(|offset| offset.checked_add(1));
        return match selected.and_then(|position| arguments.get(position)) {
            Some(argument) => argument.evaluate(batch, row),
            None => Ok(Value::Null),
        };
    }
    // Use stack storage for common small scalar function calls.
    if arguments.len() <= 4 {
        let mut values: [Value; 4] = std::array::from_fn(|_| Value::Null);
        for (slot, expression) in values.iter_mut().zip(arguments) {
            *slot = expression.evaluate(batch, row)?;
        }
        return (function.call)(&values[..arguments.len()]);
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
        assert!(supports("LeVeNsHtEiN"));
        assert!(!supports("no_such_native_function"));
    }
}

#[cfg(test)]
mod elt_lazy_evaluation_tests {
    use super::*;
    use crate::expr::BinaryOp;
    #[test]
    fn skip_unselected_failing_expression() {
        let schema = Schema::new(vec![]).unwrap();
        let batch = Batch::from_rows(schema.clone(), vec![vec![]]).unwrap();
        let failing = Expr::Cast {
            expression: Box::new(Expr::binary(Expr::Literal(Value::Integer(1)),
                BinaryOp::Divide, Expr::Literal(Value::Integer(0)))),
            target: DataType::Text,
        };
        let args = [Expr::Literal(Value::Integer(1)),
            Expr::Literal(Value::Text("ok".into())), failing.clone()];
        assert_eq!(output_type("elt", &args, &schema).unwrap(), DataType::Text);
        assert_eq!(evaluate("elt", &args, &batch, 0).unwrap(), Value::Text("ok".into()));
        let out_of_bounds = [Expr::Literal(Value::Integer(5)),
            Expr::Literal(Value::Text("ok".into())), failing];
        assert_eq!(evaluate("elt", &out_of_bounds, &batch, 0).unwrap(), Value::Null);
    }
}

#[cfg(test)]
mod builtin_stack_storage_regression_tests {
    use super::*;
    #[test]
    fn builtin_scalar_calls_use_small_and_large_arity_paths() {
        let schema = Schema::new(vec![Field::new("text", DataType::Text, true)]).unwrap();
        let batch = Batch::from_rows(schema, vec![vec![Value::Text("x".into())]]).unwrap();
        let arg = |s: &str| Expr::Literal(Value::Text(s.into()));
        assert_eq!(evaluate("ucase", &[arg("café")], &batch, 0).unwrap(),
            Value::Text("CAFÉ".into()));
        assert_eq!(evaluate("concat_ws", &[arg("-"), arg("a"), arg("b"), arg("c")], &batch, 0).unwrap(),
            Value::Text("a-b-c".into()));
        assert_eq!(evaluate("concat_ws", &[arg("-"), arg("a"), arg("b"), arg("c"), arg("d")], &batch, 0).unwrap(),
            Value::Text("a-b-c-d".into()));
    }
}
