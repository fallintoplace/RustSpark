use std::cmp::Ordering;

use crate::{
    batch::Batch,
    scalar::Value,
    schema::{DataType, Schema},
    Error,
    Result,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BinaryOp {
    Equal,
    NotEqual,
    Less,
    LessEqual,
    Greater,
    GreaterEqual,
    Add,
    Subtract,
    Multiply,
    Divide,
    And,
    Or,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Expr {
    Column(String),
    Literal(Value),
    Binary {
        left: Box<Expr>,
        op: BinaryOp,
        right: Box<Expr>,
    },
    Not(Box<Expr>),
    IsNull(Box<Expr>),
    Like { value: Box<Expr>, pattern: Box<Expr>, escape: Option<Box<Expr>> },
    Case { branches: Vec<(Expr, Expr)>, else_expr: Option<Box<Expr>> },
    Call { name: String, args: Vec<Expr> },
    TryCast { expression: Box<Expr>, target: DataType },
    Cast { expression: Box<Expr>, target: DataType },
    IsDistinctFrom { left: Box<Expr>, right: Box<Expr>, negated: bool },
    /// A bounded Spark IN/NOT IN list that evaluates its left operand once.
    InList { needle: Box<Expr>, choices: Vec<Expr>, negated: bool },
}

impl Expr {
    pub fn binary(left: Self, op: BinaryOp, right: Self) -> Self {
        Self::Binary {
            left: Box::new(left),
            op,
            right: Box::new(right),
        }
    }

    pub fn output_type(&self, schema: &Schema) -> Result<DataType> {
        match self {
            Self::Column(name) => Ok(schema.field(schema.column(name)?)?.data_type),
            Self::Literal(value) => Ok(match value {
                Value::Null | Value::Text(_) => DataType::Text,
                Value::Boolean(_) => DataType::Boolean,
                Value::Integer(_) => DataType::Integer,
                Value::Float(_) => DataType::Float,
            }),
            Self::InList { needle, choices, .. } => {
                let kind = needle.output_type(schema)?;
                let needle_null = matches!(needle.as_ref(), Expr::Literal(Value::Null));
                for choice in choices {
                    // Even a NULL needle must type-check all referenced
                    // columns, although evaluation will short-circuit.
                    let other = choice.output_type(schema)?;
                    if matches!(choice, Expr::Literal(Value::Null)) || needle_null { continue; }
                    if kind != other && !matches!(
                        (kind, other),
                        (DataType::Integer, DataType::Float) | (DataType::Float, DataType::Integer)
                    ) {
                        return Err(Error::Type("IN values require compatible SQL types".into()));
                    }
                }
                Ok(DataType::Boolean)
            },
            Self::Not(inner) => {
                if !matches!(inner.as_ref(), Self::Literal(Value::Null))
                    && inner.output_type(schema)? != DataType::Boolean {
                    return Err(Error::Type("NOT requires a boolean operand".into()));
                }
                Ok(DataType::Boolean)
            },
            Self::IsNull(inner) => {
                // IS NULL is valid for any value type, but its input still
                // needs to resolve against the schema before execution.
                inner.output_type(schema)?;
                Ok(DataType::Boolean)
            },
            Self::Like { value, pattern, escape } => {
                for expression in [value.as_ref(), pattern.as_ref()] {
                    if expression.output_type(schema)? != DataType::Text {
                        return Err(Error::Type("LIKE requires text operands".into()));
                    }
                }
                if let Some(escape) = escape {
                    if escape.output_type(schema)? != DataType::Text {
                        return Err(Error::Type("LIKE ESCAPE requires text".into()));
                    }
                }
                Ok(DataType::Boolean)
            },
            Self::IsDistinctFrom { left, right, .. } => {
                let l = left.output_type(schema)?;
                let r = right.output_type(schema)?;
                if !matches!(left.as_ref(), Self::Literal(Value::Null))
                    && !matches!(right.as_ref(), Self::Literal(Value::Null))
                    && l != r
                    && !matches!((l, r), (DataType::Integer, DataType::Float) |
                        (DataType::Float, DataType::Integer)) {
                    return Err(Error::Type("IS DISTINCT FROM requires comparable operands".into()));
                }
                Ok(DataType::Boolean)
            },
            Self::TryCast { expression, target } | Self::Cast { expression, target } => {
                // The target type does not make an unknown input column valid.
                expression.output_type(schema)?;
                Ok(*target)
            },
            Self::Call { name, args } => crate::functions::output_type(name, args, schema),
            Self::Case { branches, else_expr } => {
                for (condition, _) in branches {
                    if !matches!(condition, Expr::Literal(Value::Null))
                        && condition.output_type(schema)? != DataType::Boolean {
                        return Err(Error::Type("CASE WHEN requires a boolean condition".into()));
                    }
                }
                let mut output = None;
                for expr in branches.iter().map(|(_, value)| value)
                    .chain(else_expr.iter().map(Box::as_ref))
                {
                    if matches!(expr, Expr::Literal(Value::Null)) { continue; }
                    let kind = expr.output_type(schema)?;
                    if output.is_some_and(|existing| existing != kind) {
                        return Err(Error::Type("CASE result branches must share a type".into()));
                    }
                    output = Some(kind);
                }
                Ok(output.unwrap_or(DataType::Text))
            },
            Self::Binary { left, op, right } => match op {
                BinaryOp::Equal
                | BinaryOp::NotEqual
                | BinaryOp::Less
                | BinaryOp::LessEqual
                | BinaryOp::Greater
                | BinaryOp::GreaterEqual => {
                    let left_type = left.output_type(schema)?;
                    let right_type = right.output_type(schema)?;
                    if !matches!(left.as_ref(), Expr::Literal(Value::Null))
                        && !matches!(right.as_ref(), Expr::Literal(Value::Null))
                        && left_type != right_type
                        && !matches!((left_type, right_type),
                            (DataType::Integer, DataType::Float) |
                            (DataType::Float, DataType::Integer)) {
                        return Err(Error::Type("comparison requires comparable SQL types".into()));
                    }
                    Ok(DataType::Boolean)
                },
                BinaryOp::And | BinaryOp::Or => {
                    for operand in [left.as_ref(), right.as_ref()] {
                        if !matches!(operand, Expr::Literal(Value::Null))
                            && operand.output_type(schema)? != DataType::Boolean {
                            return Err(Error::Type("AND/OR require boolean operands".into()));
                        }
                    }
                    Ok(DataType::Boolean)
                },
                BinaryOp::Divide => {
                    let a = left.output_type(schema)?;
                    let b = right.output_type(schema)?;
                    if matches!(a, DataType::Integer | DataType::Float)
                        && matches!(b, DataType::Integer | DataType::Float) {
                        Ok(DataType::Float)
                    } else {
                        Err(Error::Type("division requires numeric operands".into()))
                    }
                },
                BinaryOp::Add | BinaryOp::Subtract | BinaryOp::Multiply => {
                    let a = left.output_type(schema)?;
                    let b = right.output_type(schema)?;
                    if a == DataType::Float || b == DataType::Float {
                        Ok(DataType::Float)
                    } else if a == DataType::Integer && b == DataType::Integer {
                        Ok(DataType::Integer)
                    } else {
                        Err(Error::Type("arithmetic requires numeric operands".into()))
                    }
                }
            },
        }
    }

    pub fn evaluate(&self, batch: &Batch, row: usize) -> Result<Value> {
        match self {
            Self::Column(name) => Ok(batch.value(row, batch.schema().column(name)?)?.clone()),
            Self::Literal(value) => Ok(value.clone()),
            Self::InList { needle, choices, negated } => {
                let input = needle.evaluate(batch, row)?;
                if input.is_null() { return Ok(Value::Null); }
                let mut unknown = false;
                for choice in choices {
                    let candidate = choice.evaluate(batch, row)?;
                    match input.sql_equal(&candidate)? {
                        Some(true) => return Ok(Value::Boolean(!negated)),
                        None => unknown = true,
                        Some(false) => {}
                    }
                }
                if unknown { Ok(Value::Null) }
                else { Ok(Value::Boolean(*negated)) }
            },
            Self::Not(expr) => Ok(match expr.evaluate(batch, row)?.as_boolean()? {
                Some(value) => Value::Boolean(!value),
                None => Value::Null,
            }),
            Self::IsNull(expr) => Ok(Value::Boolean(expr.evaluate(batch, row)?.is_null())),
            Self::IsDistinctFrom { left, right, negated } => {
                let left = left.evaluate(batch, row)?;
                let right = right.evaluate(batch, row)?;
                let distinct = match (left.is_null(), right.is_null()) {
                    (true, true) => false,
                    (true, false) | (false, true) => true,
                    (false, false) => left.sql_equal(&right)?.map(|equal| !equal)
                        .unwrap_or_else(|| left != right),
                };
                Ok(Value::Boolean(if *negated { !distinct } else { distinct }))
            },
            Self::Like { value, pattern, escape } => {
                let value = value.evaluate(batch, row)?;
                let pattern = pattern.evaluate(batch, row)?;
                let escaped = match escape {
                    None => Value::Text("\\".into()),
                    Some(expr) => expr.evaluate(batch, row)?,
                };
                match (value, pattern, escaped) {
                    (Value::Null, _, _) | (_, Value::Null, _) | (_, _, Value::Null) =>
                        Ok(Value::Null),
                    (Value::Text(value), Value::Text(pattern), Value::Text(escape)) => {
                        let mut chars = escape.chars();
                        let escape = chars.next().ok_or_else(||
                            Error::Type("LIKE ESCAPE must be one character".into()))?;
                        if chars.next().is_some() {
                            return Err(Error::Type("LIKE ESCAPE must be one character".into()));
                        }
                        Ok(Value::Boolean(sql_like_escape(&value, &pattern, escape)?))
                    }
                    _ => Err(Error::Type("LIKE and ESCAPE require text values".into())),
                }
            },
            Self::TryCast { expression, target } =>
                Ok(crate::try_cast::convert(expression.evaluate(batch, row)?, *target)),
            Self::Cast { expression, target } =>
                expression.evaluate(batch, row)?.cast_to(*target),
            Self::Call { name, args } =>
                crate::functions::evaluate(name, args, batch, row),
            Self::Case { branches, else_expr } => {
                for (condition, value) in branches {
                    if condition.evaluate(batch, row)?.as_boolean()? == Some(true) {
                        return value.evaluate(batch, row);
                    }
                }
                match else_expr {
                    Some(value) => value.evaluate(batch, row),
                    None => Ok(Value::Null),
                }
            },
            Self::Binary { left, op, right } => {
                let left = left.evaluate(batch, row)?;
                if (*op == BinaryOp::And && left.as_boolean()? == Some(false))
                    || (*op == BinaryOp::Or && left.as_boolean()? == Some(true)) {
                    return Ok(left);
                }
                let right = right.evaluate(batch, row)?;
                evaluate_binary(&left, *op, &right)
            }
        }
    }
}

/// SQL LIKE wildcard matching with bounded working memory and work.

#[derive(Clone, Copy)]
enum LikePiece { Literal(char), One, Many }

fn sql_like_escape(input: &str, pattern: &str, escape: char) -> Result<bool> {
    let mut symbols = pattern.chars();
    let mut pieces = Vec::new();
    while let Some(symbol) = symbols.next() {
        let next = if symbol == escape {
            let literal = symbols.next().ok_or_else(||
                Error::Parse("LIKE pattern ends with ESCAPE character".into()))?;
            if !matches!(literal, '_' | '%') && literal != escape {
                return Err(Error::Parse("LIKE ESCAPE may only escape %, _ or itself".into()));
            }
            LikePiece::Literal(literal)
        } else {
            match symbol {
                '%' => LikePiece::Many,
                '_' => LikePiece::One,
                other => LikePiece::Literal(other),
            }
        };
        // Adjacent '%' tokens accept exactly the same strings as one '%'.
        // Collapsing them also keeps LIKE work proportional to the effective pattern.
        if matches!(next, LikePiece::Many) &&
            matches!(pieces.last(), Some(LikePiece::Many)) {
            continue;
        }
        pieces.push(next);
    }
    // Most analytical LIKE predicates are exact, prefix, suffix or contains
    // matches. Handle these without materializing the text as Unicode scalars
    // or running the full dynamic-programming matcher.
    let literal = |parts: &[LikePiece]| -> Option<String> {
        parts.iter().map(|piece| match piece {
            LikePiece::Literal(ch) => Some(*ch),
            _ => None,
        }).collect()
    };
    if pieces.len() == 1 && matches!(pieces[0], LikePiece::Many) {
        return Ok(true);
    }
    if pieces.len() > 1 && matches!(pieces.last(), Some(LikePiece::Many)) {
        if let Some(prefix) = literal(&pieces[..pieces.len() - 1]) {
            return Ok(input.starts_with(&prefix));
        }
    }
    if pieces.len() > 1 && matches!(pieces.first(), Some(LikePiece::Many)) {
        if let Some(suffix) = literal(&pieces[1..]) {
            return Ok(input.ends_with(&suffix));
        }
    }
    if pieces.len() > 2 &&
        matches!(pieces.first(), Some(LikePiece::Many)) &&
        matches!(pieces.last(), Some(LikePiece::Many)) {
        if let Some(needle) = literal(&pieces[1..pieces.len() - 1]) {
            return Ok(input.contains(&needle));
        }
    }
    if pieces.iter().all(|piece| matches!(piece, LikePiece::Literal(_))) {
        return Ok(input.chars().eq(pieces.iter().map(|piece| match piece {
            LikePiece::Literal(ch) => *ch,
            _ => unreachable!(),
        })));
    }
    let text = input.chars().collect::<Vec<_>>();
    if (text.len() + 1).checked_mul(pieces.len() + 1)
        .is_none_or(|cells| cells > 8_000_000) {
        return Err(Error::Execution("LIKE pattern exceeds work budget".into()));
    }
    let mut prev = vec![false; text.len() + 1];
    let mut next = vec![false; text.len() + 1];
    prev[0] = true;
    for piece in pieces {
        next.fill(false);
        match piece {
            LikePiece::Many => {
                next[0] = prev[0];
                for i in 1..=text.len() { next[i] = prev[i] || next[i - 1]; }
            }
            LikePiece::One => {
                for i in 1..=text.len() { next[i] = prev[i - 1]; }
            }
            LikePiece::Literal(symbol) => {
                for i in 1..=text.len() { next[i] = prev[i - 1] && text[i - 1] == symbol; }
            }
        }
        std::mem::swap(&mut prev, &mut next);
    }
    Ok(prev[text.len()])
}

#[cfg(test)]
fn sql_like(input: &str, pattern: &str) -> Result<bool> {
    sql_like_escape(input, pattern, '\\')
}

fn bool_to_value(value: Option<bool>) -> Value {
    value.map_or(Value::Null, Value::Boolean)
}

fn evaluate_binary(left: &Value, op: BinaryOp, right: &Value) -> Result<Value> {
    use BinaryOp::*;
    match op {
        And => {
            let a = left.as_boolean()?;
            let b = right.as_boolean()?;
            Ok(bool_to_value(match (a, b) {
                (Some(false), _) | (_, Some(false)) => Some(false),
                (Some(true), Some(true)) => Some(true),
                _ => None,
            }))
        }
        Or => {
            let a = left.as_boolean()?;
            let b = right.as_boolean()?;
            Ok(bool_to_value(match (a, b) {
                (Some(true), _) | (_, Some(true)) => Some(true),
                (Some(false), Some(false)) => Some(false),
                _ => None,
            }))
        }
        Equal | NotEqual | Less | LessEqual | Greater | GreaterEqual => {
            let result = left.sql_compare(right)?;
            Ok(bool_to_value(result.map(|order| match op {
                Equal => order == Ordering::Equal,
                NotEqual => order != Ordering::Equal,
                Less => order == Ordering::Less,
                LessEqual => order != Ordering::Greater,
                Greater => order == Ordering::Greater,
                GreaterEqual => order != Ordering::Less,
                _ => unreachable!(),
            })))
        }
        Add | Subtract | Multiply | Divide => numeric(left, op, right),
    }
}

fn numeric(left: &Value, op: BinaryOp, right: &Value) -> Result<Value> {
    if left.is_null() || right.is_null() {
        return Ok(Value::Null);
    }
    if op != BinaryOp::Divide {
        if let (Value::Integer(a), Value::Integer(b)) = (left, right) {
            let value = match op {
                BinaryOp::Add => a.checked_add(*b),
                BinaryOp::Subtract => a.checked_sub(*b),
                BinaryOp::Multiply => a.checked_mul(*b),
                _ => unreachable!(),
            };
            return value
                .map(Value::Integer)
                .ok_or_else(|| Error::Execution("integer arithmetic overflow".into()));
        }
    }
    let as_float = |value: &Value| match value {
        Value::Integer(value) => Ok(*value as f64),
        Value::Float(value) => Ok(*value),
        _ => Err(Error::Type("arithmetic requires numeric operands".into())),
    };
    let a = as_float(left)?;
    let b = as_float(right)?;
    if op == BinaryOp::Divide && b == 0.0 {
        return Err(Error::Execution("division by zero".into()));
    }
    let value = match op {
        BinaryOp::Add => a + b,
        BinaryOp::Subtract => a - b,
        BinaryOp::Multiply => a * b,
        BinaryOp::Divide => a / b,
        _ => unreachable!(),
    };
    if !value.is_finite() {
        return Err(Error::Execution("non-finite arithmetic result".into()));
    }
    Ok(Value::Float(value))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{schema::Field, schema::Schema};

    pub(super) fn batch() -> Batch {
        Batch::from_rows(
            Schema::new(vec![Field::new("x", DataType::Integer, true)]).unwrap(),
            vec![vec![Value::Null], vec![Value::Integer(4)]],
        )
        .unwrap()
    }

    #[test]
    fn like_wildcards_and_unicode_are_character_aware() {
        assert!(sql_like("café", "caf_").unwrap());
        assert!(sql_like("abc", "a%c").unwrap());
        assert!(!sql_like("abc", "a_d").unwrap());
        assert!(sql_like("", "%").unwrap());
    }

    #[test]
    fn three_valued_logic_and_not() {
        let expr = Expr::binary(Expr::Column("x".into()), BinaryOp::Greater, Expr::Literal(Value::Integer(0)));
        assert_eq!(expr.evaluate(&batch(), 0).unwrap(), Value::Null);
        let false_and_null = Expr::binary(Expr::Literal(Value::Boolean(false)), BinaryOp::And, Expr::Literal(Value::Null));
        assert_eq!(false_and_null.evaluate(&batch(), 0).unwrap(), Value::Boolean(false));
    }

    #[test]
    fn overflow_and_division_by_zero_are_reported() {
        assert!(evaluate_binary(&Value::Integer(i64::MAX), BinaryOp::Add, &Value::Integer(1)).is_err());
        assert!(evaluate_binary(&Value::Integer(1), BinaryOp::Divide, &Value::Integer(0)).is_err());
    }
}

#[cfg(test)]
mod short_circuit_feature_tests {
    use super::*;
    use super::tests::batch;

    #[test]
    fn decisive_boolean_skips_invalid_right_operand() {
        let bad = Expr::binary(
            Expr::binary(Expr::Literal(Value::Integer(1)), BinaryOp::Divide,
                Expr::Literal(Value::Integer(0))),
            BinaryOp::Greater, Expr::Literal(Value::Integer(0)),
        );
        let no = Expr::binary(Expr::Literal(Value::Boolean(false)), BinaryOp::And, bad.clone());
        let yes = Expr::binary(Expr::Literal(Value::Boolean(true)), BinaryOp::Or, bad);
        assert_eq!(no.evaluate(&batch(), 0).unwrap(), Value::Boolean(false));
        assert_eq!(yes.evaluate(&batch(), 0).unwrap(), Value::Boolean(true));
    }
}

#[cfg(test)]
mod division_type_preflight_tests {
    use super::*;
    use crate::schema::{Field, Schema};

    #[test]
    fn text_division_rejected_even_with_no_rows() {
        let schema = Schema::new(vec![
            Field::new("label", DataType::Text, true),
            Field::new("price", DataType::Integer, false),
        ]).unwrap();
        let invalid = Expr::binary(Expr::Column("label".into()),
            BinaryOp::Divide, Expr::Column("price".into()));
        assert!(invalid.output_type(&schema).is_err());
        let valid = Expr::binary(Expr::Column("price".into()),
            BinaryOp::Divide, Expr::Literal(Value::Integer(2)));
        assert_eq!(valid.output_type(&schema).unwrap(), DataType::Float);
    }
}

#[cfg(test)]
mod consolidated_literal_like_fastpath_tests {
    use super::*;
    #[test]
    fn literal_patterns_do_not_pay_quadratic_work_budget() {
        let text = "é".repeat(4_000);
        assert!(sql_like(&text, &text).unwrap());
        assert!(!sql_like(&text, "x").unwrap());
        assert!(sql_like("abc", "a%c").unwrap());
    }
}

#[cfg(test)]
mod like_rolling_buffer_perf_tests {
    use super::*;

    #[test]
    fn multiple_wildcards_match_unicode_and_reset_dp_state() {
        assert!(sql_like_escape("a🙂bbccd", "a%_b%c_", '\\').unwrap());
        assert!(!sql_like_escape("abc", "%d%", '\\').unwrap());
        assert!(sql_like_escape("a%b", "a\\%b", '\\').unwrap());
        assert!(sql_like_escape("", "%%", '\\').unwrap());
        assert!(!sql_like_escape("", "_%", '\\').unwrap());
    }
}

#[cfg(test)]
mod not_expression_static_type_tests {
    use super::*;
    use crate::schema::Field;

    #[test]
    fn rejects_not_of_integer_even_with_no_rows() {
        let schema = Schema::new(vec![
            Field::new("n", DataType::Integer, true),
            Field::new("enabled", DataType::Boolean, true),
        ]).unwrap();
        let invalid = Expr::Not(Box::new(Expr::Column("n".into())));
        assert!(invalid.output_type(&schema).is_err());
        assert!(Expr::Not(Box::new(Expr::Column("missing".into())))
            .output_type(&schema).is_err());
        let valid = Expr::Not(Box::new(Expr::Column("enabled".into())));
        assert_eq!(valid.output_type(&schema).unwrap(), DataType::Boolean);
        let nullable = Expr::Not(Box::new(Expr::Literal(Value::Null)));
        assert_eq!(nullable.output_type(&schema).unwrap(), DataType::Boolean);
    }
}

#[cfg(test)]
mod is_null_static_schema_validation_tests {
    use super::*;
    use crate::schema::Field;

    #[test]
    fn validates_inner_columns_even_when_input_batch_is_empty() {
        let schema = Schema::new(vec![
            Field::new("text", DataType::Text, true),
        ]).unwrap();
        let missing = Expr::IsNull(Box::new(Expr::Column("missing".into())));
        assert!(missing.output_type(&schema).is_err());
        let valid = Expr::IsNull(Box::new(Expr::Column("text".into())));
        assert_eq!(valid.output_type(&schema).unwrap(), DataType::Boolean);
        let literal = Expr::IsNull(Box::new(Expr::Literal(Value::Null)));
        assert_eq!(literal.output_type(&schema).unwrap(), DataType::Boolean);
    }
}

#[cfg(test)]
mod like_expression_type_validation_tests {
    use super::*;
    use crate::schema::Field;

    #[test]
    fn rejects_invalid_pattern_and_escape_types_before_execution() {
        let schema = Schema::new(vec![
            Field::new("s", DataType::Text, true),
            Field::new("n", DataType::Integer, false),
        ]).unwrap();
        let like = |value: Expr, pattern: Expr, escape: Option<Expr>| Expr::Like {
            value: Box::new(value), pattern: Box::new(pattern),
            escape: escape.map(Box::new),
        };
        assert!(like(Expr::Column("n".into()), Expr::Literal(Value::Text("%".into())), None)
            .output_type(&schema).is_err());
        assert!(like(Expr::Column("s".into()), Expr::Column("n".into()), None)
            .output_type(&schema).is_err());
        assert!(like(Expr::Column("s".into()), Expr::Literal(Value::Text("%".into())),
            Some(Expr::Literal(Value::Integer(1)))).output_type(&schema).is_err());
        assert_eq!(like(Expr::Column("s".into()), Expr::Literal(Value::Text("%".into())),
            None).output_type(&schema).unwrap(), DataType::Boolean);
        assert!(like(Expr::Column("missing".into()), Expr::Literal(Value::Text("%".into())), None)
            .output_type(&schema).is_err());
    }
}

#[cfg(test)]
mod distinct_comparison_static_type_tests {
    use super::*;
    use crate::schema::Field;

    #[test]
    fn validates_compatible_types_and_nullable_literals() {
        let schema = Schema::new(vec![
            Field::new("n", DataType::Integer, true),
            Field::new("f", DataType::Float, true),
            Field::new("t", DataType::Text, true),
        ]).unwrap();
        let compare = |left, right| Expr::IsDistinctFrom {
            left: Box::new(left), right: Box::new(right), negated: false,
        };
        assert_eq!(compare(Expr::Column("n".into()), Expr::Column("f".into()))
            .output_type(&schema).unwrap(), DataType::Boolean);
        assert_eq!(compare(Expr::Column("n".into()), Expr::Literal(Value::Null))
            .output_type(&schema).unwrap(), DataType::Boolean);
        assert!(compare(Expr::Column("n".into()), Expr::Column("t".into()))
            .output_type(&schema).is_err());
        assert!(compare(Expr::Column("missing".into()), Expr::Column("n".into()))
            .output_type(&schema).is_err());
    }
}

#[cfg(test)]
mod case_condition_type_checks {
    use super::*;
    use crate::schema::Field;

    #[test]
    fn invalid_case_when_conditions_fail_before_execution() {
        let schema = Schema::new(vec![Field::new("n", DataType::Integer, false)]).unwrap();
        let invalid = Expr::Case {
            branches: vec![(Expr::Column("n".into()), Expr::Literal(Value::Integer(1)))],
            else_expr: None,
        };
        assert!(invalid.output_type(&schema).is_err());
        let valid = Expr::Case {
            branches: vec![(Expr::Literal(Value::Boolean(true)),
                Expr::Literal(Value::Integer(1)))],
            else_expr: None,
        };
        assert_eq!(valid.output_type(&schema).unwrap(), DataType::Integer);
    }
}

#[cfg(test)]
mod logical_operator_plan_validation_tests {
    use super::*;
    use crate::schema::Field;

    #[test]
    fn boolean_operators_validate_both_inputs_on_empty_batches() {
        let schema = Schema::new(vec![
            Field::new("n", DataType::Integer, false),
            Field::new("ok", DataType::Boolean, true),
        ]).unwrap();
        for op in [BinaryOp::And, BinaryOp::Or] {
            assert!(Expr::binary(Expr::Column("ok".into()), op,
                Expr::Column("n".into())).output_type(&schema).is_err());
            assert!(Expr::binary(Expr::Column("missing".into()), op,
                Expr::Column("ok".into())).output_type(&schema).is_err());
            assert_eq!(Expr::binary(Expr::Literal(Value::Null), op,
                Expr::Column("ok".into())).output_type(&schema).unwrap(), DataType::Boolean);
        }
    }
}

#[cfg(test)]
mod binary_comparison_planning_tests {
    use super::*;
    use crate::schema::Field;

    #[test]
    fn incompatible_columns_fail_before_rows_are_evaluated() {
        let schema = Schema::new(vec![
            Field::new("count", DataType::Integer, true),
            Field::new("score", DataType::Float, true),
            Field::new("name", DataType::Text, true),
        ]).unwrap();
        for op in [BinaryOp::Equal, BinaryOp::NotEqual, BinaryOp::Less,
                   BinaryOp::LessEqual, BinaryOp::Greater, BinaryOp::GreaterEqual] {
            assert!(Expr::binary(Expr::Column("count".into()), op,
                Expr::Column("name".into())).output_type(&schema).is_err());
            assert!(Expr::binary(Expr::Column("missing".into()), op,
                Expr::Column("name".into())).output_type(&schema).is_err());
            assert_eq!(Expr::binary(Expr::Column("count".into()), op,
                Expr::Column("score".into())).output_type(&schema).unwrap(), DataType::Boolean);
            assert_eq!(Expr::binary(Expr::Column("count".into()), op,
                Expr::Literal(Value::Null)).output_type(&schema).unwrap(), DataType::Boolean);
        }
    }
}

#[cfg(test)]
mod in_list_static_choice_validation_tests {
    use super::*;
    use crate::schema::Field;

    #[test]
    fn null_needle_still_requires_valid_choice_columns() {
        let schema = Schema::new(vec![Field::new("n", DataType::Integer, true)]).unwrap();
        let invalid = Expr::InList {
            needle: Box::new(Expr::Literal(Value::Null)),
            choices: vec![Expr::Column("missing".into())],
            negated: false,
        };
        assert!(invalid.output_type(&schema).is_err());
        let valid = Expr::InList {
            needle: Box::new(Expr::Literal(Value::Null)),
            choices: vec![Expr::Column("n".into()), Expr::Literal(Value::Null)],
            negated: true,
        };
        assert_eq!(valid.output_type(&schema).unwrap(), DataType::Boolean);
    }
}

#[cfg(test)]
mod cast_expression_planning_validation_tests {
    use super::*;
    use crate::schema::Field;

    #[test]
    fn validates_source_columns_for_cast_and_try_cast() {
        let schema = Schema::new(vec![
            Field::new("s", DataType::Text, true),
        ]).unwrap();
        for try_mode in [false, true] {
            let make = |inner| if try_mode {
                Expr::TryCast { expression: Box::new(inner), target: DataType::Integer }
            } else {
                Expr::Cast { expression: Box::new(inner), target: DataType::Integer }
            };
            assert!(make(Expr::Column("absent".into())).output_type(&schema).is_err());
            assert_eq!(make(Expr::Column("s".into())).output_type(&schema).unwrap(),
                DataType::Integer);
        }
    }
}

#[cfg(test)]
mod like_anchored_fast_path_tests {
    use super::*;

    #[test]
    fn anchored_and_contains_patterns_match_unicode_and_escapes() {
        for (text, pattern, expected) in [
            ("naïve café ☕", "naïve%", true),
            ("naïve café ☕", "naive%", false),
            ("naïve café ☕", "%café ☕", true),
            ("naïve café ☕", "%cafe", false),
            ("naïve café ☕", "%café%", true),
            ("naïve café ☕", "%tea%", false),
            ("a%b", r"a\%%", true),
            ("a%b", r"a\%c%", false),
            ("anything", "%%%%", true),
            ("", "%%%%", true),
            ("", "%a%", false),
        ] {
            assert_eq!(sql_like(text, pattern).unwrap(), expected, "{text:?} LIKE {pattern:?}");
        }
    }

    #[test]
    fn redundant_wildcards_do_not_trigger_the_dp_work_limit() {
        let text = "🎯".repeat(10_000);
        let pattern = format!("%{}%", "%".repeat(5_000));
        assert!(sql_like(&text, &pattern).unwrap());
        assert!(sql_like_escape("a_b", r"a!_%", '!').unwrap());
        assert!(sql_like_escape("a_b", "a!", '!').is_err());
    }
}
