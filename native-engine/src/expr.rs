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
    Like { value: Box<Expr>, pattern: Box<Expr> },
    Case { branches: Vec<(Expr, Expr)>, else_expr: Option<Box<Expr>> },
    Call { name: String, args: Vec<Expr> },
    TryCast { expression: Box<Expr>, target: DataType },
    Cast { expression: Box<Expr>, target: DataType },
    IsDistinctFrom { left: Box<Expr>, right: Box<Expr>, negated: bool },
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
            Self::Not(inner) => {
                if matches!(inner.as_ref(), Expr::Literal(Value::Null))
                    || inner.output_type(schema)? == DataType::Boolean {
                    Ok(DataType::Boolean)
                } else {
                    Err(Error::Type("NOT requires a boolean operand".into()))
                }
            },
            Self::IsNull(_) | Self::Like { .. } | Self::IsDistinctFrom { .. } =>
                Ok(DataType::Boolean),
            Self::TryCast { target, .. } | Self::Cast { target, .. } => Ok(*target),
            Self::Call { name, args } => crate::functions::output_type(name, args, schema),
            Self::Case { branches, else_expr } => {
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
                | BinaryOp::GreaterEqual
                | BinaryOp::And
                | BinaryOp::Or => Ok(DataType::Boolean),
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
            Self::Like { value, pattern } => match (
                value.evaluate(batch, row)?, pattern.evaluate(batch, row)?
            ) {
                (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                (Value::Text(value), Value::Text(pattern)) =>
                    Ok(Value::Boolean(sql_like(&value, &pattern)?)),
                _ => Err(Error::Type("LIKE requires text values".into())),
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
fn sql_like(input: &str, pattern: &str) -> Result<bool> {
    let text: Vec<char> = input.chars().collect();
    let pattern: Vec<char> = pattern.chars().collect();
    if (text.len() + 1).checked_mul(pattern.len() + 1).is_none_or(|cells| cells > 8_000_000) {
        return Err(Error::Execution("LIKE pattern exceeds work budget".into()));
    }
    let mut prev = vec![false; text.len() + 1];
    prev[0] = true;
    for symbol in pattern {
        let mut next = vec![false; text.len() + 1];
        if symbol == '%' {
            next[0] = prev[0];
            for index in 1..=text.len() {
                next[index] = prev[index] || next[index - 1];
            }
        } else {
            for index in 1..=text.len() {
                next[index] = prev[index - 1]
                    && (symbol == '_' || symbol == text[index - 1]);
            }
        }
        prev = next;
    }
    Ok(prev[text.len()])
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

    fn batch() -> Batch {
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
mod not_type_preflight_regression {
    use super::*;
    use crate::schema::Field;

    #[test]
    fn not_rejects_integer_and_unknown_columns_even_without_rows() {
        let schema = Schema::new(vec![
            Field::new("n", DataType::Integer, false),
            Field::new("flag", DataType::Boolean, true),
        ]).unwrap();
        assert!(Expr::Not(Box::new(Expr::Column("n".into()))).output_type(&schema).is_err());
        assert!(Expr::Not(Box::new(Expr::Column("missing".into()))).output_type(&schema).is_err());
        assert_eq!(Expr::Not(Box::new(Expr::Column("flag".into())))
            .output_type(&schema).unwrap(), DataType::Boolean);
        assert_eq!(Expr::Not(Box::new(Expr::Literal(Value::Null)))
            .output_type(&schema).unwrap(), DataType::Boolean);
    }
}
