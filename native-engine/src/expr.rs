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
    Call { name: String, args: Vec<Expr> },
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
            Self::Not(_) | Self::IsNull(_) => Ok(DataType::Boolean),
            Self::Call { name, args } => crate::builtin::return_type(name, args, schema),
            Self::Binary { left, op, right } => match op {
                BinaryOp::Equal
                | BinaryOp::NotEqual
                | BinaryOp::Less
                | BinaryOp::LessEqual
                | BinaryOp::Greater
                | BinaryOp::GreaterEqual
                | BinaryOp::And
                | BinaryOp::Or => Ok(DataType::Boolean),
                BinaryOp::Divide => Ok(DataType::Float),
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
            Self::Call { name, args } if name.eq_ignore_ascii_case("COALESCE") => {
                for expr in args {
                    let value = expr.evaluate(batch, row)?;
                    if !value.is_null() { return Ok(value); }
                }
                Ok(Value::Null)
            }
            Self::Call { name, args } => {
                let values = args.iter().map(|expr| expr.evaluate(batch, row)).collect::<Result<Vec<_>>>()?;
                crate::builtin::evaluate(name, &values)
            },
            Self::Binary { left, op, right } => {
                let left = left.evaluate(batch, row)?;
                let right = right.evaluate(batch, row)?;
                evaluate_binary(&left, *op, &right)
            }
        }
    }
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
