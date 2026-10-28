//! Conservative metadata predicate pruning for native column scans.
use std::cmp::Ordering;

use crate::{
    expr::{BinaryOp, Expr},
    scalar::Value,
    schema::Schema,
    statistics::{ColumnStats, TableStats},
    Error, Result,
};

fn reverse(op: BinaryOp) -> BinaryOp {
    match op {
        BinaryOp::Less => BinaryOp::Greater,
        BinaryOp::LessEqual => BinaryOp::GreaterEqual,
        BinaryOp::Greater => BinaryOp::Less,
        BinaryOp::GreaterEqual => BinaryOp::LessEqual,
        other => other,
    }
}

fn excludes(column: &ColumnStats, op: BinaryOp, literal: &Value) -> bool {
    if column.row_count == column.null_count { return true; }
    if literal.is_null() { return true; }
    let compare = |candidate: &Value| candidate.sql_compare(literal).ok().flatten();
    let min = column.min.as_ref().and_then(compare);
    let max = column.max.as_ref().and_then(compare);
    match op {
        BinaryOp::Equal => {
            min.is_some_and(|ord| ord == Ordering::Greater)
                || max.is_some_and(|ord| ord == Ordering::Less)
        }
        BinaryOp::NotEqual => {
            min == Some(Ordering::Equal) && max == Some(Ordering::Equal)
        }
        BinaryOp::Less => min.is_some_and(|ord| ord != Ordering::Less),
        BinaryOp::LessEqual => min.is_some_and(|ord| ord == Ordering::Greater),
        BinaryOp::Greater => max.is_some_and(|ord| ord != Ordering::Greater),
        BinaryOp::GreaterEqual => max.is_some_and(|ord| ord == Ordering::Less),
        _ => false,
    }
}

/// True only if metadata proves no row can satisfy the predicate.
/// Unknown expressions always return false rather than risking lost data.
pub fn can_prune(schema: &Schema, stats: &TableStats, predicate: &Expr) -> Result<bool> {
    if stats.columns.len() != schema.len() {
        return Err(Error::Schema("statistics do not match scan schema".into()));
    }
    if stats.row_count == 0 { return Ok(true); }
    match predicate {
        Expr::Binary { left, op: BinaryOp::And, right } => {
            Ok(can_prune(schema, stats, left)? || can_prune(schema, stats, right)?)
        }
        Expr::Binary { left, op: BinaryOp::Or, right } => {
            Ok(can_prune(schema, stats, left)? && can_prune(schema, stats, right)?)
        }
        Expr::Binary { left, op, right } => {
            if let (Expr::Column(name), Expr::Literal(value)) = (left.as_ref(), right.as_ref()) {
                return Ok(excludes(&stats.columns[schema.column(name)?], *op, value));
            }
            if let (Expr::Literal(value), Expr::Column(name)) = (left.as_ref(), right.as_ref()) {
                return Ok(excludes(&stats.columns[schema.column(name)?], reverse(*op), value));
            }
            Ok(false)
        }
        Expr::IsNull(inner) => {
            if let Expr::Column(name) = inner.as_ref() {
                let column = &stats.columns[schema.column(name)?];
                return Ok(column.null_count == 0);
            }
            Ok(false)
        }
        Expr::Not(inner) => {
            if let Expr::IsNull(column) = inner.as_ref() {
                if let Expr::Column(name) = column.as_ref() {
                    let column = &stats.columns[schema.column(name)?];
                    return Ok(column.null_count == column.row_count);
                }
            }
            Ok(false)
        }
        _ => Ok(false),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        batch::Batch, scalar::Value,
        schema::{DataType, Field, Schema},
        statistics,
    };

    #[test]
    fn only_prunes_when_bounds_prove_no_match() {
        let batch = Batch::from_rows(
            Schema::new(vec![Field::new("amount", DataType::Integer, true)]).unwrap(),
            vec![vec![Value::Integer(10)], vec![Value::Integer(20)], vec![Value::Null]]
        ).unwrap();
        let stats = statistics::collect(&batch, 10).unwrap();
        let predicate = |op, value| Expr::binary(
            Expr::Column("amount".into()), op, Expr::Literal(Value::Integer(value))
        );
        assert!(can_prune(batch.schema(), &stats, &predicate(BinaryOp::Greater, 100)).unwrap());
        assert!(can_prune(batch.schema(), &stats, &predicate(BinaryOp::Less, 0)).unwrap());
        assert!(!can_prune(batch.schema(), &stats, &predicate(BinaryOp::Equal, 10)).unwrap());
        assert!(can_prune(batch.schema(), &stats,
            &Expr::binary(predicate(BinaryOp::Greater, 100), BinaryOp::And,
                predicate(BinaryOp::Less, 100))).unwrap());
        assert!(!can_prune(batch.schema(), &stats,
            &Expr::binary(predicate(BinaryOp::Greater, 100), BinaryOp::Or,
                predicate(BinaryOp::Equal, 10))).unwrap());
        assert!(!can_prune(batch.schema(), &stats,
            &Expr::IsNull(Box::new(Expr::Column("amount".into())))).unwrap());
    }
}
