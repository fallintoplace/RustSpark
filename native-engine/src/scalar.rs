use std::{
    cmp::Ordering,
    hash::{Hash, Hasher},
};

use crate::{Error, Result};

#[derive(Clone, Debug)]
pub enum Value {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    Text(String),
}

fn canonical_float(value: f64) -> u64 {
    if value.is_nan() {
        f64::NAN.to_bits()
    } else if value == 0.0 {
        0.0_f64.to_bits()
    } else {
        value.to_bits()
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Null, Self::Null) => true,
            (Self::Boolean(a), Self::Boolean(b)) => a == b,
            (Self::Integer(a), Self::Integer(b)) => a == b,
            (Self::Float(a), Self::Float(b)) => canonical_float(*a) == canonical_float(*b),
            (Self::Text(a), Self::Text(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for Value {}

impl Hash for Value {
    fn hash<H: Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            Self::Null => {}
            Self::Boolean(value) => value.hash(state),
            Self::Integer(value) => value.hash(state),
            Self::Float(value) => canonical_float(*value).hash(state),
            Self::Text(value) => value.hash(state),
        }
    }
}

// Compare an i64 and an f64 without rounding the integer to 53 bits.
fn compare_int_float(integer: i64, float: f64) -> Option<Ordering> {
    if float.is_nan() {
        return None;
    }
    // 2^63 is exactly representable as f64, unlike i64::MAX.
    if float >= 9_223_372_036_854_775_808.0 {
        return Some(Ordering::Less);
    }
    if float < -9_223_372_036_854_775_808.0 {
        return Some(Ordering::Greater);
    }
    let whole = float.trunc() as i64;
    Some(integer.cmp(&whole).then_with(|| {
        if float.fract() > 0.0 {
            Ordering::Less
        } else if float.fract() < 0.0 {
            Ordering::Greater
        } else {
            Ordering::Equal
        }
    }))
}

impl Value {
    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    pub fn as_boolean(&self) -> Result<Option<bool>> {
        match self {
            Self::Null => Ok(None),
            Self::Boolean(value) => Ok(Some(*value)),
            _ => Err(Error::Type("expected a SQL boolean".into())),
        }
    }

    pub fn sql_compare(&self, other: &Self) -> Result<Option<Ordering>> {
        match (self, other) {
            (Self::Null, _) | (_, Self::Null) => Ok(None),
            (Self::Integer(a), Self::Integer(b)) => Ok(Some(a.cmp(b))),
            (Self::Float(a), Self::Float(b)) => Ok(a.partial_cmp(b)),
            (Self::Text(a), Self::Text(b)) => Ok(Some(a.cmp(b))),
            (Self::Boolean(a), Self::Boolean(b)) => Ok(Some(a.cmp(b))),
            (Self::Integer(a), Self::Float(b)) => Ok(compare_int_float(*a, *b)),
            (Self::Float(a), Self::Integer(b)) => Ok(compare_int_float(*b, *a).map(Ordering::reverse)),
            _ => Err(Error::Type("cannot compare incompatible SQL values".into())),
        }
    }

    pub fn sql_equal(&self, other: &Self) -> Result<Option<bool>> {
        Ok(self.sql_compare(other)?.map(Ordering::is_eq))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn null_does_not_compare_equal_in_predicates() {
        assert_eq!(Value::Null.sql_equal(&Value::Null).unwrap(), None);
        assert_eq!(Value::Integer(1).sql_equal(&Value::Null).unwrap(), None);
    }

    #[test]
    fn keys_normalize_signed_zero_and_nan() {
        let mut values = HashSet::new();
        values.insert(Value::Float(-0.0));
        values.insert(Value::Float(0.0));
        values.insert(Value::Float(f64::NAN));
        values.insert(Value::Float(-f64::NAN));
        assert_eq!(values.len(), 2);
    }

    #[test]
    fn incompatible_values_are_rejected() {
        assert!(Value::Text("1".into()).sql_equal(&Value::Integer(1)).is_err());
    }
}


impl Value {
    /// Convert native scalar values to an explicit SQL column type.
    /// Invalid text, nonfinite floats and out-of-range numeric casts are errors.
    pub fn cast_to(&self, target: crate::schema::DataType) -> Result<Self> {
        use crate::schema::DataType;
        match (self, target) {
            (Self::Null, _) => Ok(Self::Null),
            (Self::Boolean(value), DataType::Boolean) => Ok(Self::Boolean(*value)),
            (Self::Integer(value), DataType::Integer) => Ok(Self::Integer(*value)),
            (Self::Float(value), DataType::Float) => Ok(Self::Float(*value)),
            (Self::Text(value), DataType::Text) => Ok(Self::Text(value.clone())),
            (Self::Boolean(value), DataType::Text) => Ok(Self::Text(value.to_string())),
            (Self::Integer(value), DataType::Text) => Ok(Self::Text(value.to_string())),
            (Self::Float(value), DataType::Text) => Ok(Self::Text(value.to_string())),
            (Self::Integer(value), DataType::Float) => Ok(Self::Float(*value as f64)),
            (Self::Float(value), DataType::Integer) => {
                if !value.is_finite() || *value < i64::MIN as f64 ||
                    *value >= -(i64::MIN as f64) {
                    return Err(Error::Type("float is outside BIGINT range".into()));
                }
                Ok(Self::Integer(value.trunc() as i64))
            }
            (Self::Text(value), DataType::Integer) => value.parse::<i64>()
                .map(Self::Integer).map_err(|_| Error::Type("invalid BIGINT cast".into())),
            (Self::Text(value), DataType::Float) => value.parse::<f64>().ok()
                .filter(|n| n.is_finite()).map(Self::Float)
                .ok_or_else(|| Error::Type("invalid DOUBLE cast".into())),
            (Self::Text(value), DataType::Boolean) if value.eq_ignore_ascii_case("true") =>
                Ok(Self::Boolean(true)),
            (Self::Text(value), DataType::Boolean) if value.eq_ignore_ascii_case("false") =>
                Ok(Self::Boolean(false)),
            _ => Err(Error::Type("unsupported native CAST conversion".into())),
        }
    }
}

#[cfg(test)]
mod native_cast_feature_tests {
    use super::*;
    use crate::schema::DataType;

    #[test]
    fn converts_types_and_rejects_overflow() {
        assert_eq!(Value::Text("42".into()).cast_to(DataType::Integer).unwrap(), Value::Integer(42));
        assert_eq!(Value::Float(-2.9).cast_to(DataType::Integer).unwrap(), Value::Integer(-2));
        assert_eq!(Value::Null.cast_to(DataType::Boolean).unwrap(), Value::Null);
        assert!(Value::Float(f64::INFINITY).cast_to(DataType::Integer).is_err());
        assert!(Value::Float(i64::MAX as f64).cast_to(DataType::Integer).is_err());
        assert!(Value::Text("x".into()).cast_to(DataType::Integer).is_err());
    }
#[cfg(test)]
mod exact_numeric_comparison_tests {
    use super::*;

    #[test]
    fn compares_large_integers_without_floating_point_rounding() {
        assert_eq!(Value::Integer(9_007_199_254_740_993)
            .sql_compare(&Value::Float(9_007_199_254_740_992.0)).unwrap(),
            Some(Ordering::Greater));
        assert_eq!(Value::Integer(i64::MAX)
            .sql_compare(&Value::Float(9_223_372_036_854_775_808.0)).unwrap(),
            Some(Ordering::Less));
        assert_eq!(Value::Float(-0.5)
            .sql_compare(&Value::Integer(0)).unwrap(), Some(Ordering::Less));
        assert_eq!(Value::Float(f64::NAN)
            .sql_compare(&Value::Integer(0)).unwrap(), None);
    }
}
