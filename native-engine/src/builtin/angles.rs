use super::{float_type, float_result, Function};
use crate::{scalar::Value, Result};

fn radians(args: &[Value]) -> Result<Value> {
    match super::number(&args[0])? {
        Some(n) => super::finite(n.to_radians()),
        None => Ok(Value::Null),
    }
}
fn degrees(args: &[Value]) -> Result<Value> {
    match super::number(&args[0])? {
        Some(n) => super::finite(n.to_degrees()),
        None => Ok(Value::Null),
    }
}
fn pi(_: &[Value]) -> Result<Value> { Ok(Value::Float(std::f64::consts::PI)) }
fn e(_: &[Value]) -> Result<Value> { Ok(Value::Float(std::f64::consts::E)) }
pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "radians", min_args: 1, max_args: 1, result_type: float_type, call: radians },
    Function { name: "degrees", min_args: 1, max_args: 1, result_type: float_type, call: degrees },
    Function { name: "pi", min_args: 0, max_args: 0, result_type: float_result, call: pi },
    Function { name: "e", min_args: 0, max_args: 0, result_type: float_result, call: e },
];
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn native_angle_and_constant_functions() {
        let Value::Float(angle) = radians(&[Value::Integer(180)]).unwrap() else { panic!("float"); };
        assert!((angle - std::f64::consts::PI).abs() < 1e-12);
        assert_eq!(degrees(&[Value::Integer(0)]).unwrap(), Value::Float(0.0));
        assert_eq!(pi(&[]).unwrap(), Value::Float(std::f64::consts::PI));
        assert_eq!(degrees(&[Value::Null]).unwrap(), Value::Null);
    }
}
