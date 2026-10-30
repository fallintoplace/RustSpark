//! Native log-odds transformation for probabilities strictly between 0 and 1.
use super::{float_type, Function};
use crate::{scalar::Value, Error, Result};

fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(x) = super::number(&args[0])? else { return Ok(Value::Null) };
    if !(0.0..1.0).contains(&x) || x == 0.0 {
        return Err(Error::Execution("LOGIT requires a probability strictly between 0 and 1".into()));
    }
    super::finite(x.ln() - (-x).ln_1p())
}

pub(super) const FUNCTIONS: &[Function] = &[Function {
    name: "logit", min_args: 1, max_args: 1,
    result_type: float_type, call: evaluate,
}];

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::DataType;
    #[test]
    fn log_odds_cover_domain_and_nulls() {
        assert_eq!(evaluate(&[Value::Float(0.5)]).unwrap(), Value::Float(0.0));
        assert_eq!(evaluate(&[Value::Null]).unwrap(), Value::Null);
        for bad in [0.0, 1.0, -0.01, 1.01, f64::NAN] {
            assert!(evaluate(&[Value::Float(bad)]).is_err());
        }
        assert!(float_type(&[DataType::Text]).is_err());
        assert!(crate::builtin::supports("LOGIT"));
    }
}
