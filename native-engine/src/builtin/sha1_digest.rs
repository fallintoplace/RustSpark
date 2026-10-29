use super::Function;
use crate::{scalar::Value, schema::DataType, Error, Result};
use sha1::{Digest, Sha1};

fn output(types: &[DataType]) -> Result<DataType> {
    super::text_type(types)
}

fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null); };
    if text.len() > 16 * 1024 * 1024 {
        return Err(Error::Execution("SHA1 input exceeds native 16 MiB budget".into()));
    }
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let digest = Sha1::digest(text.as_bytes());
    let mut hex = String::with_capacity(digest.len() * 2);
    for &byte in digest.iter() {
        hex.push(char::from(DIGITS[usize::from(byte >> 4)]));
        hex.push(char::from(DIGITS[usize::from(byte & 15)]));
    }
    Ok(Value::Text(hex))
}

pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "sha", min_args: 1, max_args: 1, result_type: output, call: evaluate },
    Function { name: "sha1", min_args: 1, max_args: 1, result_type: output, call: evaluate },
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sha1_matches_reference_hash_vectors_for_text() {
        assert_eq!(evaluate(&[Value::Text("abc".into())]).unwrap(),
            Value::Text("a9993e364706816aba3e25717850c26c9cd0d89d".into()));
        assert_eq!(evaluate(&[Value::Text("".into())]).unwrap(),
            Value::Text("da39a3ee5e6b4b0d3255bfef95601890afd80709".into()));
        assert_eq!(evaluate(&[Value::Text("Spark".into())]).unwrap(),
            Value::Text("85f5955f4b27a9a4c2aab6ffe5d7189fc298b92c".into()));
        assert_eq!(evaluate(&[Value::Null]).unwrap(), Value::Null);
        assert!(output(&[DataType::Integer]).is_err());
    }
}
