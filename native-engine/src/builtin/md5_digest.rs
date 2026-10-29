use super::Function;
use crate::{scalar::Value, schema::DataType, Error, Result};
use md5::{Digest, Md5};

fn output(types: &[DataType]) -> Result<DataType> {
    super::text_type(types)
}

fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null); };
    if text.len() > 16 * 1024 * 1024 {
        return Err(Error::Execution("MD5 input exceeds native 16 MiB budget".into()));
    }
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let digest = Md5::digest(text.as_bytes());
    let mut hex = String::with_capacity(digest.len() * 2);
    for &byte in digest.iter() {
        hex.push(char::from(DIGITS[usize::from(byte >> 4)]));
        hex.push(char::from(DIGITS[usize::from(byte & 15)]));
    }
    Ok(Value::Text(hex))
}

pub(super) const FUNCTIONS: &[Function] = &[Function {
    name: "md5", min_args: 1, max_args: 1, result_type: output, call: evaluate,
}];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn md5_matches_reference_hash_vectors_for_text() {
        assert_eq!(evaluate(&[Value::Text("abc".into())]).unwrap(),
            Value::Text("900150983cd24fb0d6963f7d28e17f72".into()));
        assert_eq!(evaluate(&[Value::Text("".into())]).unwrap(),
            Value::Text("d41d8cd98f00b204e9800998ecf8427e".into()));
        assert_eq!(evaluate(&[Value::Text("Spark".into())]).unwrap(),
            Value::Text("8cde774d6f7333752ed72cacddb05126".into()));
        assert_eq!(evaluate(&[Value::Null]).unwrap(), Value::Null);
        assert!(output(&[DataType::Integer]).is_err());
    }
}
