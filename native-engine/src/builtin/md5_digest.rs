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
    const HEX_PAIRS: [[u8; 2]; 256] = {
        let digits = b"0123456789abcdef";
        let mut pairs = [[0_u8; 2]; 256];
        let mut index = 0;
        while index < 256 {
            pairs[index] = [digits[index >> 4], digits[index & 15]];
            index += 1;
        }
        pairs
    };
    let digest = Md5::digest(text.as_bytes());
    let mut hex = Vec::with_capacity(digest.len() * 2);
    for &byte in digest.iter() {
        hex.extend_from_slice(&HEX_PAIRS[usize::from(byte)]);
    }
    Ok(Value::Text(String::from_utf8(hex)
        .expect("hex digest pairs are ASCII")))
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

#[cfg(test)]
mod md5_hex_pair_encoding_regressions {
    use super::*;

    #[test]
    fn byte_pairs_match_known_digests_and_reference_formatter() {
        let long = "a long field".repeat(2048);
        for text in ["", "abc", "é🙂", long.as_str()] {
            let digest = Md5::digest(text.as_bytes());
            let expected = digest.iter().map(|byte| format!("{byte:02x}"))
                .collect::<String>();
            assert_eq!(evaluate(&[Value::Text(text.into())]).unwrap(), Value::Text(expected));
        }
        assert_eq!(evaluate(&[Value::Text("abc".into())]).unwrap(),
            Value::Text("900150983cd24fb0d6963f7d28e17f72".into()));
        assert_eq!(evaluate(&[Value::Null]).unwrap(), Value::Null);
    }
}
