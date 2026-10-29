use super::Function;
use crate::{scalar::Value, schema::DataType, Error, Result};
use sha2::{Digest, Sha224, Sha256, Sha384, Sha512};

fn output(types: &[DataType]) -> Result<DataType> {
    if types == [DataType::Text, DataType::Integer] { Ok(DataType::Text) }
    else { Err(Error::Type("SHA2 requires text and integer bit length".into())) }
}

fn lowercase_hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut hex = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        hex.push(char::from(DIGITS[usize::from(byte >> 4)]));
        hex.push(char::from(DIGITS[usize::from(byte & 15)]));
    }
    hex
}

fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null); };
    let Some(bits) = super::integer(&args[1])? else { return Ok(Value::Null); };
    if text.len() > 16 * 1024 * 1024 {
        return Err(Error::Execution("SHA2 input exceeds native 16 MiB budget".into()));
    }
    let hex = match bits {
        0 | 256 => lowercase_hex(&Sha256::digest(text.as_bytes())),
        224 => lowercase_hex(&Sha224::digest(text.as_bytes())),
        384 => lowercase_hex(&Sha384::digest(text.as_bytes())),
        512 => lowercase_hex(&Sha512::digest(text.as_bytes())),
        _ => return Ok(Value::Null),
    };
    Ok(Value::Text(hex))
}

pub(super) const FUNCTIONS: &[Function] = &[Function {
    name: "sha2", min_args: 2, max_args: 2, result_type: output, call: evaluate,
}];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_sha2_reference_vectors_and_accepts_zero_for_sha256() {
        let abc = Value::Text("abc".into());
        assert_eq!(evaluate(&[abc.clone(), Value::Integer(256)]).unwrap(),
            Value::Text("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad".into()));
        assert_eq!(evaluate(&[abc.clone(), Value::Integer(0)]).unwrap(),
            evaluate(&[abc.clone(), Value::Integer(256)]).unwrap());
        assert_eq!(evaluate(&[abc.clone(), Value::Integer(224)]).unwrap(),
            Value::Text("23097d223405d8228642a477bda255b32aadbce4bda0b3f7e36c9da7".into()));
        assert_eq!(evaluate(&[abc.clone(), Value::Integer(384)]).unwrap(),
            Value::Text("cb00753f45a35e8bb5a03d699ac65007272c32ab0eded1631a8b605a43ff5bed8086072ba1e7cc2358baeca134c825a7".into()));
        assert_eq!(evaluate(&[abc.clone(), Value::Integer(512)]).unwrap(),
            Value::Text("ddaf35a193617abacc417349ae20413112e6fa4e89a97ea20a9eeee64b55d39a2192992a274fc1a836ba3c23a3feebbd454d4423643ce80e2a9ac94fa54ca49f".into()));
        assert_eq!(evaluate(&[abc.clone(), Value::Integer(128)]).unwrap(), Value::Null);
        assert_eq!(evaluate(&[Value::Null, Value::Integer(256)]).unwrap(), Value::Null);
    }
}
