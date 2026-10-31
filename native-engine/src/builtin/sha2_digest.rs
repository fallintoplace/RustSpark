use sha2::{Digest, Sha224, Sha256, Sha384, Sha512};
use super::Function;
use crate::{scalar::Value, schema::DataType, Error, Result};

fn output(types: &[DataType]) -> Result<DataType> {
    if types == [DataType::Text, DataType::Integer] {
        Ok(DataType::Text)
    } else {
        Err(Error::Type("SHA2 requires a string and an integer bit length".into()))
    }
}

const LOWER_HEX_PAIRS: [[u8; 2]; 256] = {
    let digits = b"0123456789abcdef";
    let mut pairs = [[0_u8; 2]; 256];
    let mut index = 0;
    while index < 256 {
        pairs[index] = [digits[index >> 4], digits[index & 15]];
        index += 1;
    }
    pairs
};

fn lower_hex<B: AsRef<[u8]>>(digest: B) -> String {
    let bytes = digest.as_ref();
    let mut output = Vec::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        output.extend_from_slice(&LOWER_HEX_PAIRS[usize::from(byte)]);
    }
    String::from_utf8(output).expect("hex pairs contain only ASCII")
}

fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(input) = super::string(&args[0])? else { return Ok(Value::Null); };
    let Some(bits) = super::integer(&args[1])? else { return Ok(Value::Null); };
    if input.len() > 16 * 1024 * 1024 {
        return Err(Error::Execution("SHA2 input exceeds 16 MiB budget".into()));
    }
    let digest = match bits {
        224 => lower_hex(Sha224::digest(input.as_bytes())),
        0 | 256 => lower_hex(Sha256::digest(input.as_bytes())),
        384 => lower_hex(Sha384::digest(input.as_bytes())),
        512 => lower_hex(Sha512::digest(input.as_bytes())),
        // Spark returns NULL for unsupported digest widths.
        _ => return Ok(Value::Null),
    };
    Ok(Value::Text(digest))
}

pub(super) const FUNCTIONS: &[Function] = &[Function {
    name: "sha2", min_args: 2, max_args: 2, result_type: output, call: evaluate,
}];

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{batch::Batch, catalog::Catalog, executor, schema::{Field, Schema}, sql};

    #[test]
    fn matches_sha2_reference_vectors_across_all_variants() {
        let text = Value::Text("abc".into());
        let calc = |bits| evaluate(&[text.clone(), Value::Integer(bits)]).unwrap();
        assert_eq!(calc(224), Value::Text(
            "23097d223405d8228642a477bda255b32aadbce4bda0b3f7e36c9da7".into()));
        assert_eq!(calc(256), Value::Text(
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad".into()));
        assert_eq!(calc(384), Value::Text(
            "cb00753f45a35e8bb5a03d699ac65007272c32ab0eded1631a8b605a43ff5bed8086072ba1e7cc2358baeca134c825a7".into()));
        assert_eq!(calc(512), Value::Text(
            "ddaf35a193617abacc417349ae20413112e6fa4e89a97ea20a9eeee64b55d39a2192992a274fc1a836ba3c23a3feebbd454d4423643ce80e2a9ac94fa54ca49f".into()));
        assert_eq!(calc(0), calc(256));
        assert_eq!(calc(128), Value::Null);
        assert_eq!(evaluate(&[Value::Null, Value::Integer(256)]).unwrap(), Value::Null);
        assert_eq!(evaluate(&[text, Value::Null]).unwrap(), Value::Null);
        assert!(output(&[DataType::Integer, DataType::Integer]).is_err());
    }

    #[test]
    fn computes_sha2_over_nullable_sql_columns() {
        let schema = Schema::new(vec![Field::new("message", DataType::Text, true)]).unwrap();
        let mut catalog = Catalog::new();
        catalog.register("t", Batch::from_rows(schema,
            vec![vec![Value::Text("Spark".into())], vec![Value::Null]]).unwrap()).unwrap();
        let plan = sql::parse_sql("SELECT SHA2(message, 256) AS digest FROM t").unwrap();
        assert_eq!(executor::execute(&plan, &catalog).unwrap().rows().unwrap(), vec![
            vec![Value::Text("529bc3b07127ecb7e53a4dcf1991d9152c24537d919178022b2c42657f79a26b".into())],
            vec![Value::Null],
        ]);
    }
}

#[cfg(test)]
mod sha2_hex_pair_encoding_regressions {
    use super::*;

    #[test]
    fn all_digest_widths_match_reference_character_formatting() {
        let source = "é🙂row".repeat(257);
        for bits in [224, 256, 384, 512] {
            let digest = match bits {
                224 => Sha224::digest(source.as_bytes()).to_vec(),
                256 => Sha256::digest(source.as_bytes()).to_vec(),
                384 => Sha384::digest(source.as_bytes()).to_vec(),
                512 => Sha512::digest(source.as_bytes()).to_vec(),
                _ => unreachable!(),
            };
            let expected = digest.iter().map(|byte| format!("{byte:02x}"))
                .collect::<String>();
            assert_eq!(lower_hex(&digest), expected);
            assert_eq!(evaluate(&[Value::Text(source.clone()), Value::Integer(bits)]).unwrap(),
                Value::Text(expected));
        }
        assert_eq!(evaluate(&[Value::Text("abc".into()), Value::Integer(0)]).unwrap(),
            evaluate(&[Value::Text("abc".into()), Value::Integer(256)]).unwrap());
        assert_eq!(evaluate(&[Value::Null, Value::Integer(256)]).unwrap(), Value::Null);
    }
}
