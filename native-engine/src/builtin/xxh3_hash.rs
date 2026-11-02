use super::Function;
use crate::{scalar::Value, schema::DataType, Error, Result};

const MAX_HASH_INPUT_BYTES: usize = 16 * 1024 * 1024;

fn text_argument(types: &[DataType]) -> Result<DataType> {
    if types == [DataType::Text] { Ok(DataType::Text) }
    else { Err(Error::Type("XXH3 requires a string argument".into())) }
}

fn bigint_argument(types: &[DataType]) -> Result<DataType> {
    text_argument(types)?;
    Ok(DataType::Integer)
}

fn input(args: &[Value]) -> Result<Option<&[u8]>> {
    let Some(text) = super::string(&args[0])? else { return Ok(None); };
    if text.len() > MAX_HASH_INPUT_BYTES {
        return Err(Error::Execution("XXH3 hash input exceeds 16 MiB".into()));
    }
    Ok(Some(text.as_bytes()))
}

/// XXH3_64 returns the signed 64-bit representation of the seed-zero digest.
fn xxh3_64(args: &[Value]) -> Result<Value> {
    Ok(match input(args)? {
        Some(bytes) => Value::Integer(xxhash_rust::xxh3::xxh3_64(bytes) as i64),
        None => Value::Null,
    })
}

// Render sixteen digest bytes directly into a 32-byte lowercase hex buffer.
// Formatting a u128 invokes the general integer formatting machinery per row.
fn hex_128(digest: u128) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut output = [0_u8; 32];
    for (pair, byte) in output.chunks_exact_mut(2).zip(digest.to_be_bytes()) {
        pair[0] = DIGITS[(byte >> 4) as usize];
        pair[1] = DIGITS[(byte & 15) as usize];
    }
    String::from_utf8(output.to_vec()).expect("hex output is ASCII")
}

/// XXH3_128 renders the full 128-bit digest in fixed-width hexadecimal.
fn xxh3_128(args: &[Value]) -> Result<Value> {
    match input(args)? {
        Some(bytes) => {
            let digest = xxhash_rust::xxh3::xxh3_128(bytes);
            Ok(Value::Text(hex_128(digest)))
        }
        None => Ok(Value::Null),
    }
}

pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "xxh3_64", min_args: 1, max_args: 1,
        result_type: bigint_argument, call: xxh3_64 },
    Function { name: "xxh3_128", min_args: 1, max_args: 1,
        result_type: text_argument, call: xxh3_128 },
];

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{catalog::Catalog, executor, sql};

    #[test]
    fn matches_spark_xxh3_reference_vectors() {
        let spark = [Value::Text("Spark".into())];
        assert_eq!(xxh3_64(&spark).unwrap(), Value::Integer(80997306238743657));
        assert_eq!(xxh3_128(&spark).unwrap(),
            Value::Text("7d57dd84c60c86ca1f4e82ab91a12b5e".into()));
        assert_eq!(xxh3_64(&[Value::Null]).unwrap(), Value::Null);
        assert_eq!(xxh3_128(&[Value::Null]).unwrap(), Value::Null);
    }

    #[test]
    fn hashes_utf8_and_rejects_oversized_inputs() {
        let value = [Value::Text("café🙂".into())];
        let first = xxh3_128(&value).unwrap();
        let Value::Text(digest) = first else { panic!("expected a hexadecimal digest") };
        assert_eq!(digest.len(), 32);
        assert!(digest.bytes().all(|byte| byte.is_ascii_hexdigit()));
        assert_eq!(xxh3_128(&value).unwrap(), Value::Text(digest));
        assert_eq!(xxh3_64(&value).unwrap(), xxh3_64(&value).unwrap());
        assert!(xxh3_64(&[Value::Text("x".repeat(MAX_HASH_INPUT_BYTES + 1))]).is_err());
        assert!(text_argument(&[DataType::Integer]).is_err());
    }

    #[test]
    fn is_available_through_native_sql() {
        let query = sql::parse_sql("SELECT XXH3_64('Spark'), XXH3_128('Spark')").unwrap();
        assert_eq!(executor::execute(&query, &Catalog::new()).unwrap().rows().unwrap(),
            vec![vec![
                Value::Integer(80997306238743657),
                Value::Text("7d57dd84c60c86ca1f4e82ab91a12b5e".into()),
            ]]);
    }
}

#[cfg(test)]
mod direct_xxh3_hex_tests {
    use super::*;

    #[test]
    fn fixed_width_hex_matches_reference_formatting() {
        for digest in [0_u128, 1, u128::MAX, u128::MAX >> 4,
            0x0123_4567_89ab_cdef_fedc_ba98_7654_3210] {
            assert_eq!(hex_128(digest), format!("{digest:032x}"));
        }
        for text in ["", "Spark", "é🙂", "x".repeat(4096).as_str()] {
            let digest = xxhash_rust::xxh3::xxh3_128(text.as_bytes());
            assert_eq!(xxh3_128(&[Value::Text(text.into())]).unwrap(),
                Value::Text(format!("{digest:032x}")));
        }
        assert_eq!(xxh3_128(&[Value::Null]).unwrap(), Value::Null);
    }
}
