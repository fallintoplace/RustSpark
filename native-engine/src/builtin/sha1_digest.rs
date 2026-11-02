use sha1::{Digest, Sha1};
use super::Function;
use crate::{scalar::Value, schema::DataType, Error, Result};

fn output(types: &[DataType]) -> Result<DataType> { super::text_type(types) }

fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(input) = super::string(&args[0])? else { return Ok(Value::Null); };
    if input.len() > 16 * 1024 * 1024 {
        return Err(Error::Execution("SHA1 input exceeds native 16 MiB budget".into()));
    }
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let digest = Sha1::digest(input.as_bytes());
    let mut bytes = [0_u8; 40];
    for (index, byte) in digest.iter().enumerate() {
        bytes[2 * index] = DIGITS[(byte >> 4) as usize];
        bytes[2 * index + 1] = DIGITS[(byte & 15) as usize];
    }
    Ok(Value::Text(String::from_utf8(bytes.to_vec())
        .expect("SHA1 hex digits are ASCII")))
}

const HASH_TEXT_BUDGET: usize = 16 * 1024 * 1024;

// Hash one complete Murmur3 word without a chunk iterator.
#[inline]
fn murmur_block(hash: u32, word: u32) -> u32 {
    let mixed = word.wrapping_mul(0xCC9E_2D51)
        .rotate_left(15).wrapping_mul(0x1B87_3593);
    (hash ^ mixed).rotate_left(13)
        .wrapping_mul(5).wrapping_add(0xE654_6B64)
}

#[inline]
fn murmur_finish(mut hash: u32, size: u32) -> u32 {
    hash ^= size;
    hash ^= hash >> 16;
    hash = hash.wrapping_mul(0x85EB_CA6B);
    hash ^= hash >> 13;
    hash = hash.wrapping_mul(0xC2B2_AE35);
    hash ^ (hash >> 16)
}

#[inline]
fn murmur_u32(value: u32, seed: u32) -> u32 {
    murmur_finish(murmur_block(seed, value), 4)
}

#[inline]
fn murmur_u64(value: u64, seed: u32) -> u32 {
    let hash = murmur_block(seed, value as u32);
    murmur_finish(murmur_block(hash, (value >> 32) as u32), 8)
}

// MurmurHash3 x86_32, including Spark's 32-bit seed and final mixing.
fn murmur3_x86_32(input: &[u8], seed: u32) -> u32 {
    const C1: u32 = 0xCC9E_2D51;
    const C2: u32 = 0x1B87_3593;
    let mut hash = seed;
    let mut chunks = input.chunks_exact(4);
    for chunk in &mut chunks {
        let key = u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
        hash = murmur_block(hash, key);
    }
    let tail = chunks.remainder();
    if !tail.is_empty() {
        let mut key = 0_u32;
        for (position, &byte) in tail.iter().enumerate() {
            key |= u32::from(byte) << (position * 8);
        }
        key = key.wrapping_mul(C1).rotate_left(15).wrapping_mul(C2);
        hash ^= key;
    }
    murmur_finish(hash, input.len() as u32)
}

fn spark_hash_type(types: &[DataType]) -> Result<DataType> {
    if types.iter().all(|kind| matches!(kind,
        DataType::Boolean | DataType::Integer | DataType::Float | DataType::Text)) {
        Ok(DataType::Integer)
    } else {
        Err(Error::Type("HASH requires scalar SQL values".into()))
    }
}

fn spark_hash(args: &[Value]) -> Result<Value> {
    let mut seed = 42_u32;
    let mut text_bytes = 0_usize;
    for value in args {
        seed = match value {
            Value::Null => seed,
            Value::Boolean(value) => murmur_u32(u32::from(*value), seed),
            Value::Integer(value) => murmur_u64(*value as u64, seed),
            Value::Float(value) => {
                let bits = if *value == 0.0 { 0.0_f64.to_bits() }
                    else if value.is_nan() { f64::NAN.to_bits() }
                    else { value.to_bits() };
                murmur_u64(bits, seed)
            }
            Value::Text(value) => {
                text_bytes = text_bytes.checked_add(value.len())
                    .filter(|&size| size <= HASH_TEXT_BUDGET)
                    .ok_or_else(|| Error::Execution("HASH text exceeds 16 MiB budget".into()))?;
                murmur3_x86_32(value.as_bytes(), seed)
            }
        };
    }
    Ok(Value::Integer(i64::from(seed as i32)))
}

pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "sha", min_args: 1, max_args: 1, result_type: output, call: evaluate },
    Function { name: "sha1", min_args: 1, max_args: 1, result_type: output, call: evaluate },
    Function { name: "hash", min_args: 1, max_args: 64,
        result_type: spark_hash_type, call: spark_hash },
];

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{catalog::Catalog, executor, sql};
    #[test]
    fn sha_and_sha1_match_spark_vectors() {
        for (plain, expected) in [
            ("", "da39a3ee5e6b4b0d3255bfef95601890afd80709"),
            ("abc", "a9993e364706816aba3e25717850c26c9cd0d89d"),
            ("Spark", "85f5955f4b27a9a4c2aab6ffe5d7189fc298b92c"),
        ] {
            assert_eq!(evaluate(&[Value::Text(plain.into())]).unwrap(),
                Value::Text(expected.into()));
        }
        assert!(crate::builtin::supports("sha"));
        assert!(crate::builtin::supports("sha1"));
        assert_eq!(evaluate(&[Value::Null]).unwrap(), Value::Null);
        assert!(evaluate(&[Value::Text("x".repeat(16 * 1024 * 1024 + 1))]).is_err());
        let query = sql::parse_sql("SELECT SHA('abc'), SHA1('abc')").unwrap();
        assert_eq!(executor::execute(&query, &Catalog::new()).unwrap().rows().unwrap(),
            vec![vec![Value::Text("a9993e364706816aba3e25717850c26c9cd0d89d".into()); 2]]);
    }
}

#[cfg(test)]
mod spark_murmur3_hash_tests {
    use super::*;
    use crate::{catalog::Catalog, executor, sql};

    #[test]
    fn spark_seed_and_reference_values() {
        assert_eq!(spark_hash(&[Value::Null]).unwrap(), Value::Integer(42));
        assert_eq!(spark_hash(&[Value::Text("Spark".into())]).unwrap(),
            Value::Integer(-684_278_310));
        assert_eq!(spark_hash(&[Value::Integer(0)]).unwrap(),
            Value::Integer(-1_670_924_195));
        assert_eq!(spark_hash(&[Value::Integer(1)]).unwrap(),
            Value::Integer(-1_712_319_331));
        assert_eq!(spark_hash(&[Value::Null, Value::Integer(1)]).unwrap(),
            spark_hash(&[Value::Integer(1), Value::Null]).unwrap());
    }
    #[test]
    fn floating_point_canonicalization_and_budget() {
        assert_eq!(spark_hash(&[Value::Float(-0.0)]).unwrap(),
            spark_hash(&[Value::Float(0.0)]).unwrap());
        assert_eq!(spark_hash(&[Value::Float(f64::NAN)]).unwrap(),
            spark_hash(&[Value::Float(f64::from_bits(0x7ff8_0000_0000_0001))]).unwrap());
        assert!(spark_hash(&[Value::Text("x".repeat(HASH_TEXT_BUDGET + 1))]).is_err());
        assert_eq!(spark_hash_type(&[DataType::Integer, DataType::Text]).unwrap(),
            DataType::Integer);
    }
    #[test]
    fn native_sql_uses_spark_murmur3() {
        let plan = sql::parse_sql("SELECT HASH('Spark'), HASH(0)").unwrap();
        assert_eq!(executor::execute(&plan, &Catalog::new()).unwrap().rows().unwrap(),
            vec![vec![Value::Integer(-684_278_310), Value::Integer(-1_670_924_195)]]);
    }
}

#[cfg(test)]
mod murmur_fixed_scalar_fast_path_tests {
    use super::*;

    #[test]
    fn fixed_width_rounds_match_full_murmur3_for_many_seeds() {
        for seed in [0_u32, 42, u32::MAX, 0x1234_5678] {
            for value in [0_u32, 1, 42, u32::MAX, 0x8000_0000] {
                assert_eq!(murmur_u32(value, seed),
                    murmur3_x86_32(&value.to_le_bytes(), seed));
            }
            for value in [0_u64, 1, 42, u64::MAX, 0x8000_0000_0000_0000,
                0x0123_4567_89ab_cdef] {
                assert_eq!(murmur_u64(value, seed),
                    murmur3_x86_32(&value.to_le_bytes(), seed));
            }
        }
    }

    #[test]
    fn mixed_numeric_hash_still_preserves_spark_seed_chaining() {
        let values = [Value::Integer(i64::MIN), Value::Boolean(true),
            Value::Float(-0.0), Value::Text("é🙂".into())];
        let mut seed = 42_u32;
        for value in &values {
            seed = match value {
                Value::Integer(n) => murmur3_x86_32(&n.to_le_bytes(), seed),
                Value::Boolean(b) => murmur3_x86_32(&i32::from(*b).to_le_bytes(), seed),
                Value::Float(_) => murmur3_x86_32(&0_u64.to_le_bytes(), seed),
                Value::Text(text) => murmur3_x86_32(text.as_bytes(), seed),
                Value::Null => seed,
            };
        }
        assert_eq!(spark_hash(&values).unwrap(), Value::Integer(i64::from(seed as i32)));
    }
}
