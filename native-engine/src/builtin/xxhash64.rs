use super::Function;
use crate::{scalar::Value, schema::DataType, Error, Result};

// Spark SQL XXHASH64 uses xxHash64 with seed 42. Each non-NULL argument is
// hashed with the previous argument's result as its new seed.
const PRIME1: u64 = 0x9e37_79b1_85eb_ca87;
const PRIME2: u64 = 0xc2b2_ae3d_27d4_eb4f;
const PRIME3: u64 = 0x1656_67b1_9e37_79f9;
const PRIME4: u64 = 0x85eb_ca77_c2b2_ae63;
const PRIME5: u64 = 0x27d4_eb2f_1656_67c5;
const SPARK_SEED: u64 = 42;

fn round(acc: u64, input: u64) -> u64 {
    acc.wrapping_add(input.wrapping_mul(PRIME2))
        .rotate_left(31)
        .wrapping_mul(PRIME1)
}

fn merge_round(acc: u64, value: u64) -> u64 {
    (acc ^ round(0, value))
        .wrapping_mul(PRIME1)
        .wrapping_add(PRIME4)
}

fn read_u64(bytes: &[u8]) -> u64 {
    u64::from_le_bytes(bytes.try_into().expect("complete eight-byte chunk"))
}

fn hash_bytes(bytes: &[u8], seed: u64) -> u64 {
    let mut offset = 0usize;
    let mut hash = if bytes.len() >= 32 {
        let mut v1 = seed.wrapping_add(PRIME1).wrapping_add(PRIME2);
        let mut v2 = seed.wrapping_add(PRIME2);
        let mut v3 = seed;
        let mut v4 = seed.wrapping_sub(PRIME1);
        while bytes.len() - offset >= 32 {
            v1 = round(v1, read_u64(&bytes[offset..offset + 8]));
            v2 = round(v2, read_u64(&bytes[offset + 8..offset + 16]));
            v3 = round(v3, read_u64(&bytes[offset + 16..offset + 24]));
            v4 = round(v4, read_u64(&bytes[offset + 24..offset + 32]));
            offset += 32;
        }
        let combined = v1.rotate_left(1)
            .wrapping_add(v2.rotate_left(7))
            .wrapping_add(v3.rotate_left(12))
            .wrapping_add(v4.rotate_left(18));
        merge_round(merge_round(merge_round(merge_round(combined, v1), v2), v3), v4)
    } else {
        seed.wrapping_add(PRIME5)
    };

    hash = hash.wrapping_add(bytes.len() as u64);
    while bytes.len() - offset >= 8 {
        hash ^= round(0, read_u64(&bytes[offset..offset + 8]));
        hash = hash.rotate_left(27).wrapping_mul(PRIME1).wrapping_add(PRIME4);
        offset += 8;
    }
    if bytes.len() - offset >= 4 {
        let tail = u32::from_le_bytes(bytes[offset..offset + 4]
            .try_into().expect("complete four-byte chunk"));
        hash ^= u64::from(tail).wrapping_mul(PRIME1);
        hash = hash.rotate_left(23).wrapping_mul(PRIME2).wrapping_add(PRIME3);
        offset += 4;
    }
    for &byte in &bytes[offset..] {
        hash ^= u64::from(byte).wrapping_mul(PRIME5);
        hash = hash.rotate_left(11).wrapping_mul(PRIME1);
    }
    avalanche(hash)
}

#[inline]
fn avalanche(mut hash: u64) -> u64 {
    hash ^= hash >> 33;
    hash = hash.wrapping_mul(PRIME2);
    hash ^= hash >> 29;
    hash = hash.wrapping_mul(PRIME3);
    hash ^ (hash >> 32)
}

// Spark's numeric scalars always hash four or eight little-endian bytes.
// Avoid the generic length branches, chunk loops, and temporary byte slices.
#[inline]
fn hash_u32(value: u32, seed: u64) -> u64 {
    let mut hash = seed.wrapping_add(PRIME5).wrapping_add(4);
    hash ^= u64::from(value).wrapping_mul(PRIME1);
    hash = hash.rotate_left(23).wrapping_mul(PRIME2).wrapping_add(PRIME3);
    avalanche(hash)
}

#[inline]
fn hash_u64(value: u64, seed: u64) -> u64 {
    let mut hash = seed.wrapping_add(PRIME5).wrapping_add(8);
    hash ^= round(0, value);
    hash = hash.rotate_left(27).wrapping_mul(PRIME1).wrapping_add(PRIME4);
    avalanche(hash)
}

fn output(types: &[DataType]) -> Result<DataType> {
    if types.iter().all(|kind| matches!(
        kind, DataType::Boolean | DataType::Integer | DataType::Float | DataType::Text
    )) {
        Ok(DataType::Integer)
    } else {
        Err(Error::Type("XXHASH64 requires native scalar arguments".into()))
    }
}

fn evaluate(args: &[Value]) -> Result<Value> {
    let mut seed = SPARK_SEED;
    for value in args {
        seed = match value {
            Value::Null => seed,
            Value::Boolean(value) => hash_u32(u32::from(*value), seed),
            Value::Integer(value) => hash_u64(*value as u64, seed),
            Value::Float(value) => {
                // Spark hashes -0.0 like +0.0 and canonicalizes NaN payloads.
                let bits = if *value == 0.0 { 0 }
                    else if value.is_nan() { 0x7ff8_0000_0000_0000 }
                    else { value.to_bits() };
                hash_u64(bits, seed)
            }
            Value::Text(value) => hash_bytes(value.as_bytes(), seed),
        };
    }
    Ok(Value::Integer(seed as i64))
}

pub(super) const FUNCTIONS: &[Function] = &[Function {
    name: "xxhash64", min_args: 1, max_args: 128, result_type: output, call: evaluate,
}];

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{catalog::Catalog, executor, sql};

    fn hash(values: &[Value]) -> Value {
        evaluate(values).expect("valid scalar arguments")
    }

    #[test]
    fn matches_libxxhash_reference_vectors_for_spark_seed() {
        assert_eq!(hash(&[Value::Text("Spark".into())]), Value::Integer(-4294468057691064905));
        assert_eq!(hash(&[Value::Text("".into())]), Value::Integer(-7444071767201028348));
        assert_eq!(hash(&[Value::Text("é🙂".into())]), Value::Integer(-5770834466922380538));
        assert_eq!(hash(&[Value::Text("a".repeat(40))]), Value::Integer(-8273737738657618755));
        assert_eq!(hash(&[Value::Integer(0)]), Value::Integer(-5252525462095825812));
        assert_eq!(hash(&[Value::Integer(1)]), Value::Integer(-7001672635703045582));
        assert_eq!(hash(&[Value::Integer(-1)]), Value::Integer(3858142552250413010));
    }

    #[test]
    fn carries_the_seed_across_arguments_and_skips_nulls() {
        assert_eq!(hash(&[Value::Null]), Value::Integer(42));
        assert_eq!(hash(&[Value::Null, Value::Text("Spark".into()), Value::Null]),
            hash(&[Value::Text("Spark".into())]));
        assert_eq!(hash(&[Value::Text("Spark".into()), Value::Integer(2)]),
            Value::Integer(6174828680566760385));
        assert_eq!(hash(&[Value::Float(0.0)]), hash(&[Value::Float(-0.0)]));
        assert_eq!(hash(&[Value::Float(f64::NAN)]),
            hash(&[Value::Float(f64::from_bits(0x7ff8_0000_0000_0001))]));
    }

    #[test]
    fn parses_and_executes_native_sql() {
        let plan = sql::parse_sql("SELECT XXHASH64('Spark', 2) AS digest").unwrap();
        let batch = executor::execute(&plan, &Catalog::new()).unwrap();
        assert_eq!(batch.row(0).unwrap()[0], Value::Integer(6174828680566760385));
    }
}

#[cfg(test)]
mod scalar_fast_hash_equivalence_tests {
    use super::*;

    #[test]
    fn direct_numeric_hashes_match_generic_bytes_for_many_seeds() {
        let mut value = 0x1234_5678_9abc_def0_u64;
        for _ in 0..10_000 {
            value = value.wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            let seed = value.rotate_right(17);
            assert_eq!(hash_u64(value, seed), hash_bytes(&value.to_le_bytes(), seed));
            let value32 = value as u32;
            assert_eq!(hash_u32(value32, seed), hash_bytes(&value32.to_le_bytes(), seed));
        }
        for value in [0_u64, 1, u64::MAX, i64::MIN as u64, i64::MAX as u64] {
            assert_eq!(hash_u64(value, SPARK_SEED),
                hash_bytes(&value.to_le_bytes(), SPARK_SEED));
        }
    }

    #[test]
    fn spark_null_chaining_and_floating_point_canonicalization_are_unchanged() {
        let args = vec![
            Value::Boolean(true),
            Value::Null,
            Value::Integer(i64::MIN),
            Value::Float(-0.0),
            Value::Float(f64::NAN),
            Value::Text("é🙂".into()),
        ];
        let mut expected = SPARK_SEED;
        expected = hash_bytes(&1_u32.to_le_bytes(), expected);
        expected = hash_bytes(&i64::MIN.to_le_bytes(), expected);
        expected = hash_bytes(&0_u64.to_le_bytes(), expected);
        expected = hash_bytes(&0x7ff8_0000_0000_0000_u64.to_le_bytes(), expected);
        expected = hash_bytes("é🙂".as_bytes(), expected);
        assert_eq!(evaluate(&args).unwrap(), Value::Integer(expected as i64));
        assert_eq!(evaluate(&[Value::Float(-0.0)]).unwrap(),
            evaluate(&[Value::Float(0.0)]).unwrap());
    }
}
