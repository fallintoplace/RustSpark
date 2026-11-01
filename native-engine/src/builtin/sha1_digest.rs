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

pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "sha", min_args: 1, max_args: 1, result_type: output, call: evaluate },
    Function { name: "sha1", min_args: 1, max_args: 1, result_type: output, call: evaluate },
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
