//! Bound the number of rows transferred back to the client for preview.
use std::env;

fn parse_rows(input: &str) -> Result<usize, String> {
    let rows = input
        .parse::<usize>()
        .map_err(|_| "RUSTSPARK_MAX_ROWS must be a positive integer".to_owned())?;
    if !(1..=10_000).contains(&rows) {
        return Err("RUSTSPARK_MAX_ROWS must be between 1 and 10000".into());
    }
    Ok(rows)
}

pub fn configured_rows() -> Result<Option<usize>, Box<dyn std::error::Error>> {
    match env::var("RUSTSPARK_MAX_ROWS") {
        Ok(value) => Ok(Some(parse_rows(&value)?)),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_bounded_previews() {
        assert_eq!(parse_rows("1").unwrap(), 1);
        assert_eq!(parse_rows("10000").unwrap(), 10_000);
    }

    #[test]
    fn rejects_unsafe_or_invalid_previews() {
        for bad in ["0", "10001", "-2", "", "1.5", "lots"] {
            assert!(parse_rows(bad).is_err(), "{bad}");
        }
    }
}
