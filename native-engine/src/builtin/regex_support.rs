use crate::{Error, Result};

/// Bound the cost of compiling user-supplied regex programs.
pub(super) fn compile(pattern: &str) -> Result<regex::Regex> {
    if pattern.len() > 4096 {
        return Err(Error::Execution("regular expression exceeds 4096-byte pattern budget".into()));
    }
    regex::RegexBuilder::new(pattern)
        .size_limit(8 * 1024 * 1024)
        .dfa_size_limit(8 * 1024 * 1024)
        .build()
        .map_err(|error| Error::Execution(format!("invalid native regular expression: {error}")))
}

pub(super) fn check_input(text: &str) -> Result<()> {
    if text.len() > 16 * 1024 * 1024 {
        return Err(Error::Execution("regex input exceeds native 16 MiB budget".into()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rejects_unbounded_regex_inputs() {
        assert!(compile(&"a".repeat(4097)).is_err());
        assert!(compile("(").is_err());
        assert!(compile("(?i)^hello$").unwrap().is_match("HELLO"));
    }
}
