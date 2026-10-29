use crate::{Error, Result};
use std::cell::RefCell;

thread_local! {
    static CACHE: RefCell<Vec<(String, regex::Regex)>> = const { RefCell::new(Vec::new()) };
}
const CACHE_ENTRIES: usize = 4;

/// Bound the cost of compiling user-supplied regex programs.
pub(super) fn compile(pattern: &str) -> Result<regex::Regex> {
    if pattern.len() > 4096 {
        return Err(Error::Execution("regular expression exceeds 4096-byte pattern budget".into()));
    }
    if let Some(compiled) = CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        let index = cache.iter().position(|(key, _)| key == pattern)?;
        let entry = cache.remove(index);
        let compiled = entry.1.clone();
        cache.push(entry);
        Some(compiled)
    }) {
        return Ok(compiled);
    }
    let compiled = regex::RegexBuilder::new(pattern)
        .size_limit(8 * 1024 * 1024)
        .dfa_size_limit(8 * 1024 * 1024)
        .build()
        .map_err(|error| Error::Execution(format!("invalid native regular expression: {error}")))?;
    CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        if cache.len() == CACHE_ENTRIES { cache.remove(0); }
        cache.push((pattern.to_owned(), compiled.clone()));
    });
    Ok(compiled)
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
        for _ in 0..10 { assert!(compile("(?i)^hello$").unwrap().is_match("hello")); }
        for pattern in ["a", "b", "c", "d", "e"] {
            assert!(compile(pattern).unwrap().is_match(pattern));
        }
        CACHE.with(|cache| assert!(cache.borrow().len() <= CACHE_ENTRIES));
    }
}

#[cfg(test)]
mod lru_cache_regression_tests {
    use super::*;
    #[test]
    fn a_recently_reused_pattern_survives_eviction() {
        CACHE.with(|cache| cache.borrow_mut().clear());
        for pattern in ["^keep$", "^a$", "^b$", "^c$"] { compile(pattern).unwrap(); }
        compile("^keep$").unwrap();
        compile("^d$").unwrap();
        CACHE.with(|cache| {
            let cache = cache.borrow();
            assert_eq!(cache.len(), CACHE_ENTRIES);
            assert!(cache.iter().any(|(pattern, _)| pattern == "^keep$"));
            assert!(!cache.iter().any(|(pattern, _)| pattern == "^a$"));
        });
    }
}
