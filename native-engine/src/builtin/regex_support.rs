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
        if index + 1 == cache.len() {
            return Some(cache[index].1.clone());
        }
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
mod regex_cache_lru_tests {
    use super::*;
    #[test]
    fn recently_used_patterns_survive_capacity_eviction() {
        for suffix in ["a", "b", "c", "d"] {
            compile(&format!("rustspark-lru-{suffix}")).unwrap();
        }
        compile("rustspark-lru-a").unwrap();
        compile("rustspark-lru-e").unwrap();
        CACHE.with(|cache| {
            let entries = cache.borrow();
            assert!(entries.iter().any(|(key, _)| key == "rustspark-lru-a"));
            assert!(!entries.iter().any(|(key, _)| key == "rustspark-lru-b"));
        });
    }
}
