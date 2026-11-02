use crate::{Error, Result};
use std::cell::RefCell;

const CACHE_ENTRIES: usize = 4;

#[derive(Default)]
struct RegexCache {
    entries: Vec<(String, regex::Regex)>,
}

impl RegexCache {
    fn lookup(&mut self, pattern: &str) -> Option<regex::Regex> {
        let index = self.entries.iter().position(|(key, _)| key == pattern)?;
        if index + 1 == self.entries.len() {
            return Some(self.entries[index].1.clone());
        }
        let entry = self.entries.remove(index);
        let compiled = entry.1.clone();
        self.entries.push(entry);
        Some(compiled)
    }

    fn insert(&mut self, pattern: &str, compiled: &regex::Regex) {
        if self.entries.len() == CACHE_ENTRIES {
            self.entries.remove(0);
        }
        self.entries.push((pattern.to_owned(), compiled.clone()));
    }
}

thread_local! {
    static CACHE: RefCell<RegexCache> =
        const { RefCell::new(RegexCache { entries: Vec::new() }) };
}

fn lookup_cached(pattern: &str) -> Option<regex::Regex> {
    CACHE.with(|cache| cache.borrow_mut().lookup(pattern))
}

/// Bound the cost of compiling user-supplied regex programs.
pub(super) fn compile(pattern: &str) -> Result<regex::Regex> {
    if pattern.len() > 4096 {
        return Err(Error::Execution("regular expression exceeds 4096-byte pattern budget".into()));
    }
    if let Some(compiled) = lookup_cached(pattern) {
        return Ok(compiled);
    }
    let compiled = regex::RegexBuilder::new(pattern)
        .size_limit(8 * 1024 * 1024)
        .dfa_size_limit(8 * 1024 * 1024)
        .build()
        .map_err(|error| Error::Execution(format!("invalid native regular expression: {error}")))?;
    CACHE.with(|cache| cache.borrow_mut().insert(pattern, &compiled));
    Ok(compiled)
}

// Recognize a start-anchored identifier literal without regex compilation.
pub(super) fn anchored_identifier(pattern: &str) -> Option<&str> {
    let prefix = pattern.strip_prefix('^')?;
    (prefix.len() <= 4095 &&
        prefix.chars().all(|ch| ch.is_alphanumeric() || ch == '_'))
        .then_some(prefix)
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
        CACHE.with(|cache| assert!(cache.borrow().entries.len() <= CACHE_ENTRIES));
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
            assert!(entries.entries.iter().any(|(key, _)| key == "rustspark-lru-a"));
            assert!(!entries.entries.iter().any(|(key, _)| key == "rustspark-lru-b"));
        });
    }
}

#[cfg(test)]
mod anchored_identifier_refactor_tests {
    use super::*;

    #[test]
    fn accepts_only_bounded_start_anchored_identifiers() {
        for pattern in ["^", "^column_2", "^éclair"] {
            assert!(anchored_identifier(pattern).is_some(), "{pattern}");
        }
        for pattern in ["column", "^a+b", "^a.b", "^a|b", "^ab$", "x^ab"] {
            assert!(anchored_identifier(pattern).is_none(), "{pattern}");
        }
        assert!(anchored_identifier(&format!("^{}", "x".repeat(4096))).is_none());
    }
}
