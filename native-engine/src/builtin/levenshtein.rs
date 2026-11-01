use super::Function;
use crate::{scalar::Value, schema::DataType, Error, Result};

fn output(types: &[DataType]) -> Result<DataType> {
    super::text_type(&types[..2])?;
    if types.len() == 3 && types[2] != DataType::Integer {
        return Err(Error::Type("LEVENSHTEIN threshold must be an integer".into()));
    }
    Ok(DataType::Integer)
}

fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(first) = super::string(&args[0])? else { return Ok(Value::Null); };
    let Some(second) = super::string(&args[1])? else { return Ok(Value::Null); };
    let threshold = if args.len() == 3 {
        let Some(max_distance) = super::integer(&args[2])? else { return Ok(Value::Null) };
        if max_distance < 0 { return Ok(Value::Integer(-1)); }
        Some(usize::try_from(max_distance).unwrap_or(usize::MAX))
    } else { None };
    if first == second { return Ok(Value::Integer(0)); }
    if threshold == Some(0) { return Ok(Value::Integer(-1)); }
    if first.is_empty() || second.is_empty() {
        // Distance from the empty string is just the Unicode character count.
        // No dynamic-programming state or character vector is required.
        let other = if first.is_empty() { second } else { first };
        let distance = if other.is_ascii() { other.len() } else { other.chars().count() };
        if threshold.is_some_and(|limit| distance > limit) {
            return Ok(Value::Integer(-1));
        }
        return i64::try_from(distance).map(Value::Integer)
            .map_err(|_| Error::Execution("LEVENSHTEIN result overflow".into()));
    }
    if let Some(limit) = threshold {
        // An edit changes Unicode scalar length by at most one.
        // Reject impossible thresholds without allocating character arrays.
        let first_len = if first.is_ascii() { first.len() } else { first.chars().count() };
        let second_len = if second.is_ascii() { second.len() } else { second.chars().count() };
        if first_len.abs_diff(second_len) > limit {
            return Ok(Value::Integer(-1));
        }
    }
    // Decode identical affixes from borrowed UTF-8 slices. Wide, mostly
    // identical strings now allocate DP state only for the differing middle.
    let mut prefix_bytes = 0usize;
    for (left, right) in first.chars().zip(second.chars()) {
        if left != right { break; }
        prefix_bytes += left.len_utf8();
    }
    let first = &first[prefix_bytes..];
    let second = &second[prefix_bytes..];
    let (mut first_end, mut second_end) = (first.len(), second.len());
    while first_end > 0 && second_end > 0 {
        let left = first[..first_end].chars().next_back().expect("nonempty UTF-8");
        let right = second[..second_end].chars().next_back().expect("nonempty UTF-8");
        if left != right { break; }
        first_end -= left.len_utf8();
        second_end -= right.len_utf8();
    }
    // ASCII-only middle spans can use borrowed UTF-8 bytes directly.
    let first = &first[..first_end];
    let second = &second[..second_end];
    if first.is_ascii() && second.is_ascii() {
        if threshold == Some(1) {
            return Ok(Value::Integer(single_ascii_edit(first.as_bytes(), second.as_bytes())));
        }
        if threshold == Some(2) {
            return Ok(Value::Integer(two_ascii_edits(first.as_bytes(), second.as_bytes())));
        }
        return distance_units(first.as_bytes(), second.as_bytes(), threshold);
    }
    // Small thresholds need at most a handful of edits. Walk borrowed
    // Unicode scalar slices directly, without allocating character vectors
    // or dynamic-programming rows for the common fuzzy-match case.
    if let Some(limit @ (1 | 2)) = threshold {
        return Ok(Value::Integer(bounded_unicode_edits(
            first, second, limit as u8,
        )));
    }
    let a = first.chars().collect::<Vec<_>>();
    let b = second.chars().collect::<Vec<_>>();
    distance_units(&a, &b, threshold)
}

// Bounded Unicode search, operating on borrowed UTF-8 boundaries.
// At most two edits gives at most nine branches and constant recursion depth.
fn bounded_unicode_edits(a: &str, b: &str, max_edits: u8) -> i64 {
    fn search(mut a: &str, mut b: &str, remaining: u8) -> Option<u8> {
        if a.chars().count().abs_diff(b.chars().count()) > usize::from(remaining) {
            return None;
        }
        loop {
            let (Some(left), Some(right)) = (a.chars().next(), b.chars().next())
                else { break };
            if left != right { break; }
            a = &a[left.len_utf8()..];
            b = &b[right.len_utf8()..];
        }
        if a.is_empty() || b.is_empty() {
            let tail = a.chars().count() + b.chars().count();
            return (tail <= usize::from(remaining)).then_some(tail as u8);
        }
        if remaining == 0 { return None; }
        let left = a.chars().next().expect("nonempty Unicode suffix");
        let right = b.chars().next().expect("nonempty Unicode suffix");
        [(&a[left.len_utf8()..], &b[right.len_utf8()..]),
         (&a[left.len_utf8()..], b),
         (a, &b[right.len_utf8()..])]
            .into_iter()
            .filter_map(|(first, second)| search(first, second, remaining - 1))
            .min()
            .map(|cost| cost + 1)
    }
    search(a, b, max_edits).map_or(-1, i64::from)
}

// A threshold of one permits at most one insertion, deletion, or
// substitution. Compare ASCII slices directly instead of allocating DP rows.
fn single_ascii_edit(a: &[u8], b: &[u8]) -> i64 {
    let (longer, shorter) = if a.len() >= b.len() { (a, b) } else { (b, a) };
    if longer.len() - shorter.len() > 1 { return -1; }
    let same_length = longer.len() == shorter.len();
    let (mut i, mut j, mut edits) = (0usize, 0usize, 0usize);
    while i < longer.len() && j < shorter.len() {
        if longer[i] == shorter[j] {
            i += 1;
            j += 1;
            continue;
        }
        if edits != 0 { return -1; }
        edits += 1;
        i += 1;
        if same_length { j += 1; }
    }
    edits += (longer.len() - i) + (shorter.len() - j);
    if edits <= 1 { edits as i64 } else { -1 }
}

// With a threshold of two, at most two mismatch branches can be taken.
// This bounded search reuses input byte slices and needs no DP allocations.
fn two_ascii_edits(a: &[u8], b: &[u8]) -> i64 {
    fn bounded(mut a: &[u8], mut b: &[u8], remaining: u8) -> Option<u8> {
        if a.len().abs_diff(b.len()) > usize::from(remaining) { return None; }
        while !a.is_empty() && !b.is_empty() && a[0] == b[0] {
            a = &a[1..];
            b = &b[1..];
        }
        if a.is_empty() || b.is_empty() {
            let tail = a.len().saturating_add(b.len());
            return (tail <= usize::from(remaining)).then_some(tail as u8);
        }
        if remaining == 0 { return None; }
        // Replace, delete, or insert one byte. The recursion depth is at
        // most two, so the search examines a constant number of suffixes.
        [(&a[1..], &b[1..]), (&a[1..], b), (a, &b[1..])]
            .into_iter()
            .filter_map(|(left, right)| bounded(left, right, remaining - 1))
            .min()
            .map(|cost| cost + 1)
    }
    bounded(a, b, 2).map_or(-1, i64::from)
}

fn distance_units<T: Eq + Copy>(
    a: &[T], b: &[T], threshold: Option<usize>,
) -> Result<Value> {    if threshold.is_some_and(|limit| a.len().abs_diff(b.len()) > limit) {
        return Ok(Value::Integer(-1));
    }
    let (longer, shorter) = if a.len() >= b.len() { (a, b) } else { (b, a) };
    if shorter.is_empty() {
        let distance = longer.len();
        return Ok(Value::Integer(if threshold.is_some_and(|limit| distance > limit) { -1 }
            else { i64::try_from(distance).map_err(|_| Error::Execution("LEVENSHTEIN result overflow".into()))? }));
    }
    let band_width = threshold.map_or(shorter.len(), |limit|
        limit.saturating_mul(2).saturating_add(1).min(shorter.len()));
    if longer.len().checked_mul(band_width).is_none_or(|cells| cells > 2_000_000) {
        return Err(Error::Execution("LEVENSHTEIN exceeds native edit-distance work budget".into()));
    }
    if let Some(limit) = threshold {
        // The thresholded dynamic program needs only the diagonals within
        // edit distance 'limit'. Keep two narrow windows rather than two
        // full-width rows, which waste O(shorter.len()) memory for small limits.
        let outside = limit.saturating_add(1);
        let width = limit.saturating_mul(2).saturating_add(1)
            .min(shorter.len().saturating_add(1));
        let mut previous = vec![outside; width];
        let mut current = vec![outside; width];
        let mut previous_start = 0usize;
        let mut previous_end = shorter.len().min(limit);
        for column in 0..=previous_end {
            previous[column] = column;
        }
        for (index, from) in longer.iter().enumerate() {
            let row = index + 1;
            let start = row.saturating_sub(limit);
            let end = row.saturating_add(limit).min(shorter.len());
            let mut row_min = outside;
            for column in start..=end {
                let distance = if column == 0 {
                    row.min(outside)
                } else {
                    let insert = if column > start {
                        current[column - start - 1].saturating_add(1)
                    } else { outside };
                    let delete = if (previous_start..=previous_end).contains(&column) {
                        previous[column - previous_start].saturating_add(1)
                    } else { outside };
                    let replace = if (previous_start..=previous_end).contains(&(column - 1)) {
                        previous[column - 1 - previous_start]
                            .saturating_add(usize::from(*from != shorter[column - 1]))
                    } else { outside };
                    insert.min(delete).min(replace).min(outside)
                };
                current[column - start] = distance;
                row_min = row_min.min(distance);
            }
            if row_min > limit { return Ok(Value::Integer(-1)); }
            std::mem::swap(&mut previous, &mut current);
            previous_start = start;
            previous_end = end;
        }
        let distance = previous[shorter.len() - previous_start];
        if distance > limit { return Ok(Value::Integer(-1)); }
        return i64::try_from(distance).map(Value::Integer)
            .map_err(|_| Error::Execution("LEVENSHTEIN result overflow".into()));
    }
    let limit = threshold.unwrap_or(usize::MAX);
    let outside = limit.saturating_add(1);
    let mut previous = (0..=shorter.len()).map(|n| n.min(outside)).collect::<Vec<_>>();
    let mut current = vec![outside; shorter.len() + 1];
    for (i, from) in longer.iter().enumerate() {
        let row = i + 1;
        let start = if threshold.is_some() { row.saturating_sub(limit).max(1) } else { 1 };
        let end = if threshold.is_some() { row.saturating_add(limit).min(shorter.len()) }
            else { shorter.len() };
        current[0] = row.min(outside);
        // Only the band boundaries need clearing between rows.
        if start > 1 { current[start - 1] = outside; }
        if end < shorter.len() { current[end + 1] = outside; }
        for column in start..=end {
            let insertion = current[column - 1].saturating_add(1);
            let deletion = previous[column].saturating_add(1);
            let substitution = previous[column - 1] + usize::from(*from != shorter[column - 1]);
            current[column] = insertion.min(deletion).min(substitution);
        }
        if threshold.is_some() && current[start..=end].iter().all(|&distance| distance > limit) {
            return Ok(Value::Integer(-1));
        }
        std::mem::swap(&mut previous, &mut current);
    }
    let distance = previous[shorter.len()];
    if threshold.is_some_and(|limit| distance > limit) {
        return Ok(Value::Integer(-1));
    }
    Ok(Value::Integer(i64::try_from(distance)
        .map_err(|_| Error::Execution("LEVENSHTEIN result overflow".into()))?))
}


fn hamming_distance_type(types: &[DataType]) -> Result<DataType> {
    super::text_type(types)?;
    Ok(DataType::Integer)
}
fn hamming_distance(args: &[Value]) -> Result<Value> {
    let Some(left) = super::string(&args[0])? else { return Ok(Value::Null); };
    let Some(right) = super::string(&args[1])? else { return Ok(Value::Null); };
    if left.len() > 1_000_000 || right.len() > 1_000_000 {
        return Err(Error::Execution("HAMMING_DISTANCE input exceeds 1 MiB budget".into()));
    }
    if left.is_ascii() && right.is_ascii() {
        if left.len() != right.len() {
            return Err(Error::Type("HAMMING_DISTANCE requires equal-length strings".into()));
        }
        let mismatches = left.bytes().zip(right.bytes()).filter(|(a, b)| a != b).count();
        return Ok(Value::Integer(mismatches as i64));
    }
    let mut left = left.chars();
    let mut right = right.chars();
    let mut distance = 0_i64;
    loop {
        match (left.next(), right.next()) {
            (None, None) => return Ok(Value::Integer(distance)),
            (Some(a), Some(b)) => { if a != b { distance += 1; } }
            _ => return Err(Error::Type("HAMMING_DISTANCE requires equal-length strings".into())),
        }
    }
}

pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "hamming_distance", min_args: 2, max_args: 2, result_type: hamming_distance_type, call: hamming_distance },Function {
    name: "levenshtein", min_args: 2, max_args: 3,
    result_type: output, call: evaluate,
}];

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{batch::Batch, catalog::Catalog, executor, schema::{Field, Schema}, sql};
    #[test]
    fn computes_unicode_edit_distance_through_native_sql() {
        let schema = Schema::new(vec![Field::new("name", DataType::Text, true)]).unwrap();
        let mut catalog = Catalog::new();
        catalog.register("t", Batch::from_rows(schema, vec![
            vec![Value::Text("kitten".into())],
            vec![Value::Text("café".into())],
            vec![Value::Null],
        ]).unwrap()).unwrap();
        let rows = executor::execute(&sql::parse_sql(
            "SELECT LEVENSHTEIN(name, 'sitting') AS edits FROM t"
        ).unwrap(), &catalog).unwrap().rows().unwrap();
        assert_eq!(rows[0], vec![Value::Integer(3)]);
        assert_eq!(rows[2], vec![Value::Null]);
        assert_eq!(evaluate(&[Value::Text("café".into()), Value::Text("cafe".into())])
            .unwrap(), Value::Integer(1));
        let shared = "é".repeat(3000);
        assert_eq!(evaluate(&[
            Value::Text(format!("{shared}x{shared}")),
            Value::Text(format!("{shared}y{shared}")),
        ]).unwrap(), Value::Integer(1));
        assert_eq!(evaluate(&[Value::Text(shared.clone()), Value::Text(shared)])
            .unwrap(), Value::Integer(0));
        assert!(evaluate(&[Value::Text("a".repeat(2000)), Value::Text("b".repeat(2000))])
            .is_err());
        assert_eq!(evaluate(&[Value::Text("kitten".into()), Value::Text("sitting".into()), Value::Integer(2)])
            .unwrap(), Value::Integer(-1));
        assert_eq!(evaluate(&[Value::Text("kitten".into()), Value::Text("sitting".into()), Value::Integer(3)])
            .unwrap(), Value::Integer(3));
        assert_eq!(evaluate(&[Value::Text("a".into()), Value::Text("12345".into()), Value::Integer(2)])
            .unwrap(), Value::Integer(-1));
        assert_eq!(evaluate(&[Value::Text("a".into()), Value::Text("b".into()), Value::Null])
            .unwrap(), Value::Null);
        assert!(output(&[DataType::Text, DataType::Text, DataType::Text]).is_err());
        assert_eq!(evaluate(&[Value::Text("a".repeat(2500)), Value::Text("a".repeat(2500)), Value::Integer(0)]).unwrap(), Value::Integer(0));
        assert_eq!(evaluate(&[Value::Text("a".repeat(2500)), Value::Text("b".repeat(2500)), Value::Integer(1)]).unwrap(), Value::Integer(-1));
        assert_eq!(evaluate(&[Value::Text("é".repeat(2500)), Value::Text(format!("{}a", "é".repeat(2499))), Value::Integer(1)]).unwrap(), Value::Integer(1));
        assert_eq!(evaluate(&[Value::Text("abc".into()), Value::Text("".into()), Value::Integer(3)]).unwrap(), Value::Integer(3));
    }
}

#[cfg(test)]
mod hamming_distance_extension_tests {
    use super::*;
    #[test]
    fn preserves_unicode_and_null_semantics() {
        assert!(crate::builtin::supports("hamming_distance"));
        assert_eq!(hamming_distance(&[Value::Text("karolin".into()), Value::Text("kathrin".into())]).unwrap(), Value::Integer(3));
        assert_eq!(hamming_distance(&[Value::Text("é🙂".into()), Value::Text("é界".into())]).unwrap(), Value::Integer(1));
        assert!(hamming_distance(&[Value::Text("ab".into()), Value::Text("a".into())]).is_err());
        assert_eq!(hamming_distance(&[Value::Null, Value::Text("a".into())]).unwrap(), Value::Null);
    }
}

#[cfg(test)]
mod distance_fast_path_tests {
    use super::*;
    #[test]
    fn equal_strings_bypass_edit_work_and_ascii_hamming_counts_bytes() {
        let long = Value::Text("x".repeat(20_000));
        assert_eq!(evaluate(&[long.clone(), long, Value::Integer(0)]).unwrap(), Value::Integer(0));
        assert_eq!(evaluate(&[Value::Text("same".into()), Value::Text("sane".into()), Value::Integer(0)]).unwrap(), Value::Integer(-1));
        assert_eq!(hamming_distance(&[Value::Text("AABB".into()), Value::Text("ABBA".into())]).unwrap(), Value::Integer(2));
        assert!(hamming_distance(&[Value::Text("A".into()), Value::Text("AB".into())]).is_err());
    }
}

#[cfg(test)]
mod empty_operand_edit_distance_fast_path_tests {
    use super::*;
    #[test]
    fn counts_unicode_without_materializing_character_vectors() {
        let text = Value::Text("é🙂東京".repeat(25_000));
        assert_eq!(evaluate(&[Value::Text(String::new()), text.clone()]).unwrap(),
            Value::Integer(100_000));
        assert_eq!(evaluate(&[text.clone(), Value::Text(String::new())]).unwrap(),
            Value::Integer(100_000));
        assert_eq!(evaluate(&[Value::Text(String::new()), text.clone(),
            Value::Integer(99_999)]).unwrap(), Value::Integer(-1));
        assert_eq!(evaluate(&[Value::Text(String::new()), text,
            Value::Integer(100_000)]).unwrap(), Value::Integer(100_000));
        assert_eq!(evaluate(&[Value::Text(String::new()),
            Value::Text(String::new())]).unwrap(), Value::Integer(0));
    }
}

#[cfg(test)]
mod levenshtein_threshold_preflight_tests {
    use super::*;

    #[test]
    fn large_length_difference_rejects_without_unicode_buffers() {
        let long = Value::Text("é🙂".repeat(50_000));
        let short = Value::Text("é🙂".repeat(50));
        assert_eq!(evaluate(&[long.clone(), short.clone(), Value::Integer(3)]).unwrap(),
            Value::Integer(-1));
        assert_eq!(evaluate(&[short, long, Value::Integer(3)]).unwrap(),
            Value::Integer(-1));
    }

    #[test]
    fn equal_unicode_character_counts_still_use_edit_distance() {
        assert_eq!(evaluate(&[
            Value::Text("é🙂".into()),
            Value::Text("é界".into()),
            Value::Integer(1),
        ]).unwrap(), Value::Integer(1));
        assert_eq!(evaluate(&[
            Value::Text("abc".into()),
            Value::Text("ab".into()),
            Value::Integer(1),
        ]).unwrap(), Value::Integer(1));
    }
}

#[cfg(test)]
mod borrowed_levenshtein_affix_regressions {
    use super::*;

    #[test]
    fn long_shared_unicode_affixes_do_not_change_distances() {
        let common = "🙂é東京".repeat(30_000);
        let left = Value::Text(format!("{common}X{common}"));
        let right = Value::Text(format!("{common}Y{common}"));
        assert_eq!(evaluate(&[left.clone(), right.clone()]).unwrap(), Value::Integer(1));
        assert_eq!(evaluate(&[left.clone(), right, Value::Integer(1)]).unwrap(), Value::Integer(1));
        assert_eq!(evaluate(&[left, Value::Text(format!("{common}XYZ{common}")),
            Value::Integer(1)]).unwrap(), Value::Integer(-1));
        assert_eq!(evaluate(&[
            Value::Text(format!("{common}ab{common}")),
            Value::Text(format!("{common}a{common}")),
            Value::Integer(1),
        ]).unwrap(), Value::Integer(1));
    }
}

#[cfg(test)]
mod shared_unicode_edge_streaming_tests {
    use super::*;

    #[test]
    fn long_shared_unicode_edges_do_not_expand_dp_work() {
        let edge = "é🙂界".repeat(60_000);
        let left = Value::Text(format!("{edge}x{edge}"));
        let right = Value::Text(format!("{edge}y{edge}"));
        assert_eq!(evaluate(&[left.clone(), right.clone()]).unwrap(), Value::Integer(1));
        assert_eq!(evaluate(&[left, right, Value::Integer(1)]).unwrap(), Value::Integer(1));
    }

    #[test]
    fn shared_edges_are_trimmed_on_unicode_scalar_boundaries() {
        for (left, right, expected) in [
            ("🧡a🚀", "🧡b🚀", 1),
            ("ééhello🌙", "ééhell🌙", 1),
            ("🙂a🙂", "🙂abc🙂", 2),
            ("abc", "abc🌟", 1),
            ("🌟abc", "abc", 1),
        ] {
            assert_eq!(evaluate(&[
                Value::Text(left.into()), Value::Text(right.into())
            ]).unwrap(), Value::Integer(expected));
        }
    }
}

#[cfg(test)]
mod levenshtein_borrowed_ascii_tests {
    use super::*;

    fn distance(a: &str, b: &str, threshold: Option<i64>) -> Result<Value> {
        let mut args = vec![Value::Text(a.into()), Value::Text(b.into())];
        if let Some(threshold) = threshold { args.push(Value::Integer(threshold)); }
        evaluate(&args)
    }

    #[test]
    fn byte_slice_dp_matches_unicode_scalar_dp() {
        for (a, b, expected) in [
            ("kitten", "sitting", 3),
            ("aaaaabc", "aaaaaxc", 1),
            ("abc", "cab", 2),
            ("abc", "abc", 0),
            ("", "abc", 3),
            ("a🙂b", "a🙂c", 1),
            ("café", "cafe", 1),
            ("é🙂é", "éXé", 1),
        ] {
            assert_eq!(distance(a, b, None).unwrap(), Value::Integer(expected));
            assert_eq!(distance(a, b, Some(expected)).unwrap(),
                Value::Integer(expected));
            if expected > 0 {
                assert_eq!(distance(a, b, Some(expected - 1)).unwrap(),
                    Value::Integer(-1));
            }
        }
    }

    #[test]
    fn huge_matching_ascii_edges_only_process_the_trimmed_middle() {
        let prefix = "a".repeat(100_000);
        let left = format!("{prefix}X{prefix}");
        let right = format!("{prefix}Y{prefix}");
        assert_eq!(distance(&left, &right, Some(1)).unwrap(), Value::Integer(1));
        assert_eq!(distance(&left, &right, Some(0)).unwrap(), Value::Integer(-1));
    }
}

#[cfg(test)]
mod narrow_banded_levenshtein_tests {
    use super::*;

    #[test]
    fn bounded_unicode_distances_match_unbounded_reference() {
        let samples = ["", "a", "b", "ab", "ba", "aaa", "aab", "界a",
            "a界", "界🙂", "🙂界", "kitten", "sitting"];
        for first in samples {
            for second in samples {
                let exact = evaluate(&[
                    Value::Text(first.into()), Value::Text(second.into())
                ]).unwrap();
                let Value::Integer(distance) = exact else { panic!("distance expected"); };
                for limit in 0..=5 {
                    let actual = evaluate(&[
                        Value::Text(first.into()), Value::Text(second.into()),
                        Value::Integer(limit),
                    ]).unwrap();
                    assert_eq!(actual, Value::Integer(
                        if distance <= limit { distance } else { -1 }
                    ), "first={first:?} second={second:?} limit={limit}");
                }
            }
        }
    }

    #[test]
    fn long_shifted_strings_fit_in_tiny_edit_band() {
        let left = Value::Text("ab".repeat(20_000));
        let right = Value::Text("ba".repeat(20_000));
        assert_eq!(evaluate(&[left.clone(), right.clone(), Value::Integer(2)]).unwrap(),
            Value::Integer(2));
        assert_eq!(evaluate(&[left, right, Value::Integer(1)]).unwrap(),
            Value::Integer(-1));
    }
}

#[cfg(test)]
mod ascii_single_edit_fast_path_tests {
    use super::*;

    #[test]
    fn one_edit_threshold_matches_dynamic_programming() {
        let cases = [
            ("", ""), ("a", ""), ("", "z"), ("a", "a"),
            ("a", "b"), ("ab", "ba"), ("ab", "abc"),
            ("abc", "ac"), ("abc", "axc"), ("abc", "xbc"),
            ("abc", "abxc"), ("aaaa", "aaab"), ("aaaa", "baaa"),
            ("aaaa", "aabb"), ("abcd", "ab"),
        ];
        for (left, right) in cases {
            let expected = distance_units(left.as_bytes(), right.as_bytes(), Some(1)).unwrap();
            let actual = evaluate(&[
                Value::Text(left.into()),
                Value::Text(right.into()),
                Value::Integer(1),
            ]).unwrap();
            assert_eq!(actual, expected, "{left:?} -> {right:?}");
            let swapped = evaluate(&[
                Value::Text(right.into()),
                Value::Text(left.into()),
                Value::Integer(1),
            ]).unwrap();
            assert_eq!(swapped, expected, "{right:?} -> {left:?}");
        }
    }

    #[test]
    fn large_ascii_inputs_avoid_quadratic_work() {
        let prefix = "a".repeat(100_000);
        assert_eq!(evaluate(&[
            Value::Text(format!("{prefix}x")),
            Value::Text(format!("{prefix}z")),
            Value::Integer(1),
        ]).unwrap(), Value::Integer(1));
        assert_eq!(evaluate(&[
            Value::Text(format!("{prefix}xy")),
            Value::Text(format!("{prefix}zz")),
            Value::Integer(1),
        ]).unwrap(), Value::Integer(-1));
        assert_eq!(evaluate(&[
            Value::Text("é".into()), Value::Text("e".into()), Value::Integer(1),
        ]).unwrap(), Value::Integer(1));
    }
}


#[cfg(test)]
mod ascii_two_edit_threshold_regressions {
    use super::*;

    #[test]
    fn two_edit_search_matches_dynamic_programming_for_small_ascii_strings() {
        let mut samples = Vec::new();
        for length in 0..=5u32 {
            for mut value in 0..3usize.pow(length) {
                let mut bytes = vec![b'a'; length as usize];
                for byte in &mut bytes {
                    *byte = b'a' + (value % 3) as u8;
                    value /= 3;
                }
                samples.push(bytes);
            }
        }
        for left in &samples {
            for right in &samples {
                let exact = distance_units(left, right, None).unwrap();
                let Value::Integer(distance) = exact else { unreachable!() };
                let expected = if distance <= 2 { distance } else { -1 };
                assert_eq!(two_ascii_edits(left, right), expected,
                    "left={left:?} right={right:?}");
            }
        }
    }

    #[test]
    fn long_shifted_ascii_input_with_two_edits_needs_no_dp_rows() {
        let left = format!("a{}z", "x".repeat(20_000));
        let right = format!("b{}y", "x".repeat(20_000));
        assert_eq!(evaluate(&[
            Value::Text(left), Value::Text(right), Value::Integer(2)
        ]).unwrap(), Value::Integer(2));
    }
}

#[cfg(test)]
mod bounded_unicode_edit_search_regressions {
    use super::*;

    #[test]
    fn small_thresholds_match_unicode_dynamic_programming() {
        let cases = [
            "", "é", "è", "🙂", "界", "aé", "éa", "é界🙂",
            "界é", "🙂é界", "ééé", "🙂🙂a", "ø海a", "海øa",
        ];
        for first in cases {
            for second in cases {
                let left = first.chars().collect::<Vec<_>>();
                let right = second.chars().collect::<Vec<_>>();
                for threshold in [1usize, 2] {
                    let expected = distance_units(&left, &right, Some(threshold)).unwrap();
                    let observed = bounded_unicode_edits(first, second, threshold as u8);
                    assert_eq!(Value::Integer(observed), expected,
                        "{first:?} vs {second:?}, threshold {threshold}");
                }
            }
        }
    }

    #[test]
    fn wide_unicode_inputs_with_tiny_edits_do_not_need_dp_rows() {
        let prefix = "🙂界".repeat(20_000);
        let first = format!("{prefix}é{}", "ø".repeat(10_000));
        let second = format!("{prefix}a{}", "ø".repeat(10_000));
        assert_eq!(evaluate(&[
            Value::Text(first.clone()), Value::Text(second.clone()), Value::Integer(1),
        ]).unwrap(), Value::Integer(1));
        assert_eq!(evaluate(&[
            Value::Text(first), Value::Text(second), Value::Integer(2),
        ]).unwrap(), Value::Integer(1));
    }
}
