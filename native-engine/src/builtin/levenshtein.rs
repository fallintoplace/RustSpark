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
    let a = first.chars().collect::<Vec<_>>();
    let b = second.chars().collect::<Vec<_>>();
    // Removing identical edges does not change the edit distance.
    let mut prefix = 0;
    while prefix < a.len() && prefix < b.len() && a[prefix] == b[prefix] {
        prefix += 1;
    }
    let (mut a_end, mut b_end) = (a.len(), b.len());
    while a_end > prefix && b_end > prefix && a[a_end - 1] == b[b_end - 1] {
        a_end -= 1;
        b_end -= 1;
    }
    let a = &a[prefix..a_end];
    let b = &b[prefix..b_end];
    if threshold.is_some_and(|limit| a.len().abs_diff(b.len()) > limit) {
        return Ok(Value::Integer(-1));
    }
    let (longer, shorter) = if a.len() >= b.len() { (&a, &b) } else { (&b, &a) };
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
mod levenshtein_identical_fast_path_tests {
    use super::*;
    #[test]
    fn identical_unicode_skips_character_buffers_with_or_without_threshold() {
        let value = Value::Text("🙂é".repeat(20_000));
        assert_eq!(evaluate(&[value.clone(), value.clone()]).unwrap(), Value::Integer(0));
        assert_eq!(evaluate(&[value.clone(), value, Value::Integer(0)]).unwrap(), Value::Integer(0));
    }
}
