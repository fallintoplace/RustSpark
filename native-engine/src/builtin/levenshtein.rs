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
    if a.len().checked_mul(b.len()).is_none_or(|cells| cells > 2_000_000) {
        return Err(Error::Execution("LEVENSHTEIN exceeds native edit-distance work budget".into()));
    }
    let (longer, shorter) = if a.len() >= b.len() { (&a, &b) } else { (&b, &a) };
    let mut previous = (0..=shorter.len()).collect::<Vec<_>>();
    let mut current = vec![0usize; shorter.len() + 1];
    for (i, from) in longer.iter().enumerate() {
        current[0] = i + 1;
        for (j, to) in shorter.iter().enumerate() {
            let insertion = current[j].saturating_add(1);
            let deletion = previous[j + 1].saturating_add(1);
            let substitution = previous[j] + usize::from(from != to);
            current[j + 1] = insertion.min(deletion).min(substitution);
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

pub(super) const FUNCTIONS: &[Function] = &[Function {
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
    }
}
