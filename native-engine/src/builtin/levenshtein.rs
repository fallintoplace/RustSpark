use super::Function;
use crate::{scalar::Value, schema::DataType, Error, Result};

fn output(types: &[DataType]) -> Result<DataType> {
    super::text_type(types)?;
    Ok(DataType::Integer)
}

fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(first) = super::string(&args[0])? else { return Ok(Value::Null); };
    let Some(second) = super::string(&args[1])? else { return Ok(Value::Null); };
    let a = first.chars().collect::<Vec<_>>();
    let b = second.chars().collect::<Vec<_>>();
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
    Ok(Value::Integer(i64::try_from(previous[shorter.len()])
        .map_err(|_| Error::Execution("LEVENSHTEIN result overflow".into()))?))
}

pub(super) const FUNCTIONS: &[Function] = &[Function {
    name: "levenshtein", min_args: 2, max_args: 2,
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
        assert!(evaluate(&[Value::Text("a".repeat(2000)), Value::Text("b".repeat(2000))])
            .is_err());
    }
}
