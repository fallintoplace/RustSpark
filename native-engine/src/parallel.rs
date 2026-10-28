use std::sync::{Mutex, atomic::{AtomicUsize, Ordering}};

use crate::{
    batch::Batch,
    catalog::Catalog,
    executor,
    plan::Plan,
    Error, Result,
};

pub fn execute_parallel(
    plans: &[Plan],
    catalog: &Catalog,
    max_workers: usize,
) -> Result<Vec<Batch>> {
    if max_workers == 0 {
        return Err(Error::Execution("native worker count must be positive".into()));
    }
    if plans.is_empty() {
        return Ok(Vec::new());
    }
    let next = AtomicUsize::new(0);
    let results = Mutex::new(vec![None; plans.len()]);
    std::thread::scope(|scope| {
        for _ in 0..max_workers.min(plans.len()) {
            let next = &next;
            let results = &results;
            scope.spawn(move || {
                loop {
                    let index = next.fetch_add(1, Ordering::Relaxed);
                    if index >= plans.len() {
                        break;
                    }
                    let value = executor::execute(&plans[index], catalog);
                    results.lock().expect("native result lock poisoned")[index] = Some(value);
                }
            });
        }
    });
    results.into_inner()
        .map_err(|_| Error::Execution("native worker result lock poisoned".into()))?
        .into_iter()
        .map(|result| result.ok_or_else(|| Error::Execution("worker did not complete a task".into()))?)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{scalar::Value, schema::{DataType, Field, Schema}};

    #[test]
    fn partitions_run_concurrently_without_result_reordering() {
        let mut catalog = Catalog::new();
        catalog.register("x", Batch::from_rows(
            Schema::new(vec![Field::new("x", DataType::Integer, false)]).unwrap(),
            vec![vec![Value::Integer(1)], vec![Value::Integer(2)]],
        ).unwrap()).unwrap();
        let plans = vec![
            Plan::scan("x").limit(1, Some(1)),
            Plan::scan("x").limit(0, Some(1)),
        ];
        let results = execute_parallel(&plans, &catalog, 2).unwrap();
        assert_eq!(results[0].row(0).unwrap(), vec![Value::Integer(2)]);
        assert_eq!(results[1].row(0).unwrap(), vec![Value::Integer(1)]);
        assert!(execute_parallel(&plans, &catalog, 0).is_err());
    }
}
