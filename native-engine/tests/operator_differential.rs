//! Deterministic differential coverage for native sort, Top-K and spilled merge.
//! Replays the same seed and input order on failure, with no external dependencies.
use rustspark_native::{
    batch::Batch, batch_chunks, expr::{BinaryOp, Expr},
    external_sort::external_sort, scalar::Value, schema::{DataType, Field, Schema},
    sort::{self, SortKey}, topk::{top_k, top_k_owned}, Result,
};

fn next(seed: &mut u64) -> u64 {
    *seed = seed.wrapping_mul(6_364_136_223_846_793_005)
        .wrapping_add(1_442_695_040_888_963_407);
    *seed
}

fn randomized_batch(seed: u64, schema: &Schema) -> Batch {
    let mut random = seed.wrapping_add(0xa5a5_5a5a_0123_4567);
    let rows = (0..73i64).map(|row| {
        let score = match next(&mut random) % 13 {
            0 => Value::Null,
            1 => Value::Float(f64::NAN),
            2 => Value::Float(-0.0),
            3 => Value::Float(0.0),
            number => Value::Float((number as i64 - 7) as f64),
        };
        let label = match next(&mut random) % 7 {
            0 => Value::Null,
            1 => Value::Text(String::new()),
            n => Value::Text(format!("🦀{}-{n}", "é".repeat((row % 11) as usize))),
        };
        vec![score, label, Value::Integer(row)]
    }).collect();
    Batch::from_rows(schema.clone(), rows).unwrap()
}

#[test]
fn full_sort_topk_owned_topk_and_external_merge_are_equivalent() {
    let schema = Schema::new(vec![
        Field::new("score", DataType::Float, true),
        Field::new("label", DataType::Text, true),
        Field::new("id", DataType::Integer, false),
    ]).unwrap();

    for seed in 0u64..24 {
        let batch = randomized_batch(seed, &schema);
        let mut keys = vec![
            SortKey {
                expression: Expr::Column("score".into()),
                descending: seed % 2 == 0,
                nulls_first: seed % 4 < 2,
            },
            SortKey::ascending(Expr::Column("label".into())),
        ];
        if seed % 3 == 0 {
            keys.push(SortKey::ascending(Expr::binary(
                Expr::Column("id".into()), BinaryOp::Add,
                Expr::Literal(Value::Integer(1)),
            )));
        }
        let expected = sort::sort(&batch, &keys).unwrap().rows().unwrap();
        for k in [0usize, 1, 2, 7, 31, 72, 73, 90] {
            let prefix = &expected[..k.min(expected.len())];
            let borrowed = top_k(&batch, &keys, k).unwrap().rows().unwrap();
            let owned = top_k_owned(batch.clone(), &keys, k).unwrap().rows().unwrap();
            assert_eq!(borrowed, prefix, "borrowed topk seed={seed} k={k}");
            assert_eq!(owned, prefix, "owned topk seed={seed} k={k}");
        }

        let chunk_rows = 3 + seed as usize % 11;
        let input = batch_chunks::chunks(&batch, chunk_rows).unwrap()
            .collect::<Result<Vec<_>>>().unwrap();
        let runs = 1 + seed as usize % 8;
        let output_rows = 1 + seed as usize % 9;
        let output = external_sort(input, &schema, &keys, std::env::temp_dir(),
            runs, output_rows).unwrap()
            .collect::<Result<Vec<_>>>().unwrap()
            .into_iter().flat_map(|b| b.rows().unwrap()).collect::<Vec<_>>();
        assert_eq!(output, expected, "external merge seed={seed} run_rows={runs}");
    }
}
