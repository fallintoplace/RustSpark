use rustspark_native::{aggregate::{aggregate_global_stream, AggregateItem, AggregateOp}, batch::Batch,
    expr::Expr, scalar::Value, schema::{DataType, Field, Schema}};



    #[test]
    fn distinct_state_deduplicates_across_stream_batches() {
        let schema = Schema::new(vec![Field::new("id", DataType::Integer, true)]).unwrap();
        let one = Batch::from_rows(schema.clone(), vec![
            vec![Value::Integer(1)], vec![Value::Null], vec![Value::Integer(2)],
        ]).unwrap();
        let two = Batch::from_rows(schema.clone(), vec![
            vec![Value::Integer(1)], vec![Value::Integer(3)], vec![Value::Integer(3)],
        ]).unwrap();
        let col = Expr::Column("id".into());
        let items = vec![
            AggregateItem::new(AggregateOp::CountDistinct(col.clone()), "n"),
            AggregateItem::new(AggregateOp::SumDistinct(col.clone()), "total"),
            AggregateItem::new(AggregateOp::AvgDistinct(col), "mean"),
        ];
        let output = aggregate_global_stream([Ok(one), Ok(two)].into_iter(),
            &schema, &items, || Ok(())).unwrap();
        assert_eq!(output.rows().unwrap(), vec![vec![
            Value::Integer(3), Value::Integer(6), Value::Float(2.0),
        ]]);
    }
