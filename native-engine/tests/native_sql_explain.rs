use rustspark_native::{catalog::Catalog, executor, scalar::Value, sql::parse_sql};



    fn explanation(query: &str) -> String {
        let plan = parse_sql(query).unwrap();
        let batch = executor::execute(&plan, &Catalog::new()).unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.schema().fields()[0].name, "logical_plan");
        let rows = batch.rows().unwrap();
        let Value::Text(description) = &rows[0][0] else { panic!("EXPLAIN text expected"); };
        description.clone()
    }

    #[test]
    fn explain_does_not_execute_missing_table_scans() {
        let text = explanation("EXPLAIN SELECT id FROM unavailable WHERE id > 10");
        assert!(text.contains("Logical plan"));
        assert!(text.contains("Scan"));
        assert!(text.contains("unavailable"));
        assert!(!text.contains("Optimized logical plan"));
    }

    #[test]
    fn extended_explain_includes_optimizer_plan_and_ctes() {
        let text = explanation("EXPLAIN EXTENDED WITH c AS (SELECT 2 AS id) SELECT id FROM c");
        assert!(text.contains("Logical plan"));
        assert!(text.contains("Optimized logical plan"));
        assert!(text.contains("Project"));
    }

    #[test]
    fn explain_rejects_unsupported_analyze_and_trailing_statements() {
        assert!(parse_sql("EXPLAIN ANALYZE SELECT 1").is_err());
        assert!(parse_sql("EXPLAIN SELECT 1; SELECT 2").is_err());
        assert!(parse_sql("EXPLAIN").is_err());
    }
