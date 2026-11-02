use rustspark_native::{catalog::Catalog, executor, scalar::Value, sql::parse_sql};



    fn run(sql: &str) -> Vec<Vec<Value>> {
        executor::execute(&parse_sql(sql).unwrap(), &Catalog::new())
            .unwrap().rows().unwrap()
    }

    #[test]
    fn cross_join_accepts_derived_and_values_relations() {
        let expected = vec![
            vec![Value::Integer(1), Value::Integer(3)],
            vec![Value::Integer(1), Value::Integer(4)],
            vec![Value::Integer(2), Value::Integer(3)],
            vec![Value::Integer(2), Value::Integer(4)],
        ];
        assert_eq!(run("SELECT x, y FROM (VALUES (1), (2)) AS a(x) CROSS JOIN (VALUES (3), (4)) AS b(y)"), expected);
        assert_eq!(run("SELECT x, y FROM (VALUES (1), (2)) AS a(x), (VALUES (3), (4)) AS b(y)"), expected);
        assert_eq!(run("SELECT x, y FROM (SELECT col1 AS x FROM VALUES (1), (2)) AS a CROSS JOIN (SELECT col1 AS y FROM VALUES (3), (4)) AS b"), expected);
    }

    #[test]
    fn comma_join_and_explicit_cross_join_preserve_zero_cardinality() {
        let sql = "SELECT x FROM (VALUES (1), (2)) AS a(x), (SELECT 1 AS y WHERE FALSE) AS b";
        assert!(run(sql).is_empty());
        assert_eq!(run("SELECT x FROM (VALUES (7)) AS a(x) CROSS JOIN (SELECT 1 AS y) AS b"),
            vec![vec![Value::Integer(7)]]);
        assert!(parse_sql("SELECT x FROM (VALUES (1)) AS a(x),").is_err());
    }
