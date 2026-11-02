use rustspark_native::{catalog::Catalog, executor, scalar::Value, sql::parse_sql};



    fn run(query: &str) -> Vec<Vec<Value>> {
        executor::execute(&parse_sql(query).unwrap(), &Catalog::new())
            .unwrap().rows().unwrap()
    }

    #[test]
    fn aliases_work_for_inline_values_derived_queries_and_ctes() {
        assert_eq!(run("SELECT n FROM VALUES (2), (3) v(n) ORDER BY n"),
            vec![vec![Value::Integer(2)], vec![Value::Integer(3)]]);
        assert_eq!(run("SELECT n FROM (SELECT 9 AS n) derived"),
            vec![vec![Value::Integer(9)]]);
        assert_eq!(run("WITH source AS (SELECT 5 AS n) SELECT n FROM source s"),
            vec![vec![Value::Integer(5)]]);
        assert_eq!(run("SELECT n FROM (VALUES (4)) v(n) WHERE n > 2"),
            vec![vec![Value::Integer(4)]]);
    }

    #[test]
    fn clause_keywords_do_not_become_relation_aliases() {
        assert_eq!(run("SELECT n FROM (VALUES (1), (2)) v(n) WHERE n > 1"),
            vec![vec![Value::Integer(2)]]);
        assert!(parse_sql("SELECT n FROM (VALUES (1)) v(n) WHERE").is_err());
        assert!(parse_sql("SELECT n FROM (VALUES (1)) v(n) JOIN").is_err());
        assert!(parse_sql("SELECT n FROM (VALUES (1)) AS").is_err());
    }
