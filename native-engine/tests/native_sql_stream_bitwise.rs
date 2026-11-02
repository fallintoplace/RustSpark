use rustspark_native::{catalog::Catalog, executor, sql, scalar::Value};
use rustspark_native::scale::{
    cancel::QueryControl, file_catalog::FileCatalog,
    stream_execute::{execute, output_schema, StreamOptions},
};



    fn in_memory(query: &str) -> Vec<Vec<Value>> {
        executor::execute(&sql::parse_sql(query).unwrap(), &Catalog::new())
            .unwrap().rows().unwrap()
    }
    fn in_stream(query: &str) -> Vec<Vec<Value>> {
        execute(&sql::parse_sql(query).unwrap(), &FileCatalog::new(),
            &StreamOptions::default(), QueryControl::default()).unwrap()
            .flat_map(|batch| batch.unwrap().rows().unwrap()).collect()
    }
    fn sorted(mut rows: Vec<Vec<Value>>) -> Vec<Vec<Value>> {
        rows.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));
        rows
    }

    #[test]
    fn bitwise_spill_matches_in_memory_with_nulls_and_negative_values() {
        let query = "SELECT col1 AS k, BIT_AND(col2) AS a, BIT_OR(col2) AS o, \
            BIT_XOR(col2) AS x FROM VALUES (1, 7), (1, 3), (1, NULL), \
            (2, NULL), (2, -1), (2, 6), (3, NULL) GROUP BY col1";
        assert_eq!(sorted(in_stream(query)), sorted(in_memory(query)));
    }

    #[test]
    fn computed_bitwise_values_and_wrong_types_are_checked() {
        let query = "SELECT col1 AS k, BIT_XOR(col2 + 1) AS x \
            FROM VALUES (1, 3), (1, 5), (2, 6) GROUP BY col1";
        assert_eq!(sorted(in_stream(query)), sorted(in_memory(query)));
        let invalid = sql::parse_sql(
            "SELECT col1, BIT_AND(col2) FROM VALUES (1, 'wrong') GROUP BY col1"
        ).unwrap();
        assert!(output_schema(&invalid, &FileCatalog::new(),
            &StreamOptions::default()).is_err());
    }
