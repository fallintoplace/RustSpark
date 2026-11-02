use rustspark_native::{catalog::Catalog, executor, sql, scalar::Value};
use rustspark_native::scale::{
    cancel::QueryControl, file_catalog::FileCatalog,
    stream_execute::{execute, StreamOptions},
};



    fn memory_result(sql_text: &str) -> Vec<Vec<Value>> {
        executor::execute(&sql::parse_sql(sql_text).unwrap(), &Catalog::new())
            .unwrap().rows().unwrap()
    }

    fn stream_result(sql_text: &str) -> Vec<Vec<Value>> {
        let plan = sql::parse_sql(sql_text).unwrap();
        execute(&plan, &FileCatalog::new(), &StreamOptions {
            batch_rows: 2,
            sort_run_rows: 2,
            ..StreamOptions::default()
        }, QueryControl::default()).unwrap()
            .flat_map(|batch| batch.unwrap().rows().unwrap()).collect()
    }

    #[test]
    fn expression_sorts_match_memory_across_many_merge_runs() {
        for query in [
            "SELECT col1 AS id, col2 AS tag FROM VALUES (2, 'bbb'), (1, 'a'), (3, 'cc'), (4, 'dddd') ORDER BY col1 * -1",
            "SELECT col1 AS id, col2 AS tag FROM VALUES (2, 'bbb'), (1, 'a'), (3, 'cc'), (4, 'dddd') ORDER BY LENGTH(col2) DESC, col1",
            "SELECT col1 AS id, col2 AS tag FROM VALUES (2, 'bbb'), (1, 'a'), (3, 'cc'), (4, 'dddd') ORDER BY col1 % 2, col1",
        ] {
            assert_eq!(stream_result(query), memory_result(query), "{query}");
        }
    }

    #[test]
    fn computed_sort_keys_are_not_visible_and_nulls_keep_order() {
        let query = "SELECT col1 AS id FROM VALUES (NULL), (3), (1) ORDER BY col1 + 1 NULLS FIRST";
        assert_eq!(stream_result(query), memory_result(query));
        assert!(stream_result(query).iter().all(|row| row.len() == 1));
    }
