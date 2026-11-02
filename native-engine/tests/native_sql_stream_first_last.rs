use rustspark_native::{catalog::Catalog, executor, sql, scalar::Value};
use rustspark_native::scale::{
    cancel::QueryControl, file_catalog::FileCatalog,
    stream_execute::{execute, StreamOptions},
};



    fn collect(sql_text: &str, stream: bool) -> Vec<Vec<Value>> {
        let plan = sql::parse_sql(sql_text).unwrap();
        let mut rows = if stream {
            execute(&plan, &FileCatalog::new(), &StreamOptions::default(),
                QueryControl::default()).unwrap()
                .flat_map(|batch| batch.unwrap().rows().unwrap()).collect::<Vec<_>>()
        } else {
            executor::execute(&plan, &Catalog::new()).unwrap().rows().unwrap()
        };
        rows.sort_by(|a,b| format!("{a:?}").cmp(&format!("{b:?}")));
        rows
    }

    #[test]
    fn first_last_preserve_null_policy_and_shard_row_order() {
        let sql = "SELECT col1 AS k, FIRST(col2, FALSE) AS first, \
            LAST(col2, TRUE) AS last FROM VALUES \
            (1, NULL), (1, 'first'), (1, 'last'), (2, NULL), (2, 'only') GROUP BY col1";
        assert_eq!(collect(sql, true), collect(sql, false));
    }

    #[test]
    fn first_last_with_computed_arguments_match_in_memory() {
        let sql = "SELECT col1 AS k, FIRST(col2 + 1) AS first, \
            LAST(col2 + 1) AS last FROM VALUES \
            (1, 1), (1, 2), (1, NULL), (2, 3) GROUP BY col1";
        assert_eq!(collect(sql, true), collect(sql, false));
    }
