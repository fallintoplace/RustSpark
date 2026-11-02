use rustspark_native::{catalog::Catalog, executor, sql, scalar::Value};
use rustspark_native::scale::{cancel::QueryControl, file_catalog::FileCatalog,
    stream_execute::{execute, StreamOptions}};



    fn run_memory(query: &str) -> Vec<Vec<Value>> {
        executor::execute(&sql::parse_sql(query).unwrap(), &Catalog::new())
            .unwrap().rows().unwrap()
    }

    fn run_stream(query: &str) -> Vec<Vec<Value>> {
        execute(&sql::parse_sql(query).unwrap(), &FileCatalog::new(),
            &StreamOptions::default(), QueryControl::default()).unwrap()
            .flat_map(|batch| batch.unwrap().rows().unwrap()).collect()
    }

    #[test]
    fn global_distinct_count_sum_avg_agree_with_materialized_executor() {
        for query in [
            "SELECT COUNT(DISTINCT col1) AS n, SUM(DISTINCT col1) AS s, \
                AVG(DISTINCT col1) AS a FROM VALUES (1), (1), (3), (NULL)",
            "SELECT COUNT(DISTINCT col1) AS n, SUM(DISTINCT col1) AS s, \
                AVG(DISTINCT col1) AS a FROM VALUES (NULL), (NULL)",
            "SELECT COUNT(DISTINCT col1) AS n, SUM(DISTINCT col1) AS s, \
                AVG(DISTINCT col1) AS a FROM VALUES (4), (5), (4) WHERE col1 > 4",
        ] {
            assert_eq!(run_stream(query), run_memory(query), "{query}");
        }
    }
