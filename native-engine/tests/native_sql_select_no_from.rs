use rustspark_native::{
    catalog::Catalog, executor::execute, plan::Plan, scalar::Value, sql::parse_sql,
};

fn rows(query: &str) -> Vec<Vec<Value>> {
    execute(&parse_sql(query).unwrap(), &Catalog::new()).unwrap().rows().unwrap()
}

#[test]
fn constant_select_without_from_uses_single_row_relation() {
    let plan = parse_sql("SELECT 2 + 3 AS total, 'spark' AS engine").unwrap();
    assert!(matches!(plan, Plan::Project { input, .. } if *input == Plan::OneRow));
    let result = execute(&parse_sql("SELECT 2 + 3 AS total, 'spark' AS engine").unwrap(), &Catalog::new()).unwrap();
    assert_eq!(result.schema().fields()[0].name, "total");
    assert_eq!(result.schema().fields()[1].name, "engine");
    assert_eq!(result.rows().unwrap(), vec![vec![Value::Integer(5), Value::Text("spark".into())]]);
}

#[test]
fn constant_predicate_and_limit_have_correct_cardinality() {
    assert_eq!(rows("SELECT 7 AS x WHERE TRUE"), vec![vec![Value::Integer(7)]]);
    assert_eq!(rows("SELECT 7 AS x WHERE FALSE"), Vec::<Vec<Value>>::new());
    assert_eq!(rows("SELECT 7 AS x LIMIT 0"), Vec::<Vec<Value>>::new());
    assert_eq!(rows("SELECT 7 AS x WHERE NULL"), Vec::<Vec<Value>>::new());
}

#[test]
fn aggregate_over_one_row_matches_spark_cardinality() {
    assert_eq!(rows("SELECT COUNT(*) AS n"), vec![vec![Value::Integer(1)]]);
    assert_eq!(rows("SELECT SUM(4) AS n"), vec![vec![Value::Integer(4)]]);
    assert_eq!(rows("SELECT COUNT(*) AS n WHERE FALSE"), vec![vec![Value::Integer(0)]]);
}

#[test]
fn invalid_references_and_star_are_rejected() {
    assert!(parse_sql("SELECT *").is_err());
    let plan = parse_sql("SELECT absent_column AS x").unwrap();
    assert!(execute(&plan, &Catalog::new()).is_err());
    assert!(parse_sql("SELECT 1; SELECT 2").is_err());
}

#[test]
fn from_scans_still_use_registered_tables() {
    let plan = parse_sql("SELECT 1 FROM data").unwrap();
    assert!(!matches!(plan, Plan::Project { input, .. } if *input == Plan::OneRow));
}
