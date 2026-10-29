use rustspark_native::{
    batch::Batch, catalog::Catalog, executor::execute, scalar::Value,
    schema::{DataType, Field, Schema}, sql::parse_sql,
};

fn catalog() -> Catalog {
    let mut catalog = Catalog::new();
    catalog.register("t", Batch::from_rows(
        Schema::new(vec![Field::new("txt", DataType::Text, true)]).unwrap(),
        vec![
            vec![Value::Text("a_b".into())],
            vec![Value::Text("axb".into())],
            vec![Value::Text("a%b".into())],
            vec![Value::Text("Å_B".into())],
            vec![Value::Null],
        ],
    ).unwrap()).unwrap();
    catalog
}
fn rows(sql: &str) -> Vec<Vec<Value>> {
    execute(&parse_sql(sql).unwrap(), &catalog()).unwrap().rows().unwrap()
}

#[test]
fn default_escape_matches_literal_wildcards() {
    assert_eq!(rows(r"SELECT txt LIKE 'a\_b' FROM t"), vec![
        vec![Value::Boolean(true)], vec![Value::Boolean(false)],
        vec![Value::Boolean(false)], vec![Value::Boolean(false)],
        vec![Value::Null],
    ]);
    assert_eq!(rows(r"SELECT txt LIKE 'a\%b' FROM t")[2], vec![Value::Boolean(true)]);
}

#[test]
fn custom_escape_matches_literals_and_not_like_preserves_null() {
    assert_eq!(rows("SELECT txt LIKE 'a$_b' ESCAPE '$' FROM t")[0],
        vec![Value::Boolean(true)]);
    assert_eq!(rows("SELECT txt LIKE 'a$%b' ESCAPE '$' FROM t")[2],
        vec![Value::Boolean(true)]);
    assert_eq!(rows("SELECT txt NOT LIKE 'a$_b' ESCAPE '$' FROM t")[0],
        vec![Value::Boolean(false)]);
    assert_eq!(rows("SELECT txt NOT LIKE 'a$_b' ESCAPE '$' FROM t")[4], vec![Value::Null]);
}

#[test]
fn case_insensitive_like_respects_unicode_and_escape() {
    let rows = rows("SELECT txt ILIKE 'å$_b' ESCAPE '$' FROM t");
    assert_eq!(rows[3], vec![Value::Boolean(true)]);
    assert_eq!(rows[4], vec![Value::Null]);
}

#[test]
fn invalid_escape_patterns_and_non_character_escapes_fail() {
    for query in [
        "SELECT txt LIKE 'a%' ESCAPE '' FROM t",
        "SELECT txt LIKE 'a%' ESCAPE 'ab' FROM t",
        "SELECT txt LIKE 'a%' ESCAPE 1 FROM t",
        "SELECT txt LIKE 'a$xb' ESCAPE '$' FROM t",
        "SELECT txt LIKE 'a$' ESCAPE '$' FROM t",
    ] {
        assert!(execute(&parse_sql(query).unwrap(), &catalog()).is_err(), "{query}");
    }
}

#[test]
fn escaped_wildcards_are_not_expanded() {
    assert_eq!(rows("SELECT txt LIKE 'a_b' FROM t")[1], vec![Value::Boolean(true)]);
    assert_eq!(rows("SELECT txt LIKE 'a$_b' ESCAPE '$' FROM t")[1], vec![Value::Boolean(false)]);
}

#[test]
fn rlike_still_uses_regex_semantics() {
    assert_eq!(rows("SELECT txt RLIKE '^a.b$' FROM t")[0], vec![Value::Boolean(true)]);
}
