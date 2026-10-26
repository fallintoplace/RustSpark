use std::{fs, path::Path, process::Command};

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn run(root: &Path, sql: &str, compact: Option<&str>, crash: Option<&str>) -> std::process::Output {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_rustspark"));
    cmd.arg("--database-dir")
        .arg(root)
        .arg("--sql")
        .arg(sql)
        .args(["--stdout-format", "json"]);
    if let Some(table) = compact {
        cmd.arg("--compact-table").arg(table);
    }
    if let Some(phase) = crash {
        cmd.env("RUSTSPARK_TEST_INSERT_EXIT", phase);
    }
    cmd.output().expect("rustspark runs")
}

fn assert_ok(output: std::process::Output) -> TestResult {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}

fn count_parts(directory: &Path) -> std::io::Result<usize> {
    Ok(fs::read_dir(directory)?
        .filter_map(Result::ok)
        .filter(|item| item.path().extension().is_some_and(|ext| ext == "parquet"))
        .count())
}

#[test]
fn compaction_merges_many_parts_without_losing_rows() -> TestResult {
    let root = tempfile::tempdir()?;
    assert_ok(run(
        root.path(),
        "CREATE TABLE items (n BIGINT)",
        None,
        None,
    ))?;
    for n in 1..=5 {
        assert_ok(run(
            root.path(),
            &format!("INSERT INTO items VALUES ({n})"),
            None,
            None,
        ))?;
    }
    let table = root.path().join("items");
    assert!(count_parts(&table)? > 1);
    assert_ok(run(
        root.path(),
        "SELECT COUNT(*) FROM items",
        Some("items"),
        None,
    ))?;
    assert_eq!(count_parts(&table)?, 1);
    let query = run(root.path(), "SELECT SUM(n) AS total FROM items", None, None);
    assert!(
        query.status.success(),
        "{}",
        String::from_utf8_lossy(&query.stderr)
    );
    assert_eq!(String::from_utf8(query.stdout)?.trim(), r#"{"total":15}"#);
    assert!(!root.path().join(".rustspark-insert-items").exists());
    Ok(())
}

#[test]
fn compaction_preserves_empty_typed_tables() -> TestResult {
    let root = tempfile::tempdir()?;
    assert_ok(run(
        root.path(),
        "CREATE TABLE empty_rows (id BIGINT, text VARCHAR)",
        None,
        None,
    ))?;
    assert_ok(run(
        root.path(),
        "SELECT COUNT(*) FROM empty_rows",
        Some("empty_rows"),
        None,
    ))?;
    assert_eq!(count_parts(&root.path().join("empty_rows"))?, 1);
    let output = run(
        root.path(),
        "SELECT COUNT(*) AS count FROM empty_rows",
        None,
        None,
    );
    assert_eq!(String::from_utf8(output.stdout)?.trim(), r#"{"count":0}"#);
    Ok(())
}

#[test]
fn compaction_recovers_interrupted_publication() -> TestResult {
    for (phase, expected_parts) in [
        ("before_swap", 2),
        ("after_old_moved", 2),
        ("after_publish", 1),
    ] {
        let root = tempfile::tempdir()?;
        assert_ok(run(
            root.path(),
            "CREATE TABLE items (n BIGINT)",
            None,
            None,
        ))?;
        assert_ok(run(root.path(), "INSERT INTO items VALUES (4)", None, None))?;
        assert_eq!(count_parts(&root.path().join("items"))?, 2);
        let output = run(
            root.path(),
            "SELECT SUM(n) FROM items",
            Some("items"),
            Some(phase),
        );
        assert_eq!(
            output.status.code(),
            Some(97),
            "{phase}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let recovered = run(root.path(), "SELECT SUM(n) AS total FROM items", None, None);
        assert!(
            recovered.status.success(),
            "{phase}: {}",
            String::from_utf8_lossy(&recovered.stderr)
        );
        assert_eq!(
            String::from_utf8(recovered.stdout)?.trim(),
            r#"{"total":4}"#
        );
        assert_eq!(count_parts(&root.path().join("items"))?, expected_parts);
        assert!(!root.path().join(".rustspark-insert-items").exists());
    }
    Ok(())
}

#[test]
fn compaction_rejects_nonpersistent_tables() -> TestResult {
    let root = tempfile::tempdir()?;
    let output = run(root.path(), "SELECT 42", Some("missing"), None);
    assert!(!output.status.success());
    assert!(!root.path().join("missing").exists());
    Ok(())
}
