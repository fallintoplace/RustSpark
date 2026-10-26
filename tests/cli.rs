use std::process::Command;

fn sales_csv() -> &'static str {
    concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/sales.csv")
}

fn sales_json() -> &'static str {
    concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/sales.json")
}

#[test]
fn help_explains_options() {
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .arg("--help")
        .output()
        .unwrap();

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("--csv"));
    assert!(stdout.contains("--parquet"));
    assert!(stdout.contains("--json"));
    assert!(stdout.contains("--sql"));
}

#[test]
fn runs_a_query_from_the_command_line() {
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args([
            "--csv",
            sales_csv(),
            "--table",
            "sales",
            "--sql",
            "SELECT SUM(amount) AS total FROM sales",
        ])
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(
        stdout
            .lines()
            .any(|line| line.trim().trim_matches('|').trim() == "35"),
        "{stdout}"
    );
}

#[test]
fn reports_missing_required_arguments() {
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .arg("--csv")
        .arg(sales_csv())
        .output()
        .unwrap();

    assert!(!output.status.success());
    assert!(String::from_utf8(output.stderr).unwrap().contains("--sql"));
}

#[test]
fn runs_query_with_default_table_name() {
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args([
            "--csv",
            sales_csv(),
            "--sql",
            "SELECT COUNT(*) AS total FROM data",
        ])
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(
        stdout
            .lines()
            .any(|line| line.trim().trim_matches('|').trim() == "3"),
        "{stdout}"
    );
}

#[test]
fn reports_a_missing_csv_file() {
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args([
            "--csv",
            "tests/fixtures/this_file_does_not_exist.csv",
            "--sql",
            "SELECT * FROM data",
        ])
        .output()
        .unwrap();

    assert!(!output.status.success());
    assert!(!output.stderr.is_empty());
}

#[test]
fn rejects_multiple_input_formats() {
    let binary = env!("CARGO_BIN_EXE_rustspark");
    let both = Command::new(binary)
        .args([
            "--csv",
            sales_csv(),
            "--parquet",
            sales_csv(),
            "--sql",
            "SELECT 1",
        ])
        .output()
        .unwrap();
    assert!(!both.status.success());
}

#[test]
fn queries_json_from_the_command_line() {
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args([
            "--json",
            sales_json(),
            "--sql",
            "SELECT SUM(amount) AS total FROM data",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout
            .lines()
            .any(|line| line.trim().trim_matches('|').trim() == "35")
    );
}

#[test]
fn refuses_two_input_formats() {
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args([
            "--csv",
            sales_csv(),
            "--json",
            sales_json(),
            "--sql",
            "SELECT 1",
        ])
        .output()
        .unwrap();
    assert!(!output.status.success());
}

#[test]
fn runs_query_from_sql_file() {
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args([
            "--csv",
            sales_csv(),
            "--sql-file",
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/fixtures/count_sales.sql"
            ),
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout
            .lines()
            .any(|line| line.trim().trim_matches('|').trim() == "3")
    );
}

#[test]
fn rejects_two_query_sources() {
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args([
            "--csv",
            sales_csv(),
            "--sql",
            "SELECT 1",
            "--sql-file",
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/fixtures/count_sales.sql"
            ),
        ])
        .output()
        .unwrap();
    assert!(!output.status.success());
}

#[test]
fn reports_missing_sql_file() {
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args(["--csv", sales_csv(), "--sql-file", "missing.sql"])
        .output()
        .unwrap();
    assert!(!output.status.success());
}

#[test]
fn parses_semicolon_delimited_csv() {
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args([
            "--csv",
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/fixtures/sales_semicolon.csv"
            ),
            "--csv-delimiter",
            ";",
            "--sql",
            "SELECT SUM(amount) FROM data",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout
            .lines()
            .any(|line| line.trim().trim_matches('|').trim() == "15")
    );
}

#[test]
fn rejects_invalid_csv_delimiters() {
    for delimiter in ["::", "🔥"] {
        let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
            .args([
                "--csv",
                sales_csv(),
                "--csv-delimiter",
                delimiter,
                "--sql",
                "SELECT 1",
            ])
            .output()
            .unwrap();
        assert!(!output.status.success(), "{delimiter}");
    }
}

#[test]
fn rejects_csv_delimiter_for_parquet() {
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args([
            "--parquet",
            sales_csv(),
            "--csv-delimiter",
            ";",
            "--sql",
            "SELECT 1",
        ])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("--csv-delimiter requires --csv"));
}

#[test]
fn joins_multiple_named_csv_tables() {
    let left = format!("left_sales={}", sales_csv());
    let right = format!("right_sales={}", sales_csv());
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args([
            "--csv-table",
            left.as_str(),
            "--csv-table",
            right.as_str(),
            "--sql",
            "SELECT COUNT(*) FROM left_sales JOIN right_sales USING (city)",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout
            .lines()
            .any(|line| line.trim().trim_matches('|').trim() == "5")
    );
}

#[test]
fn rejects_invalid_named_csv_inputs() {
    for mapping in ["missing_equals", "=file.csv", "123=path.csv", "sales="] {
        let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
            .args(["--csv-table", mapping, "--sql", "SELECT 1"])
            .output()
            .unwrap();
        assert!(!output.status.success(), "{mapping}");
    }
}

#[test]
fn rejects_named_and_unnamed_csv_together() {
    let mapping = format!("sales={}", sales_csv());
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args([
            "--csv",
            sales_csv(),
            "--csv-table",
            mapping.as_str(),
            "--sql",
            "SELECT 1",
        ])
        .output()
        .unwrap();
    assert!(!output.status.success());
}

#[test]
fn limits_printed_query_results() {
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args([
            "--csv",
            sales_csv(),
            "--sql",
            "SELECT city, amount FROM data ORDER BY amount DESC",
            "--max-rows",
            "1",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("20"));
    assert!(!stdout.contains("Munich"));
    assert!(!stdout.contains("10"));
}

#[test]
fn rejects_invalid_row_limits() {
    for rows in ["0", "-1", "not-a-number"] {
        let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
            .args([
                "--csv",
                sales_csv(),
                "--sql",
                "SELECT 1",
                "--max-rows",
                rows,
            ])
            .output()
            .unwrap();
        assert!(!output.status.success(), "{rows}");
    }
}

#[test]
fn joins_named_json_and_csv_tables() {
    let csv = format!(
        "sales={}/tests/fixtures/sales.csv",
        env!("CARGO_MANIFEST_DIR")
    );
    let json = format!("events={}", sales_json());
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args([
            "--csv-table",
            &csv,
            "--json-table",
            &json,
            "--sql",
            "SELECT COUNT(*) FROM sales JOIN events USING (city)",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("5"));
}

#[test]
fn rejects_invalid_named_json_tables() {
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args(["--json-table", "123=events.json", "--sql", "SELECT 1"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("Named table"));
}

#[test]
fn queries_headerless_csv_rows() {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/sales_no_header.csv"
    );
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args([
            "--csv",
            path,
            "--csv-no-header",
            "--sql",
            "SELECT COUNT(*) FROM data",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("3"));
}

#[test]
fn rejects_csv_no_header_without_csv() {
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args([
            "--json",
            sales_json(),
            "--csv-no-header",
            "--sql",
            "SELECT 1",
        ])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("requires a CSV input"));
}

#[test]
fn configures_csv_schema_inference_rows() {
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args([
            "--csv",
            sales_csv(),
            "--csv-schema-rows",
            "1",
            "--sql",
            "SELECT SUM(amount) FROM data",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("35"));
}

#[test]
fn rejects_invalid_csv_schema_row_count() {
    for rows in ["0", "-1", "abc"] {
        let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
            .args([
                "--csv",
                sales_csv(),
                "--csv-schema-rows",
                rows,
                "--sql",
                "SELECT 1",
            ])
            .output()
            .unwrap();
        assert!(!output.status.success(), "{rows}");
    }
}

#[test]
fn skips_commented_csv_lines() {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/sales_commented.csv"
    );
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args([
            "--csv",
            path,
            "--csv-comment",
            "#",
            "--sql",
            "SELECT SUM(amount) FROM data",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("35"));
}

#[test]
fn rejects_csv_comment_for_json_inputs() {
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args([
            "--json",
            sales_json(),
            "--csv-comment",
            "#",
            "--sql",
            "SELECT 1",
        ])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("requires a CSV input"));
}

#[test]
fn queries_json_array_files() {
    let file = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/sales_array.json"
    );
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args([
            "--json",
            file,
            "--json-array",
            "--sql",
            "SELECT SUM(amount) FROM data",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("35"));
}

#[test]
fn rejects_json_array_flag_for_csv_inputs() {
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args(["--csv", sales_csv(), "--json-array", "--sql", "SELECT 1"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("requires a JSON input"));
}

#[test]
fn configures_query_execution_batch_size() {
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args([
            "--csv",
            sales_csv(),
            "--batch-size",
            "1",
            "--sql",
            "SELECT SUM(amount) FROM data",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("35"));
}

#[test]
fn rejects_invalid_batch_sizes() {
    for size in ["0", "-1", "nope"] {
        let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
            .args([
                "--csv",
                sales_csv(),
                "--batch-size",
                size,
                "--sql",
                "SELECT 1",
            ])
            .output()
            .unwrap();
        assert!(!output.status.success(), "{size}");
    }
}

#[tokio::test]
async fn exports_query_results_to_parquet() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let export = dir.path().join("parquet_results");
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args([
            "--csv",
            sales_csv(),
            "--sql",
            "SELECT city, amount FROM data",
            "--output-parquet",
            export.to_str().unwrap(),
        ])
        .output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );

    let session = rustspark::RustSparkSession::new();
    let count = session.read_parquet(&export).await?.count().await?;
    assert_eq!(count, 3);
    Ok(())
}

#[test]
fn rejects_parquet_export_with_preview_limit() {
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args([
            "--csv",
            sales_csv(),
            "--sql",
            "SELECT 1",
            "--output-parquet",
            "results",
            "--max-rows",
            "1",
        ])
        .output()
        .unwrap();
    assert!(!output.status.success());
}

#[test]
fn exports_query_results_to_csv() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let export = dir.path().join("csv_results");
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args([
            "--csv",
            sales_csv(),
            "--sql",
            "SELECT city, amount FROM data",
            "--output-csv",
            export.to_str().unwrap(),
        ])
        .output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );

    let mut content = String::new();
    for entry in std::fs::read_dir(&export)? {
        let path = entry?.path();
        if path.extension().is_some_and(|extension| extension == "csv") {
            content.push_str(&std::fs::read_to_string(path)?);
        }
    }
    assert!(content.contains("Berlin"));
    assert!(content.contains("Munich"));
    Ok(())
}

#[test]
fn exports_query_results_to_json() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let export = dir.path().join("json_results");
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args([
            "--csv",
            sales_csv(),
            "--sql",
            "SELECT city, amount FROM data",
            "--output-json",
            export.to_str().unwrap(),
        ])
        .output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );

    let mut content = String::new();
    for entry in std::fs::read_dir(&export)? {
        let path = entry?.path();
        if path
            .extension()
            .is_some_and(|extension| extension == "json")
        {
            content.push_str(&std::fs::read_to_string(path)?);
        }
    }
    assert!(content.contains("Berlin"));
    assert!(content.contains("Munich"));
    Ok(())
}

#[test]
fn rejects_two_export_formats() {
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args([
            "--csv",
            sales_csv(),
            "--sql",
            "SELECT 1",
            "--output-csv",
            "csv",
            "--output-json",
            "json",
        ])
        .output()
        .unwrap();
    assert!(!output.status.success());
}

#[test]
fn rejects_conflicting_csv_parser_characters() {
    let path = sales_csv();
    for flags in [
        ["--csv-comment", ","],
        ["--csv-comment", "\""],
        ["--csv-escape", ","],
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
            .args(["--csv", path, flags[0], flags[1], "--sql", "SELECT COUNT(*) FROM data"])
            .output()
            .unwrap();
        assert!(!output.status.success(), "{flags:?}");
        assert!(
            String::from_utf8_lossy(&output.stderr).contains("must differ"),
            "{flags:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}
