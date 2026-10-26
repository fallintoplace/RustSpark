use std::{fs, process::Command};

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn run(args: &[&str]) -> Result<std::process::Output, std::io::Error> {
    Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args(args)
        .output()
}

fn query_result(output: std::process::Output, expected: &str) -> TestResult {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let text = String::from_utf8(output.stdout)?;
    assert!(
        text.lines()
            .any(|line| line.trim().trim_matches('|').trim() == expected),
        "{text}"
    );
    Ok(())
}

#[test]
fn reads_typed_hive_partitions_with_pruning_predicates() -> TestResult {
    let tmp = tempfile::tempdir()?;
    let source = tmp.path().join("source.parquet");
    let source_result = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args(["--sql", "SELECT 7 AS amount"])
        .arg("--output-parquet")
        .arg(&source)
        .arg("--output-single-file")
        .output()?;
    assert!(
        source_result.status.success(),
        "{}",
        String::from_utf8_lossy(&source_result.stderr)
    );

    let table = tmp.path().join("events");
    for (year, month) in [(2025, 10), (2026, 11)] {
        let partition = table
            .join(format!("year={year}"))
            .join(format!("month={month}"));
        fs::create_dir_all(&partition)?;
        fs::copy(&source, partition.join("part.parquet"))?;
    }
    let table_path = table.to_str().ok_or("non-UTF8 test path")?;

    query_result(
        run(&[
            "--parquet",
            table_path,
            "--parquet-partitions",
            "year:int32,month:int32",
            "--sql",
            "SELECT COUNT(*) FROM data WHERE year = 2025 AND month = 10",
        ])?,
        "1",
    )?;
    query_result(
        run(&[
            "--parquet-table",
            &format!("events={table_path}"),
            "--parquet-partitions",
            "year:int32,month:int32",
            "--sql",
            "SELECT SUM(amount) FROM events WHERE year = 2026",
        ])?,
        "7",
    )?;
    Ok(())
}

#[test]
fn validates_partition_configuration() -> TestResult {
    for spec in [
        "year:unknown",
        "year:int32,year:int64",
        "1year:int32",
        "year",
    ] {
        let output = run(&[
            "--sql",
            "SELECT 1",
            "--parquet",
            "unused.parquet",
            "--parquet-partitions",
            spec,
        ])?;
        assert!(!output.status.success(), "{spec}");
        assert!(
            String::from_utf8_lossy(&output.stderr).contains("--parquet-partitions"),
            "{spec}"
        );
    }
    let no_parquet = run(&["--sql", "SELECT 1", "--parquet-partitions", "year:int32"])?;
    assert!(!no_parquet.status.success());
    assert!(
        String::from_utf8_lossy(&no_parquet.stderr)
            .contains("--parquet-partitions requires a Parquet input")
    );
    Ok(())
}
