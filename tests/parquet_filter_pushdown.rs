use std::process::Command;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[test]
fn selective_parquet_filters_return_the_same_rows() -> TestResult {
    let dir = tempfile::tempdir()?;
    let file = dir.path().join("sales.parquet");
    let export = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args([
            "--csv",
            "tests/fixtures/sales.csv",
            "--sql",
            "SELECT city, amount FROM data",
            "--output-parquet",
        ])
        .arg(&file)
        .arg("--output-single-file")
        .output()?;
    assert!(
        export.status.success(),
        "{}",
        String::from_utf8_lossy(&export.stderr)
    );

    for (pushdown, reorder) in [(false, false), (true, false), (true, true)] {
        let mut command = Command::new(env!("CARGO_BIN_EXE_rustspark"));
        command
            .arg("--parquet")
            .arg(&file)
            .args([
                "--sql",
                "SELECT SUM(amount) FROM data WHERE city = 'Berlin' AND amount > 10",
            ]);
        if pushdown {
            command.arg("--parquet-pushdown-filters");
        }
        if reorder {
            command.arg("--parquet-reorder-filters");
        }
        let output = command.output()?;
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let stdout = String::from_utf8(output.stdout)?;
        assert!(
            stdout
                .lines()
                .any(|line| line.trim().trim_matches('|').trim() == "20"),
            "{stdout}"
        );
    }
    Ok(())
}

#[test]
fn rejects_reordering_without_pushdown() -> TestResult {
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args(["--sql", "SELECT 1", "--parquet-reorder-filters"])
        .output()?;
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("--parquet-pushdown-filters")
    );
    Ok(())
}

#[test]
fn rejects_parquet_tuning_without_parquet_input() -> TestResult {
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args([
            "--csv",
            "tests/fixtures/sales.csv",
            "--sql",
            "SELECT 1",
            "--parquet-pushdown-filters",
        ])
        .output()?;
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("requires a Parquet input")
    );
    Ok(())
}
