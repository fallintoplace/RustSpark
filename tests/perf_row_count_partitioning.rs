use std::process::Command;

fn run(extra: &[&str]) -> std::process::Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_rustspark"));
    command.args([
        "--csv",
        "tests/fixtures/sales.csv",
        "--csv-schema",
        "city:utf8,amount:int64",
        "--sql",
        "SELECT city, SUM(amount) AS revenue FROM data GROUP BY city ORDER BY city",
        "--stdout-format",
        "csv",
    ]);
    command.args(extra);
    let result = command.output().expect("run RustSpark CLI");
    assert!(result.status.success(), "{}", String::from_utf8_lossy(&result.stderr));
    result
}

#[test]
fn row_count_partition_estimates_keeps_query_results() {
    assert_eq!(run(&[]).stdout, run(&["--row-count-partition-estimates"]).stdout);
}
