use std::process::Command;

fn output(extra: &[&str]) -> String {
    let mut command = Command::new(env!("CARGO_BIN_EXE_rustspark"));
    command.args([
        "--csv",
        "tests/fixtures/sales.csv",
        "--csv-schema",
        "city:utf8,amount:int64",
        "--sql",
        "SELECT city, amount FROM (SELECT city, amount, ROW_NUMBER() OVER (PARTITION BY city ORDER BY amount DESC) AS rn FROM data) WHERE rn <= 2 ORDER BY city, amount",
        "--stdout-format",
        "csv",
    ]);
    command.args(extra);
    let result = command.output().expect("start RustSpark CLI");
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    String::from_utf8(result.stdout).expect("UTF-8 CSV")
}

#[test]
fn window_topn_preserves_ranked_query_results() {
    assert_eq!(output(&[]), output(&["--window-topn"]));
}
