use std::process::Command;

#[test]
fn grouped_query_returns_same_result() {
    let query = ["--csv", "tests/fixtures/sales.csv", "--csv-schema", "city:utf8,amount:int64", "--sql", "SELECT city, SUM(amount) AS total FROM data GROUP BY city ORDER BY city", "--stdout-format", "csv"];
    let run = |extra: &[&str]| {
        let output = Command::new(env!("CARGO_BIN_EXE_rustspark")).args(query).args(extra).output().expect("run RustSpark");
        assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
        output.stdout
    };
    assert_eq!(run(&[]), run(&["--partial-aggregate-probe-rows", "8"]));
}
