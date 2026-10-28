use std::process::Command;

#[test]
fn query_results_match_without_tuning() {
    let query = [
        "--csv",
        "tests/fixtures/sales.csv",
        "--csv-schema",
        "city:utf8,amount:int64",
        "--sql",
        "SELECT SUM(amount) AS total FROM data",
        "--stdout-format",
        "csv",
    ];
    let run = |extra: &[&str]| {
        let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
            .args(query)
            .args(extra)
            .output()
            .expect("run RustSpark");
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        output.stdout
    };
    assert_eq!(run(&[]), run(&["--hash-join-inlist-max-values", "8"]));
}
