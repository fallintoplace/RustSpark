use std::process::Command;

#[test]
fn join_results_match_untuned_query() {
    let query = ["--csv", "tests/fixtures/sales.csv", "--csv-schema", "city:utf8,amount:int64", "--sql", "SELECT a.city, COUNT(*) AS matches FROM data a JOIN data b ON a.city = b.city GROUP BY a.city ORDER BY a.city", "--stdout-format", "csv"];
    let run = |extra: &[&str]| {
        let output = Command::new(env!("CARGO_BIN_EXE_rustspark")).args(query).args(extra).output().expect("run RustSpark");
        assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
        output.stdout
    };
    assert_eq!(run(&[]), run(&["--hash-join-single-partition-mb", "8"]));
}
