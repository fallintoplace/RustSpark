use std::process::Command;

#[test]
fn query_outputs_are_unchanged() {
    let query = ["--csv", "tests/fixtures/sales.csv", "--csv-schema", "city:utf8,amount:int64", "--sql", "SELECT city, amount FROM data ORDER BY amount DESC LIMIT 2", "--stdout-format", "csv"];
    let run = |extra: &[&str]| {
        let output = Command::new(env!("CARGO_BIN_EXE_rustspark")).args(query).args(extra).output().expect("run RustSpark");
        assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
        output.stdout
    };
    assert_eq!(run(&[]), run(&["--sort-in-place-kb", "8"]));
}
