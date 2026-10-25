use std::{
    io::Write,
    process::{Command, Stdio},
};

#[test]
fn accepts_sql_script_from_stdin() {
    let mut child = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args(["--csv", "tests/fixtures/sales.csv", "--sql-file", "-"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();

    child
        .stdin
        .take()
        .unwrap()
        .write_all(b"SELECT SUM(amount) FROM data")
        .unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("35"));
}
