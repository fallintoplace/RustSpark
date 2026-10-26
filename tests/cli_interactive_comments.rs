use std::{io::Write, process::{Command, Stdio}};

fn shell(script: &str) -> std::process::Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .arg("--interactive")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("starts shell");
    child.stdin.take().expect("stdin").write_all(script.as_bytes()).expect("writes SQL");
    child.wait_with_output().expect("shell exits")
}

#[test]
fn interactive_executes_statement_with_trailing_line_comment() {
    let output = shell("SELECT 42 AS answer; -- trailing comment\n\\q\n");
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    assert!(String::from_utf8_lossy(&output.stdout).contains("42"));
}

#[test]
fn interactive_executes_statement_with_trailing_block_comment() {
    let output = shell("SELECT '-- ;' AS answer; /* trailing ; comment */\n\\q\n");
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    assert!(String::from_utf8_lossy(&output.stdout).contains("-- ;"));
}

#[test]
fn interactive_does_not_execute_semicolon_inside_comment() {
    let output = shell("SELECT 1 -- ; in comment\n");
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("terminating semicolon"));
}
