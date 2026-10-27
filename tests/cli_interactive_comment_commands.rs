use std::{io::Write, process::{Command, Stdio}};

fn shell(script: &str) -> Result<std::process::Output, Box<dyn std::error::Error>> {
    let mut child = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args(["--interactive", "--csv", "tests/fixtures/sales.csv"])
        .stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped()).spawn()?;
    child.stdin.take().ok_or("missing stdin")?.write_all(script.as_bytes())?;
    Ok(child.wait_with_output()?)
}

#[test]
fn comment_only_lines_do_not_hide_shell_commands() -> Result<(), Box<dyn std::error::Error>> {
    let output = shell(
        "-- leading comment\n/* block comment\n * finished here */\n.tables\nSELECT 9 AS answer;\n\\q\n",
    )?;
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    let stdout = String::from_utf8(output.stdout)?;
    assert!(stdout.contains("datafusion.public.data"), "{stdout}");
    assert!(stdout.contains('9'), "{stdout}");
    Ok(())
}

#[test]
fn comment_only_input_at_eof_is_not_unfinished_sql() -> Result<(), Box<dyn std::error::Error>> {
    for comment in ["-- comment only", "/* comment only */\n"] {
        let output = shell(comment)?;
        assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    }
    Ok(())
}
