use std::{
    io::Write,
    process::{Command, Stdio},
};

#[test]
fn shell_lists_tables_and_describes_schema_without_exiting()
-> Result<(), Box<dyn std::error::Error>> {
    let mut process = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args(["--interactive", "--csv", "tests/fixtures/sales.csv"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    process
        .stdin
        .take()
        .ok_or("missing input pipe")?
        .write_all(b".tables\n.schema data\n.schema missing\n.schema\nSELECT 9 AS after_error;\n\\q\n")?;
    let result = process.wait_with_output()?;
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );

    let stdout = String::from_utf8(result.stdout)?;
    assert!(stdout.contains(".data"), "missing table listing: {stdout}");
    assert!(stdout.contains("city\t"), "missing column metadata: {stdout}");
    assert!(stdout.contains("amount\t"), "missing column metadata: {stdout}");
    assert!(stdout.contains('9'), "shell stopped after invalid .schema: {stdout}");

    let stderr = String::from_utf8(result.stderr)?;
    assert!(stderr.contains("Shell error:"), "{stderr}");
    assert!(
        stderr.contains(".schema requires exactly one table name"),
        "{stderr}"
    );
    Ok(())
}
