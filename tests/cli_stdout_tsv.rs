use std::process::Command;

#[test]
fn streams_tsv_with_a_single_header() -> Result<(), Box<dyn std::error::Error>> {
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args([
            "--sql",
            "SELECT 'a,b' AS city, CAST(7 AS BIGINT) AS amount",
            "--stdout-format",
            "tsv",
        ])
        .output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        String::from_utf8(output.stdout)?,
        "city\tamount\na,b\t7\n"
    );
    Ok(())
}

#[test]
fn emits_tsv_headers_when_query_is_empty() -> Result<(), Box<dyn std::error::Error>> {
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args([
            "--sql",
            "SELECT 1 AS answer WHERE FALSE",
            "--stdout-format",
            "tsv",
        ])
        .output()?;
    assert!(output.status.success());
    assert_eq!(String::from_utf8(output.stdout)?, "answer\n");
    Ok(())
}
