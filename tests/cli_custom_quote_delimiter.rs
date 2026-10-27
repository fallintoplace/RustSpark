use std::{fs, process::Command};

#[test]
fn double_quote_delimiter_requires_a_different_quote_character()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let csv = dir.path().join("data.csv");
    fs::write(&csv, "city\"amount\n'New\"York'\"10\nBerlin\"20\n")?;
    let run = |quote: Option<&str>| {
        let mut command = Command::new(env!("CARGO_BIN_EXE_rustspark"));
        command.arg("--csv").arg(&csv).args([
            "--csv-delimiter",
            "\"",
            "--sql",
            "SELECT SUM(amount) AS total FROM data",
            "--stdout-format",
            "csv",
        ]);
        if let Some(quote) = quote {
            command.args(["--csv-quote", quote]);
        }
        command.output()
    };
    let success = run(Some("'"))?;
    assert!(
        success.status.success(),
        "{}",
        String::from_utf8_lossy(&success.stderr)
    );
    assert_eq!(String::from_utf8(success.stdout)?, "total\n30\n");

    let invalid = run(None)?;
    assert!(!invalid.status.success());
    assert!(String::from_utf8_lossy(&invalid.stderr).contains("must differ"));
    Ok(())
}
