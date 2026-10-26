use std::process::Command;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[test]
fn reads_uint64_values_outside_signed_range() -> TestResult {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("wide.csv");
    std::fs::write(&path, "name,value\nmax,18446744073709551615\n")?;
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args([
            "--csv", path.to_str().unwrap(),
            "--csv-schema", "name:utf8,value:uint64",
            "--sql", "SELECT value FROM data",
        ])
        .output()?;
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    assert!(String::from_utf8_lossy(&output.stdout).contains("18446744073709551615"));
    Ok(())
}

#[test]
fn describes_extended_csv_types() -> TestResult {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("events.csv");
    std::fs::write(&path, "small,rate,event_date\n1,1.25,2026-10-26\n")?;
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args([
            "--csv", path.to_str().unwrap(),
            "--csv-schema", "small:int32,rate:float32,event_date:date32",
            "--sql", "SELECT * FROM data",
            "--describe",
        ])
        .output()?;
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("small\tInt32"));
    assert!(stdout.contains("rate\tFloat32"));
    assert!(stdout.contains("event_date\tDate32"));
    Ok(())
}
