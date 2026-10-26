use std::process::Command;

#[test]
fn rejects_offset_overflow_before_loading_inputs() {
    let directory = tempfile::tempdir().unwrap();
    let missing = directory.path().join("missing.csv");
    for format in [None, Some("csv")] {
        let mut command = Command::new(env!("CARGO_BIN_EXE_rustspark"));
        command
            .arg("--csv")
            .arg(&missing)
            .args(["--sql", "SELECT * FROM data", "--max-rows", "2", "--offset"])
            .arg(usize::MAX.to_string());
        if let Some(format) = format {
            command.args(["--stdout-format", format]);
        }
        let output = command.output().unwrap();
        assert!(!output.status.success());
        assert!(
            String::from_utf8_lossy(&output.stderr)
                .contains("--offset plus --max-rows exceeds the supported range"),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}
