use std::process::Command;

#[test]
fn accepts_narrow_and_unsigned_typed_parameters() {
    for (parameter, expected) in [
        ("int8:-12", "-12"),
        ("int16:-1234", "-1234"),
        ("int32:50000", "50000"),
        ("uint8:255", "255"),
        ("uint16:65535", "65535"),
        ("uint32:4294967295", "4294967295"),
        ("uint64:18446744073709551615", "18446744073709551615"),
        ("float32:1.25", "1.25"),
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
            .args([
                "--sql",
                "SELECT $1 AS value",
                "--param",
                parameter,
                "--stdout-format",
                "json",
            ])
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{parameter}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            String::from_utf8_lossy(&output.stdout).contains(expected),
            "{parameter}: {}",
            String::from_utf8_lossy(&output.stdout)
        );
    }
}

#[test]
fn rejects_out_of_range_and_nonfinite_numeric_parameters() {
    for parameter in [
        "int8:128",
        "uint8:-1",
        "uint64:18446744073709551616",
        "float32:NaN",
        "float32:Infinity",
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
            .args(["--sql", "SELECT $1", "--param", parameter])
            .output()
            .unwrap();
        assert!(!output.status.success(), "{parameter}");
    }
}
