use std::process::Command;

#[test]
fn positional_booleans_accept_sql_case_insensitive_literals()
-> Result<(), Box<dyn std::error::Error>> {
    for (value, expected) in [("bool:TRUE", "1"), ("boolean:FaLsE", "0")] {
        let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
            .args([
                "--sql",
                "SELECT CASE WHEN $1 THEN 1 ELSE 0 END",
                "--param",
                value,
            ])
            .output()?;
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let text = String::from_utf8(output.stdout)?;
        assert!(
            text.lines()
                .any(|line| line.trim().trim_matches('|').trim() == expected)
        );
    }
    Ok(())
}

#[test]
fn named_booleans_accept_uppercase_literal() -> Result<(), Box<dyn std::error::Error>> {
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args([
            "--sql",
            "SELECT CASE WHEN $ready THEN 42 ELSE 0 END",
            "--named-param",
            "ready=bool:TRUE",
        ])
        .output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("42"));
    Ok(())
}
