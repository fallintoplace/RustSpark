# RustSpark CLI diagnostics

The standard-library diagnostic tool verifies the installed binary,
a standalone SQL query and, optionally, a CSV input. It does not open,
modify, compact, or back up a persistent database.

```sh
python3 scripts/rustspark_doctor.py --binary target/release/rustspark
python3 scripts/rustspark_doctor.py --binary target/release/rustspark \
  --csv tests/fixtures/sales.csv --json
```

The command returns exit code 0 only when all checks succeed.
The JSON report contains check names and pass/fail flags, but not SQL
data rows, environment values, input paths or a database path. Review
reports before sharing them with a third party.

This tool does not claim to test durability, Spark compatibility,
distributed execution, security or benchmark speed. Use the existing
release and integration suites for those checks.

```sh
python3 -m unittest discover -s tests/product_tools -p 'test_rustspark_doctor.py'
```
