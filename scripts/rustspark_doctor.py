#!/usr/bin/env python3
"""Check that a RustSpark CLI can run basic, read-only SQL without exposing data."""
from __future__ import annotations

import argparse
import json
import os
import platform
import subprocess
from pathlib import Path


def invoke(args: list[str], timeout: float = 15) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        args, text=True, encoding="utf-8", capture_output=True,
        check=False, timeout=timeout,
    )


def diagnose(binary: str, csv: str | None = None) -> dict:
    """Return a support report that avoids filesystem paths and environment values."""
    report = {
        "engine": "rustspark",
        "platform": platform.system(),
        "checks": [],
    }

    def check(name: str, args: list[str], *, expected: str | None = None) -> None:
        item = {"name": name, "ok": False}
        try:
            result = invoke([binary, *args])
            if result.returncode != 0:
                item["error"] = f"exit status {result.returncode}"
            elif expected is None:
                item["ok"] = bool(result.stdout.strip())
            else:
                try:
                    rows = json.loads(result.stdout)
                    item["ok"] = isinstance(rows, list) and len(rows) == 1 and rows[0].get("probe") == 42
                except (ValueError, AttributeError, TypeError):
                    item["error"] = "Unexpected JSON result"
        except FileNotFoundError:
            item["error"] = "Binary not found"
        except subprocess.TimeoutExpired:
            item["error"] = "Command timed out"
        except OSError:
            item["error"] = "Unable to execute binary"
        report["checks"].append(item)

    check("version", ["--version"])
    check("standalone_sql", [
        "--sql", "SELECT 42 AS probe", "--stdout-format", "json-array", "--max-rows", "1",
    ], expected="probe")
    if csv is not None:
        source = Path(csv)
        if not source.is_file():
            report["checks"].append({"name": "csv_input", "ok": False, "error": "CSV file not found"})
        else:
            # A user-supplied CSV path is never copied into the report.
            check("csv_input", [
                "--csv", os.fspath(source), "--sql", "SELECT 42 AS probe FROM data LIMIT 1",
                "--stdout-format", "json-array", "--max-rows", "1",
            ], expected="probe")
    report["ok"] = all(item["ok"] for item in report["checks"])
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", default="rustspark")
    parser.add_argument("--csv", help="Optional CSV input to test, path not included in report")
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON")
    args = parser.parse_args()
    report = diagnose(args.binary, args.csv)
    if args.json:
        print(json.dumps(report, indent=2))
    else:
        print(f"RustSpark diagnostic ({report['platform']})")
        for item in report["checks"]:
            state = "PASS" if item["ok"] else "FAIL"
            detail = f" ({item['error']})" if "error" in item else ""
            print(f"{state}: {item['name']}{detail}")
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
