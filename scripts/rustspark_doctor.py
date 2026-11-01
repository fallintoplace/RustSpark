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


def unique_json_fields(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate diagnostic result key")
        result[key] = value
    return result


def diagnose(binary: str, csv: str | None = None,
             parquet: str | None = None, json_input: str | None = None) -> dict:
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
                    rows = json.loads(result.stdout, object_pairs_hook=unique_json_fields)
                    item["ok"] = (
                        isinstance(rows, list) and len(rows) == 1
                        and isinstance(rows[0], dict) and set(rows[0]) == {expected}
                        and type(rows[0][expected]) is int and rows[0][expected] == 42
                    )
                    if not item["ok"]:
                        item["error"] = "Unexpected JSON result"
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
    for label, path, flag in (
        ("csv_input", csv, "--csv"),
        ("parquet_input", parquet, "--parquet"),
        ("json_input", json_input, "--json"),
    ):
        if path is None:
            continue
        source = Path(path)
        if not source.is_file():
            report["checks"].append({"name": label, "ok": False, "error": "Input file not found"})
        else:
            # A user-supplied path is never copied into the report.
            check(label, [
                flag, os.fspath(source), "--sql",
                "SELECT 42 AS probe FROM (SELECT COUNT(*) AS row_count FROM data) AS input_count",
                "--stdout-format", "json-array", "--max-rows", "1",
            ], expected="probe")
    report["ok"] = all(item["ok"] for item in report["checks"])
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", default="rustspark")
    parser.add_argument("--csv", help="Optional CSV input to test")
    parser.add_argument("--parquet", help="Optional Parquet input to test")
    parser.add_argument("--json-input", help="Optional JSON or NDJSON input to test")
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON")
    args = parser.parse_args()
    report = diagnose(args.binary, args.csv, args.parquet, args.json_input)
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
