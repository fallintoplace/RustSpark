#!/usr/bin/env python3
"""Generate and run independent TPC-H datasets across scale factors."""

import argparse
import json
import math
from pathlib import Path
import subprocess
import sys


def scales(raw):
    numbers = []
    for item in raw.split(","):
        try:
            value = float(item.strip())
        except ValueError as error:
            raise ValueError(f"Invalid scale factor: {item}") from error
        if not math.isfinite(value) or value <= 0 or value in numbers:
            raise ValueError("Scale factors must be unique, finite and positive")
        numbers.append(value)
    return numbers


def commands(scale, binary, output):
    # repr keeps distinct binary floats distinct while preserving short names.
    fixture = output / f"sf-{scale!r}"
    report = output / f"sf-{scale!r}.json"
    directory = Path(__file__).parent
    prepare = [sys.executable, str(directory / "tpch_prepare.py"),
               "--scale-factor", str(scale), "--output", str(fixture)]
    run = [sys.executable, str(directory / "tpch_run.py"), "--binary", str(binary),
           "--parquet-dir", str(fixture / "data"), "--queries-dir", str(fixture / "queries"),
           "--repeat", "1", "--warmup", "0", "--json-out", str(report)]
    return fixture, report, prepare, run


def valid_query_measurement(row):
    median = row.get("median_ms")
    rows = row.get("result_rows")
    hashes = (row.get("sql_sha256"), row.get("result_sha256"))
    return (row.get("status") == "ok"
            and type(median) in (int, float) and math.isfinite(median) and median > 0
            and type(rows) is int and rows >= 0
            and all(type(value) is str and len(value) == 64
                    and all(char in "0123456789abcdef" for char in value)
                    for value in hashes))


def run_scale(prepare, run, report):
    subprocess.run(prepare, check=True, timeout=1800)
    # Never consume a report left by an earlier invocation.
    report.unlink(missing_ok=True)
    process = subprocess.run(
        run, check=False, capture_output=True, text=True, timeout=7200,
    )
    rows = json.loads(report.read_text())["results"]
    if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
        raise ValueError("Invalid TPC-H query results")
    ok = sum(row.get("status") == "ok" for row in rows)
    ids = [row.get("id") for row in rows]
    complete = (len(rows) == 22 and ok == 22 and process.returncode == 0
                and all(type(number) is int for number in ids)
                and set(ids) == set(range(1, 23))
                and all(valid_query_measurement(row) for row in rows))
    return {"attempted": len(rows), "passed": ok,
            "status": "ok" if complete else "error"}


def measure_scale(factor, binary, output):
    fixture, report, prepare, run = commands(factor, binary, output)
    item = {"scale_factor": factor, "fixture": fixture.name, "report": report.name}
    try:
        item.update(run_scale(prepare, run, report))
    except (OSError, ValueError, KeyError, subprocess.SubprocessError) as error:
        item.update({"status": "error", "error": str(error)[-500:]})
    return item


def measure(binary, output, factors):
    if output.exists():
        raise ValueError("Output directory must not exist")
    output.mkdir(parents=True)
    results = []
    for factor in factors:
        results.append(measure_scale(factor, binary, output))
    summary = {"metric": "cold RustSpark process per query", "scales": results}
    (output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    return summary


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", default="./target/release/rustspark")
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--scales", default="0.01,0.1")
    args = parser.parse_args(argv)
    try:
        report = measure(args.binary, args.output_dir, scales(args.scales))
    except (OSError, ValueError) as error:
        parser.exit(1, f"TPC-H scale sweep failed: {error}\n")
    print(json.dumps(report, indent=2))
    return 0 if all(row["status"] == "ok" for row in report["scales"]) else 1


if __name__ == "__main__":
    raise SystemExit(main())
