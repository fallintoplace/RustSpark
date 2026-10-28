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
    fixture = output / f"sf-{scale:g}"
    report = output / f"sf-{scale:g}.json"
    directory = Path(__file__).parent
    prepare = [sys.executable, str(directory / "tpch_prepare.py"),
               "--scale-factor", str(scale), "--output", str(fixture)]
    run = [sys.executable, str(directory / "tpch_run.py"), "--binary", str(binary),
           "--parquet-dir", str(fixture / "data"), "--queries-dir", str(fixture / "queries"),
           "--repeat", "1", "--warmup", "0", "--json-out", str(report)]
    return fixture, report, prepare, run


def run_scale(prepare, run, report):
    subprocess.run(prepare, check=True, timeout=1800)
    process = subprocess.run(
        run, check=False, capture_output=True, text=True, timeout=7200,
    )
    rows = json.loads(report.read_text())["results"]
    ok = sum(row.get("status") == "ok" for row in rows)
    return {"attempted": len(rows), "passed": ok,
            "status": "ok" if len(rows) == 22 and ok == 22 and process.returncode == 0 else "error"}


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
