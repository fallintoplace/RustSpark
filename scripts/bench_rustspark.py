#!/usr/bin/env python3
"""Reproducible end-to-end local RustSpark SQL benchmark. Python stdlib only."""
import argparse
import hashlib
import json
import math
import pathlib
import platform
import statistics
import subprocess
import tempfile
import time


WORKLOADS = {
    "scan_count": "SELECT COUNT(*) AS n FROM data",
    "filter_sum": "SELECT SUM(amount) AS total FROM data WHERE amount >= 70",
    "group_by": "SELECT city, SUM(amount) AS total FROM data GROUP BY city",
    "aggregate_join": (
        "WITH agg AS (SELECT city, COUNT(*) AS n FROM data GROUP BY city) "
        "SELECT SUM(l.n * r.n) AS pairs FROM agg l JOIN agg r ON l.city = r.city"
    ),
}


ROW_PATTERN_PERIOD = math.lcm(16, 100)
CSV_BUFFER_ROWS = 4096


def benchmark_row(index: int) -> tuple[str, int]:
    return f"city_{index % 16}", (index * 37) % 100


def write_csv(path: pathlib.Path, rows: int):
    suffixes = [
        f"{city},{amount}\n" for city, amount in (benchmark_row(i) for i in range(ROW_PATTERN_PERIOD))
    ]
    with path.open("w", encoding="utf-8", newline="") as file:
        file.write("id,city,amount\n")
        pending = []
        for index in range(rows):
            pending.append(f"{index},{suffixes[index % ROW_PATTERN_PERIOD]}")
            if len(pending) == CSV_BUFFER_ROWS:
                file.write("".join(pending))
                pending.clear()
        if pending:
            file.write("".join(pending))


def percentile(samples, fraction):
    if not 0 < fraction <= 1:
        raise ValueError("fraction must be greater than zero and at most one")
    if not samples:
        raise ValueError("samples cannot be empty")
    ordered = sorted(samples)
    return ordered[max(0, math.ceil(len(ordered) * fraction) - 1)]


def output_signature(stdout: str):
    rows = [json.loads(line) for line in stdout.splitlines() if line.strip()]
    normalized = sorted(json.dumps(row, sort_keys=True) for row in rows)
    digest = hashlib.sha256("\n".join(normalized).encode("utf-8")).hexdigest()
    return digest, rows


def expected_results(rows: int):
    """Compute workload answers from the deterministic benchmark input."""
    counts = {}
    totals = {}
    filtered_total = 0
    filtered_rows = 0
    cycles, remainder = divmod(rows, ROW_PATTERN_PERIOD)
    for index in range(min(rows, ROW_PATTERN_PERIOD)):
        occurrences = cycles + (index < remainder)
        city, amount = benchmark_row(index)
        counts[city] = counts.get(city, 0) + occurrences
        totals[city] = totals.get(city, 0) + amount * occurrences
        if amount >= 70:
            filtered_total += amount * occurrences
            filtered_rows += occurrences
    return {
        "scan_count": [{"n": rows}],
        "filter_sum": [{"total": filtered_total if filtered_rows else None}],
        "group_by": [{"city": city, "total": total} for city, total in totals.items()],
        "aggregate_join": [{"pairs": sum(count * count for count in counts.values())}],
    }


def execute(binary: str, csv: pathlib.Path, sql: str, timeout: int):
    started = time.perf_counter()
    output = subprocess.run(
        [
            binary, "--csv", str(csv), "--csv-schema",
            "id:int64,city:utf8,amount:int64", "--sql", sql,
            "--stdout-format", "json",
        ],
        capture_output=True,
        text=True,
        timeout=timeout,
        check=True,
    )
    elapsed = (time.perf_counter() - started) * 1000
    digest, rows = output_signature(output.stdout)
    return elapsed, digest, rows


def measure_workload(binary, csv, name, sql, expected_digest, repeat, warmup, timeout):
    measurements = []
    digest = None
    for run in range(warmup + repeat):
        elapsed, current_digest, output = execute(binary, csv, sql, timeout)
        if digest is not None and digest != current_digest:
            raise RuntimeError(f"Nondeterministic results for {name}")
        if current_digest != expected_digest:
            raise RuntimeError(f"Incorrect benchmark results for {name}: {output!r}")
        digest = current_digest
        if run >= warmup:
            measurements.append(round(elapsed, 3))
    return {
        "name": name,
        "sql": sql,
        "median_ms": round(statistics.median(measurements), 3),
        "p95_ms": round(percentile(measurements, 0.95), 3),
        "samples_ms": measurements,
        "result_sha256": digest,
    }


def benchmark(binary: str, rows: int, repeat: int, warmup: int, timeout: int):
    if rows < 1 or repeat < 1 or warmup < 0 or timeout < 1:
        raise ValueError("rows, repeat, and timeout must be positive; warmup cannot be negative")
    with tempfile.TemporaryDirectory(prefix="rustspark-benchmark-") as tmp:
        csv = pathlib.Path(tmp) / "input.csv"
        write_csv(csv, rows)
        expected = {
            name: output_signature("\n".join(json.dumps(row) for row in result))[0]
            for name, result in expected_results(rows).items()
        }
        workloads = []
        for name, sql in WORKLOADS.items():
            workloads.append(
                measure_workload(binary, csv, name, sql, expected[name], repeat, warmup, timeout)
            )
        return {
            "binary": binary,
            "rows": rows,
            "csv_bytes": csv.stat().st_size,
            "repeat": repeat,
            "warmup": warmup,
            "python": platform.python_version(),
            "platform": platform.platform(),
            "description": "wall time including CLI startup and CSV scan; not engine-only latency",
            "workloads": workloads,
        }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", default="./target/release/rustspark")
    parser.add_argument("--rows", type=int, default=100000)
    parser.add_argument("--repeat", type=int, default=5)
    parser.add_argument("--warmup", type=int, default=1)
    parser.add_argument("--timeout", type=int, default=120)
    parser.add_argument("--json-out", type=pathlib.Path)
    args = parser.parse_args()
    result = benchmark(args.binary, args.rows, args.repeat, args.warmup, args.timeout)
    output = json.dumps(result, indent=2) + "\n"
    if args.json_out:
        args.json_out.parent.mkdir(parents=True, exist_ok=True)
        args.json_out.write_text(output, encoding="utf-8")
    print(output, end="")


if __name__ == "__main__":
    main()
