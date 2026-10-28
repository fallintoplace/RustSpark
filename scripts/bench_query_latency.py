#!/usr/bin/env python3
"""Measure verified, one-shot RustSpark SQL latencies on deterministic CSV."""

import argparse
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import statistics
import subprocess
import tempfile

from bench_rustspark import (
    ROW_PATTERN_PERIOD,
    benchmark_row,
    execute,
    expected_results,
    output_signature,
    percentile,
    write_csv,
)

# These queries deliberately use the same typed CSV schema as bench_rustspark.py.
# They are separate from WORKLOADS, so cross-engine comparisons stay unchanged.
QUERIES = {
    "scan_count": "SELECT COUNT(*) AS n FROM data",
    "filtered_sum": "SELECT SUM(amount) AS total FROM data WHERE amount >= 70",
    "grouped_sum": "SELECT city, SUM(amount) AS total FROM data GROUP BY city",
    "distinct_cities": "SELECT COUNT(DISTINCT city) AS n FROM data",
    "range_filter": "SELECT COUNT(*) AS n FROM data WHERE amount BETWEEN 20 AND 79",
    "in_filter": (
        "SELECT COUNT(*) AS n FROM data "
        "WHERE city IN ('city_1', 'city_3', 'city_7')"
    ),
    "group_having": (
        "SELECT city, COUNT(*) AS n FROM data "
        "GROUP BY city HAVING COUNT(*) >= 100"
    ),
    "top_k": (
        "SELECT id, amount FROM data ORDER BY amount DESC, id ASC LIMIT 10"
    ),
    "window_rank": (
        "WITH ranked AS (SELECT city, id, "
        "ROW_NUMBER() OVER (PARTITION BY city ORDER BY amount DESC, id ASC) AS rn "
        "FROM data WHERE id < 256) "
        "SELECT city, id, rn FROM ranked WHERE rn <= 2"
    ),
    "union_all": (
        "SELECT SUM(n) AS n FROM ("
        "SELECT COUNT(*) AS n FROM data WHERE amount < 30 "
        "UNION ALL "
        "SELECT COUNT(*) AS n FROM data WHERE amount >= 30"
        ") parts"
    ),
    "self_join": (
        "SELECT SUM(l.amount) AS total FROM data l "
        "INNER JOIN data r ON l.id = r.id WHERE l.amount >= 70"
    ),
    "aggregate_join": (
        "WITH agg AS (SELECT city, COUNT(*) AS n FROM data GROUP BY city) "
        "SELECT SUM(l.n * r.n) AS pairs FROM agg l JOIN agg r ON l.city = r.city"
    ),
}

ORDERED_QUERIES = frozenset({"top_k"})


def _expected_ranked_rows(rows):
    groups = {}
    for index in range(min(rows, 256)):
        city, amount = benchmark_row(index)
        groups.setdefault(city, []).append((index, amount))
    ranked_rows = []
    for city, entries in groups.items():
        for rank, (index, _) in enumerate(
            sorted(entries, key=lambda entry: (-entry[1], entry[0]))[:2], 1
        ):
            ranked_rows.append({"city": city, "id": index, "rn": rank})

    return ranked_rows


def expected_answers(rows):
    """Independent expected values in O(ROW_PATTERN_PERIOD) work per query."""
    if rows < 1:
        raise ValueError("rows must be positive")
    base = expected_results(rows)
    cycles, remainder = divmod(rows, ROW_PATTERN_PERIOD)
    city_counts = {}
    range_count = 0
    in_count = 0
    for index in range(ROW_PATTERN_PERIOD):
        occurrences = cycles + (index < remainder)
        if not occurrences:
            continue
        city, amount = benchmark_row(index)
        city_counts[city] = city_counts.get(city, 0) + occurrences
        if 20 <= amount <= 79:
            range_count += occurrences
        if city in ("city_1", "city_3", "city_7"):
            in_count += occurrences

    # Every amount repeats every 100 rows. The first 1,000 rows include the
    # earliest ten IDs for each amount, which are enough for global top ten.
    top_ids = sorted(
        range(min(rows, 1000)),
        key=lambda index: (-benchmark_row(index)[1], index),
    )[:10]
    top_rows = [
        {"id": index, "amount": benchmark_row(index)[1]} for index in top_ids
    ]

    ranked_rows = _expected_ranked_rows(rows)

    return {
        "scan_count": base["scan_count"],
        "filtered_sum": base["filter_sum"],
        "grouped_sum": base["group_by"],
        "distinct_cities": [{"n": len(city_counts)}],
        "range_filter": [{"n": range_count}],
        "in_filter": [{"n": in_count}],
        "group_having": [
            {"city": city, "n": count}
            for city, count in city_counts.items() if count >= 100
        ],
        "top_k": top_rows,
        "window_rank": ranked_rows,
        "union_all": [{"n": rows}],
        "self_join": base["filter_sum"],
        "aggregate_join": base["aggregate_join"],
    }


def measure_case(binary, csv, name, expected, repeat, warmup, timeout):
    sql = QUERIES[name]
    digest = output_signature("\n".join(json.dumps(row) for row in expected))[0]
    samples = []
    for attempt in range(warmup + repeat):
        elapsed, actual_digest, actual = execute(binary, csv, sql, timeout)
        if (isinstance(elapsed, bool) or not isinstance(elapsed, (int, float))
                or not math.isfinite(elapsed) or elapsed < 0):
            raise ValueError(f"Invalid duration for {name}: {elapsed}")
        if actual_digest != digest:
            raise RuntimeError(f"Incorrect SQL result for {name}: {actual[:10]!r}")
        if name in ORDERED_QUERIES and actual != expected:
            raise RuntimeError(f"Incorrect SQL ordering for {name}: {actual[:10]!r}")
        if attempt >= warmup:
            samples.append(round(elapsed, 3))
    return {
        "status": "ok",
        "samples_ms": samples,
        "median_ms": round(statistics.median(samples), 3),
        "p95_ms": round(percentile(samples, 0.95), 3),
        "result_sha256": digest,
        "result_rows": len(expected),
    }


def sha256_file(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def benchmark(binary, rows, repeat, warmup, timeout, only=None):
    if rows < 1 or repeat < 1 or warmup < 0 or timeout < 1:
        raise ValueError("rows, repeat and timeout must be positive; warmup >= 0")
    cases = list(QUERIES) if only is None else list(only)
    if not cases or len(cases) != len(set(cases)) or any(name not in QUERIES for name in cases):
        raise ValueError("Select one or more distinct, known query names")

    with tempfile.TemporaryDirectory(prefix="rustspark-query-latency-") as tmp:
        csv = Path(tmp) / "data.csv"
        write_csv(csv, rows)
        expected = expected_answers(rows)
        results = []
        for name in cases:
            entry = {"name": name, "sql": QUERIES[name]}
            try:
                entry.update(measure_case(
                    binary, csv, name, expected[name], repeat, warmup, timeout
                ))
            except (OSError, RuntimeError, ValueError, subprocess.SubprocessError) as error:
                detail = error.stderr if isinstance(error, subprocess.CalledProcessError) else None
                if isinstance(detail, bytes):
                    detail = detail.decode(errors="replace")
                entry.update({
                    "status": "error",
                    "error": str(detail or error).strip()[-900:],
                })
            results.append(entry)
        return {
            "metric": "cold process + typed CSV registration + SQL + JSON output",
            "warning": "Wall-clock CLI latency, not warmed query-only or distributed performance",
            "binary": str(binary),
            "revision": os.environ.get("GITHUB_SHA"),
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "platform": platform.platform(),
            "python": platform.python_version(),
            "cpu_count": os.cpu_count(),
            "rows": rows,
            "csv_bytes": csv.stat().st_size,
            "csv_sha256": sha256_file(csv),
            "repeat": repeat,
            "warmup": warmup,
            "results": results,
        }


def render_markdown(report):
    lines = [
        "# RustSpark SQL query latency",
        "",
        f"**Rows:** {report['rows']:,} | **Samples/query:** {report['repeat']} "
        f"| **Warmups/query:** {report['warmup']}",
        f"**Metric:** {report['metric']}",
        f"**Platform:** {report['platform']}",
        f"**Revision:** {report['revision'] or 'not recorded'}",
        "",
        "| Query | Median (ms) | p95 (ms) | Result |",
        "| --- | ---: | ---: | --- |",
    ]
    for result in report["results"]:
        if result["status"] == "ok":
            lines.append(
                f"| {result['name']} | {result['median_ms']:.3f} "
                f"| {result['p95_ms']:.3f} | OK ({result['result_rows']} rows) |"
            )
        else:
            safe_error = (result["error"].replace("&", "&amp;")
                          .replace("<", "&lt;").replace(">", "&gt;")
                          .replace("|", "\\|").replace("\n", " ").replace("\r", " "))
            lines.append(f"| {result['name']} | - | - | ERROR: {safe_error} |")
    lines.extend([
        "",
        f"**Scope:** {report['warning']}.",
        "These are observations on this runner, not stable regression thresholds.",
        "Raw JSON includes each SQL statement, samples and result fingerprints.",
        "",
    ])
    return "\n".join(lines)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", default="./target/release/rustspark")
    parser.add_argument("--rows", type=int, default=100_000)
    parser.add_argument("--repeat", type=int, default=3)
    parser.add_argument("--warmup", type=int, default=1)
    parser.add_argument("--timeout", type=int, default=120)
    parser.add_argument("--only", nargs="+", metavar="QUERY")
    parser.add_argument("--json-out", type=Path)
    parser.add_argument("--markdown-out", type=Path)
    args = parser.parse_args(argv)
    try:
        report = benchmark(
            args.binary, args.rows, args.repeat, args.warmup, args.timeout, args.only
        )
    except ValueError as error:
        parser.error(str(error))
    if args.json_out:
        args.json_out.parent.mkdir(parents=True, exist_ok=True)
        args.json_out.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    summary = render_markdown(report)
    if args.markdown_out:
        args.markdown_out.parent.mkdir(parents=True, exist_ok=True)
        args.markdown_out.write_text(summary, encoding="utf-8")
    print(summary, end="")
    return 1 if any(row["status"] != "ok" for row in report["results"]) else 0


if __name__ == "__main__":
    raise SystemExit(main())
