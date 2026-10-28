#!/usr/bin/env python3
"""Compare base and PR JSON_VALID end-to-end latency on the same CI runner."""

import argparse
import csv
import hashlib
import json
import math
from pathlib import Path
import platform
import statistics
import subprocess
import tempfile
import time


QUERY = "SELECT COUNT(*) AS n FROM data WHERE json_valid(payload)"
_VALID_OBJECT = json.dumps(
    {
        "events": [
            {"id": index, "tags": ["alpha", "beta", "München"], "active": index % 2 == 0}
            for index in range(24)
        ],
        "meta": {"source": "perf", "notes": "a" * 96},
    },
    ensure_ascii=False,
    separators=(",", ":"),
)
_VALID_ARRAY = json.dumps(
    [{"name": "value" * 12, "index": index} for index in range(16)],
    separators=(",", ":"),
)
# Include malformed JSON and primitive roots, not only the easy valid-object path.
PAYLOADS = (
    (_VALID_OBJECT, True),
    (_VALID_ARRAY, True),
    (_VALID_OBJECT[:-1], False),
    ("{invalid", False),
    ("null", True),
    ('[1,2,{"ok":true}]', True),
)


def expected_valid_count(rows):
    full, remainder = divmod(rows, len(PAYLOADS))
    return (full * sum(int(valid) for _, valid in PAYLOADS)
            + sum(int(valid) for _, valid in PAYLOADS[:remainder]))


def write_fixture(path, rows):
    with path.open("w", encoding="utf-8", newline="") as output:
        writer = csv.writer(output)
        writer.writerow(["payload"])
        for index in range(rows):
            writer.writerow([PAYLOADS[index % len(PAYLOADS)][0]])


def run_case(binary, csv_path, expected, timeout):
    start = time.perf_counter()
    output = subprocess.run(
        [
            str(binary), "--csv", str(csv_path), "--csv-schema", "payload:utf8",
            "--sql", QUERY, "--stdout-format", "json",
        ],
        capture_output=True, text=True, check=True, timeout=timeout,
    )
    elapsed = (time.perf_counter() - start) * 1000
    try:
        rows = [json.loads(line) for line in output.stdout.splitlines() if line.strip()]
    except json.JSONDecodeError as error:
        raise ValueError(f"Invalid SQL JSON result from {binary}: {error}") from error
    if rows != [{"n": expected}]:
        raise ValueError(
            f"Wrong JSON_VALID count from {binary}: expected {expected}, got {rows!r}"
        )
    if not math.isfinite(elapsed) or elapsed <= 0:
        raise ValueError("Benchmark returned an invalid elapsed time")
    return elapsed


def compare(baseline_binary, candidate_binary, rows=120_000, repeat=7, warmup=2,
            timeout=120):
    if rows < 1 or repeat < 1 or warmup < 0 or timeout < 1:
        raise ValueError("rows, repeat and timeout must be positive; warmup >= 0")
    with tempfile.TemporaryDirectory(prefix="rustspark-json-valid-") as directory:
        fixture = Path(directory) / "payloads.csv"
        write_fixture(fixture, rows)
        fixture_sha256 = hashlib.sha256(fixture.read_bytes()).hexdigest()
        expected = expected_valid_count(rows)
        binaries = {"baseline": baseline_binary, "candidate": candidate_binary}
        samples = {"baseline": [], "candidate": []}
        # Alternate run order. Both versions use the same fixture and host.
        for trial in range(warmup + repeat):
            order = ("baseline", "candidate") if trial % 2 == 0 else ("candidate", "baseline")
            for name in order:
                duration = run_case(binaries[name], fixture, expected, timeout)
                if trial >= warmup:
                    samples[name].append(duration)
        base_ms = statistics.median(samples["baseline"])
        candidate_ms = statistics.median(samples["candidate"])
        return {
            "metric": "end-to-end CLI startup + typed CSV scan + JSON_VALID + output",
            "warning": "shared-runner latency is informational, not a performance gate",
            "platform": platform.platform(),
            "rows": rows,
            "expected_valid_rows": expected,
            "csv_bytes": fixture.stat().st_size,
            "csv_sha256": fixture_sha256,
            "query": QUERY,
            "repeat": repeat,
            "warmup": warmup,
            "baseline_binary": str(baseline_binary),
            "candidate_binary": str(candidate_binary),
            "samples_ms": {
                name: [round(value, 3) for value in values]
                for name, values in samples.items()
            },
            "baseline_median_ms": round(base_ms, 3),
            "candidate_median_ms": round(candidate_ms, 3),
            "speedup": round(base_ms / candidate_ms, 3),
            "latency_reduction_pct": round(100 * (1 - candidate_ms / base_ms), 2),
        }


def markdown(report):
    return (
        "## JSON_VALID: base vs pull request\n\n"
        f"**Rows:** {report['rows']:,} (valid: {report['expected_valid_rows']:,})"
        f" | **Samples per version:** {report['repeat']}"
        f" | **Warmups:** {report['warmup']}\n\n"
        "| Base median | PR median | Speedup (base / PR) | Latency reduction |\n"
        "| ---: | ---: | ---: | ---: |\n"
        f"| {report['baseline_median_ms']:.3f} ms"
        f" | {report['candidate_median_ms']:.3f} ms"
        f" | **{report['speedup']:.3f}x**"
        f" | {report['latency_reduction_pct']:+.2f}% |\n\n"
        "The same deterministic CSV is queried by both release binaries"
        " on the same runner. Both results must match the expected count."
        " Positive latency reduction means the PR is faster.\n\n"
        f"**Metric:** {report['metric']}.\n\n"
        f"**Caution:** {report['warning']}; compare repeated CI runs"
        " before concluding that a small difference is real.\n"
    )


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-binary", type=Path, required=True)
    parser.add_argument("--candidate-binary", type=Path, required=True)
    parser.add_argument("--rows", type=int, default=120_000)
    parser.add_argument("--repeat", type=int, default=7)
    parser.add_argument("--warmup", type=int, default=2)
    parser.add_argument("--timeout", type=int, default=120)
    parser.add_argument("--json-out", type=Path)
    parser.add_argument("--markdown-out", type=Path)
    args = parser.parse_args(argv)
    try:
        report = compare(
            args.baseline_binary, args.candidate_binary, args.rows,
            args.repeat, args.warmup, args.timeout,
        )
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        parser.exit(1, f"benchmark failed: {error}\n")
    summary = markdown(report)
    if args.json_out:
        args.json_out.parent.mkdir(parents=True, exist_ok=True)
        args.json_out.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    if args.markdown_out:
        args.markdown_out.parent.mkdir(parents=True, exist_ok=True)
        args.markdown_out.write_text(summary, encoding="utf-8")
    print(summary, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
