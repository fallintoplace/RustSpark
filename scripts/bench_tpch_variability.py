#!/usr/bin/env python3
"""Flag noisy TPC-H timing samples before claiming a benchmark speedup."""

import argparse
import json
import math
from pathlib import Path
import statistics


def check_samples(entry):
    samples = entry.get("samples_ms")
    if not isinstance(samples, list) or len(samples) < 3:
        raise ValueError("At least three measured samples are required")
    if any(type(x) not in (int, float) or not math.isfinite(x) or x < 0 for x in samples):
        raise ValueError("Invalid timing sample")
    median = statistics.median(samples)
    recorded = entry.get("median_ms")
    if type(recorded) not in (int, float) or not math.isfinite(recorded) or abs(recorded - median) > 0.002:
        raise ValueError("Reported median differs from timing samples")
    return samples


def inspect(report, threshold=0.15):
    if type(threshold) not in (int, float) or not math.isfinite(threshold) or threshold < 0:
        raise ValueError("Variation threshold must be finite and nonnegative")
    results = report.get("results")
    if not isinstance(results, list) or not results:
        raise ValueError("TPC-H report has no query results")
    seen = set()
    rows = []
    for entry in results:
        number = entry.get("id")
        if type(number) is not int or number not in range(1, 23) or number in seen:
            raise ValueError("Invalid or repeated query ID")
        seen.add(number)
        if entry.get("status") != "ok":
            raise ValueError(f"Query Q{number:02} did not succeed")
        samples = check_samples(entry)
        mean = statistics.fmean(samples)
        cv = statistics.pstdev(samples) / mean if mean else 0.0
        median = statistics.median(samples)
        mad = statistics.median(abs(x - median) for x in samples)
        rows.append({"id": number, "coefficient_of_variation": round(cv, 4),
                     "median_absolute_deviation_ms": round(mad, 3),
                     "unstable": cv > threshold, "samples": len(samples)})
    return {"metric": "sample variability for cold TPC-H query timings",
            "warning": "Noise diagnostics; not statistical proof of a speedup",
            "max_cv": threshold, "unstable_queries": sorted(x["id"] for x in rows if x["unstable"]),
            "results": sorted(rows, key=lambda item: item["coefficient_of_variation"], reverse=True)}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("report", type=Path)
    parser.add_argument("--max-cv", type=float, default=0.15)
    parser.add_argument("--json-out", type=Path)
    args = parser.parse_args(argv)
    try:
        result = inspect(json.loads(args.report.read_text(encoding="utf-8")), args.max_cv)
        output = json.dumps(result, indent=2) + "\n"
        if args.json_out:
            args.json_out.parent.mkdir(parents=True, exist_ok=True)
            args.json_out.write_text(output, encoding="utf-8")
        print(output, end="")
    except (OSError, ValueError, TypeError) as error:
        parser.exit(1, f"TPC-H variability check failed: {error}\n")
    return 1 if result["unstable_queries"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
