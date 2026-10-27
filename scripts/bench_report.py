#!/usr/bin/env python3
"""Render checked RustSpark / DuckDB / Spark benchmark JSON as Markdown.

The source harness already checks SQL results against deterministic expectations.
This renderer also rejects malformed timings and cross-engine result mismatches.
"""

import argparse
import json
import math
import pathlib
import statistics


def _unique_json_keys(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"Duplicate JSON key: {key}")
        result[key] = value
    return result


def _reject_nonfinite(value):
    raise ValueError(f"Non-finite JSON constant: {value}")


def parse_report(text):
    return json.loads(
        text, object_pairs_hook=_unique_json_keys, parse_constant=_reject_nonfinite,
    )


def _number(value, field):
    if isinstance(value, bool) or not isinstance(value, (float, int)):
        raise ValueError(f"{field} must be a nonnegative finite number")
    if not math.isfinite(value) or value < 0:
        raise ValueError(f"{field} must be a nonnegative finite number")
    return float(value)


def _cell(value):
    return (str(value).replace("\\", "\\\\").replace("&", "&amp;")
            .replace("<", "&lt;").replace(">", "&gt;")
            .replace("|", "\\|").replace("\n", " ").replace("\r", " "))


def _validate(report):
    if not isinstance(report, dict):
        raise ValueError("Benchmark report must be an object")
    results = report.get("results")
    if not isinstance(results, dict) or not results or "rustspark" not in results:
        raise ValueError("Benchmark must contain RustSpark engine results")
    if isinstance(report.get("rows"), bool) or not isinstance(report.get("rows"), int) or report["rows"] < 1:
        raise ValueError("Benchmark rows must be a positive integer")
    if not isinstance(report.get("metric"), str) or not report["metric"].strip():
        raise ValueError("Benchmark metric is missing")
    if not isinstance(report.get("warning"), str) or not report["warning"].strip():
        raise ValueError("Benchmark warning is missing")
    repeat = report.get("repeat")
    if isinstance(repeat, bool) or not isinstance(repeat, int) or repeat < 1:
        raise ValueError("Benchmark repeat must be a positive integer")
    warmup = report.get("warmup")
    if isinstance(warmup, bool) or not isinstance(warmup, int) or warmup < 0:
        raise ValueError("Benchmark warmup must be a nonnegative integer")
    rustspark = results["rustspark"]
    if not isinstance(rustspark, dict) or not rustspark:
        raise ValueError("RustSpark workloads are missing")
    workloads = list(rustspark)
    for engine, measurements in results.items():
        if not isinstance(measurements, dict) or set(measurements) != set(workloads):
            raise ValueError(f"Workloads differ between RustSpark and {engine}")
        for workload in workloads:
            entry = measurements[workload]
            if not isinstance(entry, dict):
                raise ValueError(f"Invalid measurements for {engine}/{workload}")
            median = _number(entry.get("median_ms"), f"{engine}/{workload} median")
            p95 = _number(entry.get("p95_ms"), f"{engine}/{workload} p95")
            if p95 + 0.001 < median:
                raise ValueError(f"p95 is smaller than median for {engine}/{workload}")
            samples = entry.get("samples_ms")
            if not isinstance(samples, list) or len(samples) != repeat:
                raise ValueError(f"Expected {repeat} samples for {engine}/{workload}")
            for index, sample in enumerate(samples):
                _number(sample, f"{engine}/{workload} sample {index}")
            measured_median = round(statistics.median(samples), 3)
            measured_p95 = round(sorted(samples)[math.ceil(len(samples) * 0.95) - 1], 3)
            if abs(median - measured_median) > 0.001 or abs(p95 - measured_p95) > 0.001:
                raise ValueError(f"Reported timings do not match samples for {engine}/{workload}")
            digest = entry.get("result_sha256")
            if not isinstance(digest, str) or len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest):
                raise ValueError(f"Invalid result digest for {engine}/{workload}")
            if digest != rustspark[workload].get("result_sha256"):
                raise ValueError(f"Result digest mismatch for {engine}/{workload}")
    return results, workloads


def _latency_rows(results, workloads, engines, metric):
    return [
        "| " + _cell(name) + " | " +
        " | ".join(f"{results[engine][name][metric]:.3f}" for engine in engines) + " |"
        for name in workloads
    ]


def render_report(report):
    """Render only actual measured values, preserving the harness's caveats."""
    results, workloads = _validate(report)
    engines = list(results)
    competitors = [engine for engine in engines if engine != "rustspark"]
    lines = [
        "# Local SQL benchmark comparison",
        "",
        f"**Metric:** {_cell(report['metric'])}",
        f"**Rows:** {report['rows']:,}",
        f"**Platform:** {_cell(report.get('platform', 'not recorded'))}",
        f"**Samples per workload:** {_cell(report.get('repeat', 'not recorded'))}",
        f"**Warmups:** {_cell(report.get('warmup', 'not recorded'))}",
        "",
        f"**Scope warning:** {_cell(report['warning'])}",
        "",
        "## Median latency (ms, lower is better)",
        "",
        "| Workload | " + " | ".join(_cell(engine) for engine in engines) + " |",
        "| --- | " + " | ".join("---:" for _ in engines) + " |",
    ]
    lines.extend(_latency_rows(results, workloads, engines, "median_ms"))
    lines.extend(["", "## p95 latency (ms, lower is better)", "",
                  "| Workload | " + " | ".join(_cell(engine) for engine in engines) + " |",
                  "| --- | " + " | ".join("---:" for _ in engines) + " |"])
    lines.extend(_latency_rows(results, workloads, engines, "p95_ms"))
    if competitors:
        lines.extend(["", "## RustSpark speed ratio (competitor median / RustSpark median)", "",
                      "| Workload | " + " | ".join(_cell(f"vs {engine}") for engine in competitors) + " |",
                      "| --- | " + " | ".join("---:" for _ in competitors) + " |"])
        for name in workloads:
            rustspark_ms = results["rustspark"][name]["median_ms"]
            ratios = [f"{results[engine][name]['median_ms'] / rustspark_ms:.2f}x"
                      if rustspark_ms > 0 else "n/a" for engine in competitors]
            lines.append("| " + _cell(name) + " | " + " | ".join(ratios) + " |")
        lines.extend(["", "A ratio above 1.00x means RustSpark was faster **in this run**;",
                      "below 1.00x means RustSpark was slower. Ratios are not distributed benchmarks."])
    else:
        lines.extend(["", "No competing engine was measured; no relative performance claim is possible."])
    lines.extend(["", "All compared workloads passed the harness's expected-result checks.",
                  "This report checks matching result hashes and timing structure, not statistical significance.", ""])
    return "\n".join(lines)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=pathlib.Path, help="JSON file from bench_compare.py or bench_warm_compare.py")
    parser.add_argument("--output", type=pathlib.Path, help="Write Markdown report to this path")
    args = parser.parse_args(argv)
    try:
        report = parse_report(args.input.read_text(encoding="utf-8"))
        rendered = render_report(report)
        if args.output:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(rendered, encoding="utf-8")
        else:
            print(rendered, end="")
    except (OSError, ValueError, TypeError) as error:
        parser.exit(1, f"error: {error}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
