#!/usr/bin/env python3
"""Run line-delimited SQL workloads against a Parquet file and retain failures."""

import argparse
import hashlib
import json
import math
from pathlib import Path
import platform
import re
import statistics
import subprocess
import time

VIEW_SQL = (
    'hits=SELECT * EXCEPT ("EventDate"), '
    'CAST(CAST("EventDate" AS INTEGER) AS DATE) AS "EventDate" FROM hits_raw'
)


def read_queries(path: Path) -> list[str]:
    queries = [line.strip() for line in path.read_text(encoding="utf-8").splitlines()]
    queries = [line for line in queries if line]
    if not queries:
        raise ValueError("query file is empty")
    for number, query in enumerate(queries, 1):
        if not query.upper().startswith("SELECT ") or not query.endswith(";") or query.count(";") != 1:
            raise ValueError(f"query {number} is not one semicolon-terminated SELECT statement")
    return queries


def parse_query_numbers(only: str, query_count: int) -> list[int]:
    numbers = []
    seen = set()
    for entry in only.split(","):
        if not entry.strip().isdigit():
            raise ValueError("--only needs comma-separated positive query numbers")
        number = int(entry.strip())
        if number < 1 or number > query_count:
            raise ValueError(f"query number out of range: {number}")
        if number in seen:
            raise ValueError(f"duplicate query number: {number}")
        seen.add(number)
        numbers.append(number)
    return numbers


def choose_queries(queries: list[str], only: str | None) -> list[tuple[int, str]]:
    if not only:
        return list(enumerate(queries, 1))
    return [(number, queries[number - 1]) for number in parse_query_numbers(only, len(queries))]


def command(binary: str, parquet: Path, sql: str, engine_args: tuple[str, ...] = ()) -> list[str]:
    return [
        binary,
        "--parquet", str(parquet),
        "--table", "hits_raw",
        "--parquet-binary-as-string",
        "--sql-view", VIEW_SQL,
        "--sql", sql,
        "--stdout-format", "json",
    ] + list(engine_args)


def reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict:
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON result key: {key}")
        result[key] = value
    return result


def reject_nonfinite(value: str):
    raise ValueError(f"invalid JSON numeric value: {value}")


def has_top_level_order_by(sql: str) -> bool:
    # Ignore quoted values, identifiers, and SQL comments before scanning keywords.
    masked = re.sub(
        r"""'(?:''|[^'])*'|"(?:\"\"|[^"])*"|--[^\n]*|/\*.*?\*/""",
        " ",
        sql,
        flags=re.DOTALL,
    )
    depth = 0
    previous = ""
    for token in re.findall(r"[A-Za-z_]+|[()]", masked):
        if token == "(":
            depth += 1
        elif token == ")":
            depth = max(0, depth - 1)
        elif depth == 0:
            word = token.upper()
            if previous == "ORDER" and word == "BY":
                return True
            previous = word
    return False


def result_signature(stdout: str, ordered: bool = False) -> tuple[str, int]:
    rows = []
    for line in stdout.splitlines():
        if line.strip():
            value = json.loads(
                line, object_pairs_hook=reject_duplicate_keys, parse_constant=reject_nonfinite,
            )
            if not isinstance(value, dict):
                raise ValueError("expected a JSON object for each result row")
            rows.append(json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False))
    if not ordered:
        rows.sort()
    digest = hashlib.sha256("\n".join(rows).encode("utf-8")).hexdigest()
    return digest, len(rows)


def execute(
    binary: str, parquet: Path, sql: str, timeout: float, engine_args: tuple[str, ...] = (),
) -> dict:
    started = time.perf_counter()
    try:
        process = subprocess.run(
            command(binary, parquet, sql, engine_args),
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return {"error": f"Timed out after {timeout:g} seconds", "elapsed_ms": round((time.perf_counter() - started) * 1000, 3)}
    except (OSError, UnicodeError) as error:
        return {"error": str(error), "elapsed_ms": round((time.perf_counter() - started) * 1000, 3)}
    elapsed = round((time.perf_counter() - started) * 1000, 3)
    if process.returncode:
        return {"error": f"Exit {process.returncode}: {process.stderr.strip()[:4096]}", "elapsed_ms": elapsed}
    try:
        digest, count = result_signature(process.stdout, ordered=has_top_level_order_by(sql))
    except (json.JSONDecodeError, ValueError) as error:
        return {"error": f"Invalid JSON result: {error}", "elapsed_ms": elapsed}
    return {"elapsed_ms": elapsed, "sha256": digest, "result_rows": count}


def percentile(samples: list[float], fraction: float) -> float:
    return sorted(samples)[max(0, math.ceil(len(samples) * fraction) - 1)]


def measure_query(
    binary: str, parquet: Path, number: int, sql: str, repeat: int, warmup: int, timeout: float,
    engine_args: tuple[str, ...] = (),
) -> dict:
    times = []
    signature = None
    result_rows = None
    for run in range(repeat + warmup):
        result = execute(binary, parquet, sql, timeout, engine_args)
        if "error" in result:
            return {
                "id": number, "sql": sql, "status": "error",
                "error": result["error"], "samples_ms": times,
            }
        if signature is not None and result["sha256"] != signature:
            return {
                "id": number, "sql": sql, "status": "nondeterministic",
                "error": "Result changed between runs", "samples_ms": times,
            }
        signature = result["sha256"]
        result_rows = result["result_rows"]
        if run >= warmup:
            times.append(result["elapsed_ms"])
    return {
        "id": number, "sql": sql, "status": "executed",
        "median_ms": round(statistics.median(times), 3),
        "p95_ms": round(percentile(times, 0.95), 3),
        "samples_ms": times,
        "result_rows": result_rows,
        "result_sha256": signature,
    }


def benchmark(
    binary: str, parquet: Path, query_file: Path, repeat: int, warmup: int,
    timeout: float, only: str | None = None, engine_args: tuple[str, ...] = (),
) -> dict:
    if repeat < 1 or warmup < 0 or not (math.isfinite(timeout) and timeout > 0):
        raise ValueError("repeat and timeout must be positive; warmup cannot be negative")
    if not parquet.is_file():
        raise ValueError(f"Parquet input does not exist: {parquet}")
    queries = read_queries(query_file)
    selected = choose_queries(queries, only)
    results = [
        measure_query(binary, parquet, number, sql, repeat, warmup, timeout, engine_args)
        for number, sql in selected
    ]
    return {
        "binary": binary,
        "engine_args": list(engine_args),
        "parquet": str(parquet.resolve()),
        "query_file": str(query_file.resolve()),
        "query_file_sha256": hashlib.sha256(query_file.read_bytes()).hexdigest(),
        "description": "cold CLI wall time including startup, registration, planning, execution and JSON output",
        "repeat": repeat,
        "warmup": warmup,
        "timeout_seconds": timeout,
        "platform": platform.platform(),
        "queries_total": len(queries),
        "queries_attempted": len(results),
        "queries_executed": sum(result["status"] == "executed" for result in results),
        "results": results,
    }


def build_engine_args(args: argparse.Namespace) -> tuple[str, ...]:
    engine_args = []
    for flag, value in [
        ("--batch-size", args.batch_size),
        ("--target-partitions", args.target_partitions),
        ("--memory-limit-mb", args.memory_limit_mb),
    ]:
        if value is not None:
            if value <= 0:
                raise ValueError(f"{flag} must be positive")
            engine_args.extend([flag, str(value)])
    if args.spill_dir is not None:
        if not args.spill_dir.is_dir():
            raise ValueError("--spill-dir must name an existing directory")
        engine_args.extend(["--spill-dir", str(args.spill_dir)])
    return tuple(engine_args)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", default="./target/release/rustspark")
    parser.add_argument("--parquet", type=Path, required=True)
    parser.add_argument("--queries", type=Path, required=True)
    parser.add_argument("--only", help="comma-separated, 1-based query IDs")
    parser.add_argument("--repeat", type=int, default=3)
    parser.add_argument("--warmup", type=int, default=1)
    parser.add_argument("--timeout", type=float, default=300)
    parser.add_argument("--json-out", type=Path, required=True)
    parser.add_argument("--batch-size", type=int)
    parser.add_argument("--target-partitions", type=int)
    parser.add_argument("--memory-limit-mb", type=int)
    parser.add_argument("--spill-dir", type=Path)
    args = parser.parse_args(argv)
    try:
        engine_args = build_engine_args(args)
        report = benchmark(
            args.binary, args.parquet, args.queries, args.repeat, args.warmup,
            args.timeout, args.only, engine_args,
        )
    except (OSError, UnicodeError, ValueError) as error:
        parser.error(str(error))
    args.json_out.parent.mkdir(parents=True, exist_ok=True)
    args.json_out.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(f"{report['queries_executed']}/{report['queries_attempted']} queries executed")
    return 0 if report["queries_executed"] == report["queries_attempted"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
