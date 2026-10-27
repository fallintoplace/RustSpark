#!/usr/bin/env python3
"""Single entry point for Python test suites and SQL benchmark tools (stdlib only)."""

import argparse
import fnmatch
from pathlib import Path
import subprocess
import sys
import unittest


SCRIPTS = Path(__file__).resolve().parent
TEST_PATTERNS = {
    "bench": (
        "test_bench_*.py", "test_clickbench_*.py", "test_tpch_*.py",
        "test_quality.py", "test_ballista_smoke.py", "test_sql_result_verify.py",
    ),
    "compat": (
        "test_spark_*.py", "test_release_*.py", "test_verify_release_*.py",
    ),
}
BENCHMARK_TOOLS = {
    "local": "bench_rustspark.py",
    "cold": "bench_compare.py",
    "warm": "bench_warm_compare.py",
    "tpch": "tpch_run.py",
    "tpch-prepare": "tpch_prepare.py",
    "clickbench": "clickbench_run.py",
    "clickbench-fetch": "clickbench_fetch.py",
    "report": "bench_report.py",
    "compare-reports": "sql_report_compare.py",
}


def test_manifest():
    """Fail closed if a new unittest module is missing from CI or assigned twice."""
    candidates = {path.name for path in SCRIPTS.glob("test_*.py")}
    assigned = {}
    for suite, patterns in TEST_PATTERNS.items():
        for name in candidates:
            if any(fnmatch.fnmatchcase(name, pattern) for pattern in patterns):
                assigned.setdefault(name, []).append(suite)
    missing = sorted(candidates - assigned.keys())
    duplicates = sorted(name for name, owners in assigned.items() if len(owners) != 1)
    if missing or duplicates:
        raise ValueError(f"Invalid Python test manifest; missing={missing}, duplicate={duplicates}")
    return {suite: sorted(name for name, owners in assigned.items() if suite in owners)
            for suite in TEST_PATTERNS}


def run_tests(suite, *, list_only=False):
    selected = test_manifest()
    filenames = sorted({name for group, names in selected.items()
                        if suite in ("all", group) for name in names})
    if not filenames:
        raise ValueError(f"No Python tests selected for {suite}")
    if list_only:
        print("\n".join(filenames))
        return 0
    loader = unittest.TestLoader()
    tests = unittest.TestSuite()
    for filename in filenames:
        tests.addTests(loader.discover(str(SCRIPTS), pattern=filename))
    if not tests.countTestCases():
        raise ValueError(f"No unittest cases discovered for {suite}")
    return 0 if unittest.TextTestRunner(verbosity=2).run(tests).wasSuccessful() else 1


def run_benchmark(tool, arguments):
    """Pass tool flags unchanged; preserve its own input validation and exit code."""
    return subprocess.call([sys.executable, str(SCRIPTS / BENCHMARK_TOOLS[tool]), *arguments])


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    tests = commands.add_parser("test", help="Run centrally registered Python unit tests")
    tests.add_argument("--suite", choices=("all", *TEST_PATTERNS), default="all")
    tests.add_argument("--list", action="store_true", help="List selected test modules")
    benchmarks = commands.add_parser("bench", help="Launch an existing benchmark or reporter")
    benchmarks.add_argument("tool", choices=tuple(BENCHMARK_TOOLS))
    args, passthrough = parser.parse_known_args(argv)
    if args.command == "test":
        if passthrough:
            parser.error(f"unrecognized test arguments: {' '.join(passthrough)}")
        try:
            return run_tests(args.suite, list_only=args.list)
        except ValueError as error:
            parser.error(str(error))
    if passthrough[:1] == ["--"]:
        passthrough = passthrough[1:]
    return run_benchmark(args.tool, passthrough)


if __name__ == "__main__":
    raise SystemExit(main())
