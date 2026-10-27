#!/usr/bin/env python3
"""Verify Ballista SQL joins and aggregation using shared deterministic CSV files.

Standalone starts an in-process Ballista scheduler. Remote mode requires a
running external scheduler and executor(s) with access to the fixture paths.
Neither mode tests worker crash recovery or production-scale throughput.
"""
import argparse
import os
import pathlib
import subprocess
import tempfile

ROOT = pathlib.Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "integrations" / "ballista" / "Cargo.toml"
QUERY = (
    "SELECT COUNT(*) AS matches, SUM(o.amount * r.multiplier) AS weighted_total "
    "FROM orders o JOIN rates r ON o.id = r.id"
)
EXPECTED_COLUMNS = ["matches", "weighted_total"]
EXPECTED_VALUES = ["2", "80"]


def command_for(mode: str, client: str | None) -> list[str]:
    if mode == "standalone":
        return [
            "cargo", "run", "--quiet", "--manifest-path", str(MANIFEST),
            "--features", "standalone", "--example", "standalone",
        ]
    if mode == "remote" and client:
        return [client]
    raise ValueError("remote mode requires --client pointing to the built Ballista binary")


def write_fixtures(directory: pathlib.Path) -> tuple[pathlib.Path, pathlib.Path]:
    orders = directory / "orders.csv"
    rates = directory / "rates.csv"
    orders.write_text("id,amount\n1,10\n2,20\n3,30\n5,50\n", encoding="utf-8")
    rates.write_text("id,multiplier\n1,2\n2,3\n4,4\n", encoding="utf-8")
    return orders, rates


def verify_output(stdout: str) -> None:
    # The aggregate returns one header and one data row. Ignore table borders
    # and ordinary log lines, but never accept a matching value elsewhere.
    table_rows = [
        [cell.strip() for cell in line.strip()[1:-1].split("|")]
        for line in stdout.splitlines()
        if line.strip().startswith("|") and line.strip().endswith("|")
    ]
    if table_rows != [EXPECTED_COLUMNS, EXPECTED_VALUES]:
        raise RuntimeError("Ballista join/aggregate expected one row (2, 80):\n" + stdout)

def run_smoke(mode: str, client: str | None = None, timeout: int = 300) -> None:
    command = command_for(mode, client)
    if timeout <= 0:
        raise ValueError("timeout must be positive")
    with tempfile.TemporaryDirectory(prefix="rustspark-ballista-smoke-") as temp:
        orders, rates = write_fixtures(pathlib.Path(temp))
        env = os.environ.copy()
        # A developer's local query settings must not change the smoke workload.
        for name in ("RUSTSPARK_PARQUET", "RUSTSPARK_MAX_ROWS",
                     "RUSTSPARK_QUERY_TIMEOUT_SECS", "RUSTSPARK_INPUTS_FILE"):
            env.pop(name, None)
        env.update({
            "RUSTSPARK_INPUTS": f"csv:orders={orders};csv:rates={rates}",
            "RUSTSPARK_SQL": QUERY,
            "RUSTSPARK_TARGET_PARTITIONS": "4",
            "RUSTSPARK_STANDALONE_PARALLELISM": "4",
        })
        result = subprocess.run(
            command, cwd=ROOT, env=env, capture_output=True, text=True,
            timeout=timeout, check=False,
        )
        if result.returncode:
            raise RuntimeError(
                f"Ballista exited with code {result.returncode}:\n{result.stderr}\n{result.stdout}"
            )
        verify_output(result.stdout)
    print(f"PASS: {mode} Ballista join and aggregation returned matches=2, total=80")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("standalone", "remote"), default="standalone")
    parser.add_argument("--client", help="Built remote client binary, required for remote mode")
    parser.add_argument("--timeout", type=int, default=300)
    args = parser.parse_args(argv)
    try:
        run_smoke(args.mode, args.client, args.timeout)
    except (ValueError, RuntimeError, subprocess.TimeoutExpired) as error:
        parser.exit(1, f"FAIL: {error}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
