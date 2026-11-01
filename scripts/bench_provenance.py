#!/usr/bin/env python3
"""Append reproducible host and binary metadata to a benchmark JSON report."""

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import re
import subprocess


def load_report(path):
    """Reject ambiguous keys and nonfinite measurements before attaching provenance."""
    def unique_keys(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f"Duplicate report key: {key}")
            result[key] = value
        return result

    def finite_float(raw):
        value = float(raw)
        if not math.isfinite(value):
            raise ValueError("Nonfinite report number")
        return value

    def reject_constant(value):
        raise ValueError(f"Nonfinite report constant: {value}")

    return json.loads(
        Path(path).read_text(encoding="utf-8"),
        object_pairs_hook=unique_keys,
        parse_float=finite_float,
        parse_constant=reject_constant,
    )


def sha256_and_size(path):
    """Hash and count the bytes from the same binary file handle."""
    digest = hashlib.sha256()
    count = 0
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
            count += len(chunk)
    return digest.hexdigest(), count


def sha256(path):
    return sha256_and_size(path)[0]


def git_revision(directory):
    try:
        process = subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=directory, capture_output=True,
            text=True, check=False, timeout=10,
        )
        revision = process.stdout.strip()
        if process.returncode == 0 and re.fullmatch(r"[0-9a-fA-F]{40}", revision):
            return revision.lower()
    except (OSError, subprocess.TimeoutExpired):
        pass
    return None


def git_dirty(directory):
    """Report uncommitted changes, or None when Git state is unavailable."""
    try:
        process = subprocess.run(
            ["git", "status", "--porcelain=v1"], cwd=directory,
            capture_output=True, text=True, check=False, timeout=10,
        )
        if process.returncode == 0:
            return bool(process.stdout.strip())
    except (OSError, subprocess.TimeoutExpired):
        pass
    return None


def available_cpu_count():
    try:
        return len(os.sched_getaffinity(0))
    except (AttributeError, OSError):
        return os.cpu_count()


def host_metadata():
    """Collect platform information independently of the benchmark executable."""
    return {
        "platform": platform.platform(),
        "machine": platform.machine(),
        "processor": platform.processor(),
        "python": platform.python_version(),
        "logical_cpus": os.cpu_count(),
        "available_cpus": available_cpu_count(),
    }


def collect_provenance(executable, repository):
    digest, size = sha256_and_size(executable)
    return {
        "binary_sha256": digest,
        "binary_bytes": size,
        **host_metadata(),
        "git_revision": git_revision(repository),
        "git_dirty": git_dirty(repository),
    }


def enrich(report, binary, repository="."):
    if not isinstance(report, dict):
        raise ValueError("Input report must be a JSON object")
    executable = Path(binary)
    if not executable.is_file():
        raise ValueError(f"Benchmark executable is missing: {binary}")
    copy = dict(report)
    if "provenance" in copy:
        raise ValueError("Report already contains provenance")
    copy["provenance"] = collect_provenance(executable, repository)
    return copy


def write_new_report(path: Path, report: dict) -> None:
    """Create a report only if its destination does not already exist."""
    path.parent.mkdir(parents=True, exist_ok=True)
    # Windows exclusive open may follow a dangling symlink. Reject it before
    # opening the destination as well as relying on exclusive creation.
    if path.is_symlink():
        raise FileExistsError(f"Report output must not be a symlink: {path}")
    with path.open("x", encoding="utf-8") as output:
        output.write(json.dumps(report, indent=2) + "\n")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("report", type=Path)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--repo", type=Path, default=Path("."))
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args(argv)
    try:
        if args.report.resolve() == args.output.resolve():
            raise ValueError("Output must differ from source report")
        report = enrich(load_report(args.report), args.binary, args.repo)
        write_new_report(args.output, report)
    except (OSError, ValueError) as error:
        parser.exit(1, f"provenance failed: {error}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
