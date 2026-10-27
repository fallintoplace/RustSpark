#!/usr/bin/env python3
"""Append reproducible host and binary metadata to a benchmark JSON report."""

import argparse
import hashlib
import json
from pathlib import Path
import platform
import subprocess


def sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def git_revision(directory):
    try:
        process = subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=directory, capture_output=True,
            text=True, check=False, timeout=10,
        )
        if process.returncode == 0 and len(process.stdout.strip()) == 40:
            return process.stdout.strip()
    except (OSError, subprocess.TimeoutExpired):
        pass
    return None


def collect_provenance(executable, repository):
    return {
        "binary_sha256": sha256(executable),
        "binary_bytes": executable.stat().st_size,
        "platform": platform.platform(),
        "machine": platform.machine(),
        "processor": platform.processor(),
        "python": platform.python_version(),
        "git_revision": git_revision(repository),
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
        if args.output.exists():
            raise ValueError("Output already exists")
        report = enrich(json.loads(args.report.read_text(encoding="utf-8")),
                        args.binary, args.repo)
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    except (OSError, ValueError) as error:
        parser.exit(1, f"provenance failed: {error}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
