#!/usr/bin/env python3
"""Reject DataFusion or Ballista in either native Rust package's dependency graph."""

from __future__ import annotations

import json
from pathlib import Path
import re
import subprocess
import sys

ROOT = Path(__file__).resolve().parent.parent
MANIFESTS = ("Cargo.toml", "native-engine/Cargo.toml")
FORBIDDEN = re.compile(r"^(?:datafusion|ballista)(?:-|$)", re.IGNORECASE)


def check_manifest(manifest: str) -> set[str]:
    command = [
        "cargo",
        "metadata",
        "--format-version",
        "1",
        "--manifest-path",
        str(ROOT / manifest),
    ]
    output = subprocess.check_output(command, text=True, cwd=ROOT)
    metadata = json.loads(output)
    return {package["name"] for package in metadata["packages"]
            if FORBIDDEN.search(package["name"])}


def main() -> int:
    offenders = {manifest: sorted(check_manifest(manifest))
                 for manifest in MANIFESTS}
    offenders = {manifest: names for manifest, names in offenders.items() if names}
    if offenders:
        for manifest, names in offenders.items():
            print(f"Forbidden engine dependencies in {manifest}: {', '.join(names)}",
                  file=sys.stderr)
        return 1
    print("Native dependency gate passed: no DataFusion or Ballista crates resolved.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
