#!/usr/bin/env python3
"""Validate the files shipped in a release archive and smoke-test its binary.

Read only expected files, so malformed archives cannot write outside the
temporary directory. The binary must match the executable tested before packaging.
"""
import argparse
import pathlib
import stat
import subprocess
import sys
import tarfile
import tempfile
import zipfile


MAX_ARCHIVE_ENTRY_BYTES = 512 * 1024 * 1024


def archive_contents(path: pathlib.Path, expected_sizes: dict[str, int] | None = None) -> dict[str, tuple[bytes, int]]:
    entries = {}
    if path.name.endswith(".tar.gz"):
        with tarfile.open(path, "r:gz") as archive:
            for member in archive.getmembers():
                if not member.isfile() or member.name in entries:
                    raise ValueError(f"Unexpected archive entry: {member.name}")
                if expected_sizes is not None and member.name not in expected_sizes:
                    raise ValueError(f"Archive members differ: unexpected {member.name}")
                limit = (expected_sizes[member.name] if expected_sizes is not None
                         else MAX_ARCHIVE_ENTRY_BYTES)
                if member.size > limit:
                    raise ValueError(f"Packaged {member.name} size differs from checked source")
                stream = archive.extractfile(member)
                if stream is None:
                    raise ValueError(f"Unreadable archive member: {member.name}")
                with stream:
                    entries[member.name] = (stream.read(limit + 1), member.mode)
    elif path.suffix.lower() == ".zip":
        with zipfile.ZipFile(path) as archive:
            for member in archive.infolist():
                mode = member.external_attr >> 16
                if (member.is_dir() or stat.S_ISLNK(mode)
                        or member.filename in entries):
                    raise ValueError(f"Unexpected archive entry: {member.filename}")
                if expected_sizes is not None and member.filename not in expected_sizes:
                    raise ValueError(f"Archive members differ: unexpected {member.filename}")
                limit = (expected_sizes[member.filename] if expected_sizes is not None
                         else MAX_ARCHIVE_ENTRY_BYTES)
                if member.file_size > limit:
                    raise ValueError(f"Packaged {member.filename} size differs from checked source")
                with archive.open(member) as stream:
                    entries[member.filename] = (stream.read(limit + 1), mode)
    else:
        raise ValueError(f"Unsupported release archive: {path}")
    return entries


def verify_archive(archive: pathlib.Path, binary: pathlib.Path,
                   repo_root: pathlib.Path, destination: pathlib.Path) -> pathlib.Path:
    expected = {
        "README.md": (repo_root / "README.md").read_bytes(),
        "LICENSE": (repo_root / "LICENSE").read_bytes(),
        binary.name: binary.read_bytes(),
    }
    entries = archive_contents(archive, {name: len(contents) for name, contents in expected.items()})
    if set(entries) != set(expected):
        raise ValueError(
            f"Archive members differ: expected {sorted(expected)}, got {sorted(entries)}"
        )
    for name, contents in expected.items():
        if entries[name][0] != contents:
            raise ValueError(f"Packaged {name} differs from the checked source")
    mode = entries[binary.name][1]
    if archive.name.endswith(".tar.gz") and not mode & 0o111:
        raise ValueError("Packaged Unix binary lost its executable permissions")
    unpacked = destination / binary.name
    unpacked.write_bytes(entries[binary.name][0])
    if archive.name.endswith(".tar.gz"):
        unpacked.chmod(mode & 0o777)
    return unpacked


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", type=pathlib.Path, required=True)
    parser.add_argument("--binary", type=pathlib.Path, required=True)
    args = parser.parse_args(argv)
    repo_root = pathlib.Path(__file__).resolve().parents[1]
    with tempfile.TemporaryDirectory(prefix="rustspark-release-") as directory:
        unpacked = verify_archive(
            args.archive, args.binary, repo_root, pathlib.Path(directory)
        )
        # Exercise the executable read from the final archive, not just the
        # original target/release binary.
        subprocess.run(
            [sys.executable, str(repo_root / "scripts" / "release_smoke.py"),
             "--binary", str(unpacked)],
            check=True, timeout=300,
        )
    print(f"Verified packaged release: {args.archive}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
