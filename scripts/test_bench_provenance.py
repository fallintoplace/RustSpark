"""Reproducible binary fingerprints."""

from pathlib import Path
import tempfile
import unittest
from unittest import mock

import bench_provenance as provenance


class ProvenanceTests(unittest.TestCase):
    def test_sha256_is_deterministic(self):
        with tempfile.TemporaryDirectory() as tmp:
            binary = Path(tmp) / "rs"
            binary.write_bytes(b"hello world")
            self.assertEqual(provenance.sha256(binary), provenance.sha256(binary))
            binary.write_bytes(b"changed")
            self.assertNotEqual(provenance.sha256(binary), provenance.sha256.__globals__["hashlib"].sha256(b"hello world").hexdigest())

    def test_hash_and_size_are_measured_from_same_read(self):
        with tempfile.TemporaryDirectory() as tmp:
            binary = Path(tmp) / "rs"
            binary.write_bytes(b"binary" * 1024)
            digest, size = provenance.sha256_and_size(binary)
            self.assertEqual(size, len(b"binary" * 1024))
            self.assertEqual(digest, provenance.hashlib.sha256(b"binary" * 1024).hexdigest())

    def test_enrich_does_not_mutate_source(self):
        with tempfile.TemporaryDirectory() as tmp:
            binary = Path(tmp) / "rs"
            binary.write_bytes(b"binary")
            report = {"results": []}
            with mock.patch.object(provenance, "git_revision", return_value=None):
                value = provenance.enrich(report, binary)
            self.assertNotIn("provenance", report)
            self.assertEqual(value["provenance"]["binary_bytes"], 6)
            self.assertEqual(len(value["provenance"]["binary_sha256"]), 64)

    def test_rejects_existing_provenance(self):
        with tempfile.TemporaryDirectory() as tmp:
            binary = Path(tmp) / "rs"
            binary.write_bytes(b"x")
            with self.assertRaisesRegex(ValueError, "already"):
                provenance.enrich({"provenance": {}}, binary)


    def test_git_revision_rejects_fake_sha(self):
        with mock.patch.object(
            provenance.subprocess, "run",
            return_value=mock.Mock(returncode=0, stdout="z" * 40),
        ):
            self.assertIsNone(provenance.git_revision("."))
        with mock.patch.object(
            provenance.subprocess, "run",
            return_value=mock.Mock(returncode=0, stdout="a" * 40 + "\n"),
        ):
            self.assertEqual(provenance.git_revision("."), "a" * 40)
        with mock.patch.object(
            provenance.subprocess, "run",
            return_value=mock.Mock(returncode=0, stdout="A" * 40),
        ):
            self.assertEqual(provenance.git_revision("."), "a" * 40)


if __name__ == "__main__":
    unittest.main()
