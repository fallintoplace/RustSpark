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


if __name__ == "__main__":
    unittest.main()
