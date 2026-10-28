"""Malformed row-count manifests must fail before launching SQL."""

import json
from pathlib import Path
import tempfile
import unittest

import bench_tpch_rowcounts as counts


class RowCountManifestShapeTests(unittest.TestCase):
    def test_non_object_manifest_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = Path(directory)
            manifest = fixture / "manifest.json"
            for value in (None, [], 5):
                with self.subTest(value=value):
                    manifest.write_text(json.dumps(value), encoding="utf-8")
                    with self.assertRaisesRegex(ValueError, "JSON object"):
                        counts.check("rs", fixture)


if __name__ == "__main__":
    unittest.main()
