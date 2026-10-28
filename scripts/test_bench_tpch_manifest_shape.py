"""Reject malformed top-level TPC-H fixture JSON with a useful error."""

import json
from pathlib import Path
import tempfile
import unittest

import bench_tpch_manifest as manifest


class ManifestShapeTests(unittest.TestCase):
    def test_duplicate_manifest_keys_fail_before_audit(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "manifest.json"
            path.write_text('{"rows": {}, "rows": {}}', encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "Duplicate"):
                manifest.check(folder)

    def test_non_object_top_level_manifest(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "manifest.json"
            for value in (None, [], 3, "not a manifest"):
                with self.subTest(value=value):
                    path.write_text(json.dumps(value), encoding="utf-8")
                    with self.assertRaisesRegex(ValueError, "JSON object"):
                        manifest.check(folder)


if __name__ == "__main__":
    unittest.main()
