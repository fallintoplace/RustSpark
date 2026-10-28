"""Malformed report JSON must not crash the TPC-H smoke result check."""

import json
from pathlib import Path
import tempfile
import unittest

import bench_tpch_smoke as smoke


class SmokeReportShapeTests(unittest.TestCase):
    def test_non_object_reports_fail_without_attribute_error(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "report.json"
            for payload in ([], None, 1, "invalid"):
                with self.subTest(payload=payload):
                    path.write_text(json.dumps(payload), encoding="utf-8")
                    self.assertFalse(smoke.smoke_report_succeeded(path, 0))


if __name__ == "__main__":
    unittest.main()
