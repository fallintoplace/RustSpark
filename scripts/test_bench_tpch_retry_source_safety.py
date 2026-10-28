"""Do not delete the input report when retry output aliases it."""

from argparse import Namespace
import json
from pathlib import Path
import tempfile
import unittest

import bench_tpch_retry as retry


class RetrySourceSafetyTests(unittest.TestCase):
    def test_same_input_and_output_are_rejected_without_deleting_source(self):
        with tempfile.TemporaryDirectory() as folder:
            report = Path(folder) / "prior.json"
            original = json.dumps({"results": [{"id": 1, "status": "error"}]})
            report.write_text(original, encoding="utf-8")
            args = Namespace(previous=report, json_out=report)
            with self.assertRaisesRegex(ValueError, "must differ"):
                retry.execute_retry(args)
            self.assertEqual(report.read_text(encoding="utf-8"), original)


if __name__ == "__main__":
    unittest.main()
