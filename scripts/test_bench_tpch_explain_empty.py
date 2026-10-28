"""An empty EXPLAIN collection must never report benchmark success."""

from pathlib import Path
import tempfile
import unittest

import bench_tpch_explain as explain


class ExplainEmptyRunTests(unittest.TestCase):
    def test_rejects_empty_queries_without_creating_output(self):
        with tempfile.TemporaryDirectory() as folder:
            output = Path(folder) / "plans"
            with self.assertRaisesRegex(ValueError, "At least one"):
                explain.capture("rs", Path(folder), [], output)
            self.assertFalse(output.exists())


if __name__ == "__main__":
    unittest.main()
