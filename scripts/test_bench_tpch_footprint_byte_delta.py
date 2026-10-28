"""Parquet fixture comparisons should quantify their storage difference."""

from pathlib import Path
import tempfile
import unittest

import bench_tpch_footprint as footprint
from test_bench_tpch_footprint import make_fixture


class FootprintByteDeltaTests(unittest.TestCase):
    def test_reports_signed_byte_delta_and_percentage(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            make_fixture(root / "first", content=b"PAR1")
            make_fixture(root / "second", content=b"PAR1modified")
            first = footprint.profile(root / "first")
            second = footprint.profile(root / "second")
            increased = footprint.compare(first, second)
            self.assertEqual(increased["physical_bytes_delta"], 8 * 8)
            self.assertEqual(increased["physical_bytes_change_percent"], 200.0)
            decreased = footprint.compare(second, first)
            self.assertEqual(decreased["physical_bytes_delta"], -(8 * 8))
            self.assertEqual(decreased["physical_bytes_change_percent"], -66.667)


if __name__ == "__main__":
    unittest.main()
