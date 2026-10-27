"""Validate multi-scale arguments and command construction."""

from pathlib import Path
import unittest

import bench_tpch_scale as sweep


class ScaleTests(unittest.TestCase):
    def test_scales(self):
        self.assertEqual(sweep.scales("0.01,0.1,1"), [0.01, 0.1, 1.0])
        for item in ("", "0", "-1", "nan", "inf", "1,1", "one"):
            with self.subTest(value=item), self.assertRaises(ValueError):
                sweep.scales(item)

    def test_commands(self):
        fixture, report, prepare, run = sweep.commands(0.1, "rs", Path("/tmp/out"))
        self.assertEqual(fixture.name, "sf-0.1")
        self.assertEqual(report.name, "sf-0.1.json")
        self.assertIn("tpch_prepare.py", prepare[1])
        self.assertIn("tpch_run.py", run[1])
        self.assertNotIn("--query-ids", run)


if __name__ == "__main__":
    unittest.main()
