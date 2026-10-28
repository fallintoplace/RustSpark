"""Distinct finite scale factors must never reuse benchmark artifacts."""

from pathlib import Path
import unittest

import bench_tpch_scale as scale


class ScaleArtifactNameTests(unittest.TestCase):
    def test_adjacent_scale_values_get_different_paths(self):
        first, second = scale.scales("0.1,0.10000000000000002")
        self.assertNotEqual(first, second)
        fixture_a, report_a, _, _ = scale.commands(first, "rs", Path("results"))
        fixture_b, report_b, _, _ = scale.commands(second, "rs", Path("results"))
        self.assertNotEqual(fixture_a, fixture_b)
        self.assertNotEqual(report_a, report_b)
        self.assertEqual(fixture_a.name, "sf-0.1")


if __name__ == "__main__":
    unittest.main()
