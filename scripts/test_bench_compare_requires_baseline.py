"""A RustSpark comparison must contain the RustSpark baseline engine."""

import unittest
from unittest import mock

import bench_compare as compare


class RustSparkBaselineTests(unittest.TestCase):
    def test_rejects_competitor_only_before_generating_fixture(self):
        with mock.patch.object(compare, "write_csv") as writer, mock.patch.object(
            compare, "check_dependencies"
        ) as dependencies:
            with self.assertRaisesRegex(ValueError, "include RustSpark"):
                compare.benchmark("binary", ["duckdb"], 2, 1, 0, 30)
            writer.assert_not_called()
            dependencies.assert_not_called()


if __name__ == "__main__":
    unittest.main()
