"""Compare matched TPC-H query medians."""

import unittest

import bench_tpch_regress as regress


def report(*items):
    return {"results": [{"id": i, "status": "ok", "median_ms": t} for i, t in items]}


class RegressionTests(unittest.TestCase):
    def test_detects_regression(self):
        result = regress.compare(report((1, 100), (2, 30)), report((1, 130), (2, 10)))
        self.assertEqual(result["regressions"], [1])

    def test_equal_or_faster_is_not_regression(self):
        self.assertFalse(regress.compare(report((1, 10)), report((1, 10)))["regressions"])

    def test_rejects_mismatched_query_sets(self):
        with self.assertRaisesRegex(ValueError, "coverage"):
            regress.compare(report((1, 10)), report((2, 10)))

    def test_rejects_failed_and_invalid_samples(self):
        with self.assertRaises(ValueError):
            regress.medians({"results": [{"id": 1, "status": "error"}]})
        with self.assertRaises(ValueError):
            regress.medians(report((1, float("inf"))))


if __name__ == "__main__":
    unittest.main()
