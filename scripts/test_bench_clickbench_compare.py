"""Comparison must reject result drift and missing queries."""

import unittest

import bench_clickbench_compare as comparison


def report(rows):
    return {"results": [{"id": n, "status": "executed",
                         "median_ms": ms, "result_sha256": digest * 64}
                        for n, ms, digest in rows]}


class ComparisonTests(unittest.TestCase):
    def test_regression_and_mismatch(self):
        old = report([(1, 100, "a"), (2, 100, "b")])
        new = report([(1, 130, "a"), (2, 90, "c")])
        diff = comparison.compare(old, new)
        self.assertEqual(diff["slowdowns"], [1])
        self.assertEqual(diff["result_mismatches"], [2])

    def test_equal_reports(self):
        value = report([(1, 10, "a")])
        diff = comparison.compare(value, value)
        self.assertFalse(diff["slowdowns"])
        self.assertFalse(diff["result_mismatches"])

    def test_different_query_ids(self):
        with self.assertRaisesRegex(ValueError, "differ"):
            comparison.compare(report([(1, 5, "a")]), report([(2, 5, "a")]))

    def test_rejects_overflowed_latency_ratio(self):
        baseline = report([(1, 1e-308, "a")])
        candidate = report([(1, 1e308, "a")])
        with self.assertRaisesRegex(ValueError, "latency ratio is not finite"):
            comparison.compare(baseline, candidate)


if __name__ == "__main__":
    unittest.main()
