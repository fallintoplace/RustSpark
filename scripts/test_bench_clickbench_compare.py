"""Comparison must reject result drift and missing queries."""

import unittest

import bench_clickbench_compare as comparison


def report(rows):
    return {"results": [{"id": n, "status": "executed",
                         "median_ms": ms, "result_sha256": digest * 64}
                        for n, ms, digest in rows]}


class ComparisonTests(unittest.TestCase):
    def test_rejects_non_object_results_and_reports(self):
        valid = report([(1, 10, "a")])
        for invalid in [None, [], {"results": ["not an object"]}]:
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                comparison.compare(valid, invalid)

    def test_rejects_integer_overflow_without_crashing(self):
        valid = report([(1, 10, "a")])
        oversized = report([(1, 10 ** 500, "a")])
        with self.assertRaisesRegex(ValueError, "median"):
            comparison.compare(valid, oversized)
        with self.assertRaisesRegex(ValueError, "Threshold"):
            comparison.compare(valid, valid, 10 ** 500)

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

    def test_small_finite_ratio_remains_positive(self):
        baseline = report([(1, 1e9, "a")])
        candidate = report([(1, 1.0, "a")])
        comparison_result = comparison.compare(baseline, candidate)
        self.assertEqual(comparison_result["results"][0]["median_ratio"], 1e-9)

    def test_underflowed_ratio_is_rejected_instead_of_reporting_zero(self):
        baseline = report([(1, 1e308, "a")])
        candidate = report([(1, 5e-324, "a")])
        with self.assertRaisesRegex(ValueError, "underflowed"):
            comparison.compare(baseline, candidate)

    def test_rejects_overflowed_latency_ratio(self):
        baseline = report([(1, 1e-308, "a")])
        candidate = report([(1, 1e308, "a")])
        with self.assertRaisesRegex(ValueError, "latency ratio is not finite"):
            comparison.compare(baseline, candidate)


if __name__ == "__main__":
    unittest.main()
