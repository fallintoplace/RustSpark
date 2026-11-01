"""Require complete TPC-H coverage before producing suite statistics."""

import unittest

import bench_tpch_summary as suite


def report():
    return {"results": [{"id": index, "status": "ok", "median_ms": float(index)}
                        for index in range(1, 23)]}


class SuiteTests(unittest.TestCase):
    def test_summary_requires_all_22(self):
        result = suite.summarize(report())
        self.assertEqual(result["queries_executed"], 22)
        self.assertEqual(result["slowest_query"], 22)
        self.assertEqual(result["fastest_query"], 1)
        self.assertGreater(result["geometric_mean_ms"], 0)

    def test_rejects_medians_that_disagree_with_samples(self):
        data = report()
        data["results"][0]["samples_ms"] = [1.0, 1.0, 100.0]
        self.assertEqual(suite.summarize(data)["queries_executed"], 22)
        data["results"][0]["samples_ms"] = [100.0, 100.0, 100.0]
        with self.assertRaisesRegex(ValueError, "differs from samples"):
            suite.summarize(data)
        data["results"][0]["samples_ms"] = [float("nan")]
        with self.assertRaisesRegex(ValueError, "Invalid timing samples"):
            suite.summarize(data)

    def test_even_large_finite_samples_match_reported_median(self):
        entries = [{"id": 1, "status": "ok", "median_ms": 1e308,
                    "samples_ms": [1e308, 1e308]}]
        self.assertEqual(suite.checked_medians(entries), [1e308])

    def test_missing_or_failed_queries_are_rejected(self):
        data = report()
        data["results"].pop()
        with self.assertRaises(ValueError):
            suite.summarize(data)
        data = report()
        data["results"][3]["status"] = "error"
        with self.assertRaises(ValueError):
            suite.summarize(data)

    def test_huge_integer_timing_values_raise_value_error(self):
        data = report()
        data["results"][0]["median_ms"] = 10**500
        with self.assertRaisesRegex(ValueError, "Invalid median"):
            suite.summarize(data)
        data["results"][0]["median_ms"] = 1.0
        data["results"][0]["samples_ms"] = [10**500]
        with self.assertRaisesRegex(ValueError, "Invalid timing samples"):
            suite.summarize(data)

    def test_nonfinite_median_is_rejected(self):
        data = report()
        data["results"][0]["median_ms"] = float("inf")
        with self.assertRaises(ValueError):
            suite.summarize(data)

    def test_boolean_query_id_is_not_a_complete_suite(self):
        data = report()
        data["results"][0]["id"] = True
        with self.assertRaisesRegex(ValueError, "query ID"):
            suite.summarize(data)

    def test_large_finite_summaries_preserve_valid_percentages(self):
        data = report()
        for row in data["results"]:
            row["median_ms"] = 5e306
        result = suite.summarize(data)
        self.assertAlmostEqual(result["top_three_latency_percent"], round(3 / 22 * 100, 2))
        for row in data["results"]:
            row["median_ms"] = 1e308
        with self.assertRaisesRegex(ValueError, "latency sum"):
            suite.summarize(data)

    def test_hotspot_share_uses_sum_of_all_twenty_two_queries(self):
        data = suite.summarize(report())
        self.assertEqual(data["top_latency_queries"], [22, 21, 20])
        self.assertAlmostEqual(data["top_three_latency_percent"], 63 / 253 * 100, places=2)
        self.assertGreater(data["sum_median_ms"], 0)


if __name__ == "__main__":
    unittest.main()
