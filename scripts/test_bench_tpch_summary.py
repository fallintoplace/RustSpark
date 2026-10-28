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

    def test_missing_or_failed_queries_are_rejected(self):
        data = report()
        data["results"].pop()
        with self.assertRaises(ValueError):
            suite.summarize(data)
        data = report()
        data["results"][3]["status"] = "error"
        with self.assertRaises(ValueError):
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


if __name__ == "__main__":
    unittest.main()
