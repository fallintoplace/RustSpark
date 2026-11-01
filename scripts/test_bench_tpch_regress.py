"""Compare matched TPC-H query medians."""

import unittest

import bench_tpch_regress as regress


def report(*items):
    return {"results": [{"id": i, "status": "ok", "median_ms": t} for i, t in items]}


class RegressionTests(unittest.TestCase):
    def test_different_environment_blocks_comparison(self):
        first = report((1, 10))
        second = report((1, 9))
        first["platform"] = "linux-a"
        second["platform"] = "linux-b"
        with self.assertRaisesRegex(ValueError, "platform"):
            regress.compare(first, second)
        second["platform"] = "linux-a"
        first["target_partitions"] = 4
        with self.assertRaisesRegex(ValueError, "target_partitions"):
            regress.compare(first, second)

    def test_matching_environment_is_accepted(self):
        first = report((1, 10))
        second = report((1, 9))
        for run in (first, second):
            run.update({"platform": "linux", "target_partitions": 4,
                        "metric": "cold", "repeat": 3, "warmup": 1})
        self.assertFalse(regress.compare(first, second)["regressions"])

    def test_detects_regression(self):
        result = regress.compare(report((1, 100), (2, 30)), report((1, 130), (2, 10)))
        self.assertEqual(result["regressions"], [1])

    def test_equal_or_faster_is_not_regression(self):
        self.assertFalse(regress.compare(report((1, 10)), report((1, 10)))["regressions"])

    def test_rejects_mismatched_query_sets(self):
        with self.assertRaisesRegex(ValueError, "coverage"):
            regress.compare(report((1, 10)), report((2, 10)))

    def test_rejects_changed_queries_or_results(self):
        old = report((1, 10))
        new = report((1, 5))
        old["results"][0]["sql_sha256"] = "a" * 64
        new["results"][0]["sql_sha256"] = "b" * 64
        with self.assertRaisesRegex(ValueError, "sql_sha256 mismatch"):
            regress.compare(old, new)
        new["results"][0]["sql_sha256"] = "a" * 64
        old["results"][0]["result_sha256"] = "c" * 64
        new["results"][0]["result_sha256"] = "d" * 64
        with self.assertRaisesRegex(ValueError, "result_sha256 mismatch"):
            regress.compare(old, new)

    def test_full_suite_reports_geometric_mean_speedup(self):
        baseline = report(*[(i, 100.0) for i in range(1, 23)])
        candidate = report(*[(i, 50.0) for i in range(1, 23)])
        result = regress.compare(baseline, candidate)
        self.assertTrue(result["full_22_query_suite"])
        self.assertEqual(result["geometric_mean_baseline_over_candidate"], 2.0)
        partial = regress.compare(report((1, 100)), report((1, 50)))
        self.assertFalse(partial["full_22_query_suite"])
        self.assertIsNone(partial["geometric_mean_baseline_over_candidate"])

    def test_huge_numeric_inputs_fail_with_validation_errors(self):
        with self.assertRaisesRegex(ValueError, "Invalid median"):
            regress.medians(report((1, 10**500)))
        with self.assertRaisesRegex(ValueError, "Regression threshold"):
            regress.compare(report((1, 1)), report((1, 2)), max_regression=10**500)

    def test_rejects_failed_and_invalid_samples(self):
        with self.assertRaises(ValueError):
            regress.medians({"results": [{"id": 1, "status": "error"}]})
        with self.assertRaises(ValueError):
            regress.medians(report((1, float("inf"))))

    def test_rejects_one_sided_fingerprint(self):
        old = report((1, 100))
        new = report((1, 50))
        old["results"][0]["sql_sha256"] = "a" * 64
        with self.assertRaisesRegex(ValueError, "sql_sha256 mismatch"):
            regress.compare(old, new)
        old["results"][0].pop("sql_sha256")
        new["results"][0]["result_sha256"] = "b" * 64
        with self.assertRaisesRegex(ValueError, "result_sha256 mismatch"):
            regress.compare(old, new)

    def test_rejects_nonfinite_ratio_even_when_samples_are_finite(self):
        for baseline, candidate in ((1e-308, 1e308), (1e308, 1e-308)):
            with self.subTest(baseline=baseline), self.assertRaisesRegex(ValueError, "ratio"):
                regress.compare(report((1, baseline)), report((1, candidate)))

    def test_rounding_never_erases_small_positive_ratios(self):
        faster = regress.compare(report((1, 1.0)), report((1, 0.000001)))
        self.assertEqual(faster["results"][0]["ratio"], 0.000001)
        slower = regress.compare(report((1, 0.000001)), report((1, 1.0)))
        self.assertEqual(slower["results"][0]["speedup"], 0.000001)

    def test_reports_absolute_latency_delta_and_speedup(self):
        result = regress.compare(report((1, 20.0)), report((1, 5.0)))
        row = result["results"][0]
        self.assertEqual(row["delta_ms"], -15.0)
        self.assertEqual(row["speedup"], 4.0)
        self.assertEqual(row["ratio"], 0.25)
        slower = regress.compare(report((1, 4.0)), report((1, 8.0)))
        self.assertEqual(slower["results"][0]["delta_ms"], 4.0)
        self.assertEqual(slower["results"][0]["speedup"], 0.5)


if __name__ == "__main__":
    unittest.main()
