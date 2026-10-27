"""Warm engine comparisons require full correctness and full query coverage."""

import unittest

import bench_tpch_compare_warm as compare


def report(ms, name):
    return {"metric": f"warm {name} query execution", "results": [
        {"id": number, "status": "ok", "median_ms": ms}
        for number in range(1, 23)
    ]}


def verification():
    return {"results": [{"id": number, "status": "match"}
                        for number in range(1, 23)]}


class CompareWarmTests(unittest.TestCase):
    def test_ratio_direction_and_full_coverage(self):
        summary = compare.compare(report(10, "rs"), report(20, "duckdb"), verification())
        self.assertEqual(summary["verified_queries"], 22)
        self.assertEqual(summary["rustspark_faster_queries"], 22)
        self.assertEqual(summary["geometric_mean_duckdb_over_rustspark"], 2.0)
        self.assertEqual(summary["queries"][0]["duckdb_over_rustspark"], 2.0)

    def test_wrong_results_block_speed_comparison(self):
        data = verification()
        data["results"][0]["status"] = "mismatch"
        with self.assertRaisesRegex(ValueError, "incomplete"):
            compare.compare(report(10, "rs"), report(20, "duckdb"), data)

    def test_failed_missing_and_zero_timing_rejected(self):
        rs = report(10, "rs")
        dd = report(20, "duckdb")
        for bad in (0, float("nan"), float("inf"), True):
            with self.subTest(bad=bad):
                broken = report(20, "duckdb")
                broken["results"][0]["median_ms"] = bad
                with self.assertRaises(ValueError):
                    compare.compare(rs, broken, verification())
        dd["results"].pop()
        with self.assertRaises(ValueError):
            compare.compare(rs, dd, verification())

    def test_no_cold_vs_warm_comparison(self):
        cold = report(20, "duckdb")
        cold["metric"] = "cold CLI SQL"
        with self.assertRaisesRegex(ValueError, "warm"):
            compare.compare(report(10, "rs"), cold, verification())


if __name__ == "__main__":
    unittest.main()
