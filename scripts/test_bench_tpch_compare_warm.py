"""Warm engine comparisons require full correctness and full query coverage."""

import unittest

import bench_tpch_compare_warm as compare


def report(ms, name):
    return {"metric": f"warm {name} query execution", "results": [
        {"id": number, "status": "ok", "median_ms": ms,
         "sql_sha256": "a" * 64}
        for number in range(1, 23)
    ]}


def verification():
    return {"results": [{"id": number, "status": "match",
                         "sql_sha256": "a" * 64} for number in range(1, 23)]}


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

    def test_rejects_changed_sql(self):
        rustspark = report(10, "rs")
        duckdb = report(20, "duckdb")
        duckdb["results"][0]["sql_sha256"] = "b" * 64
        with self.assertRaisesRegex(ValueError, "different SQL"):
            compare.compare(rustspark, duckdb, verification())

    def test_rejects_malformed_hash_even_when_all_reports_agree(self):
        rustspark = report(10, "rs")
        duckdb = report(20, "duckdb")
        reference = verification()
        for data in (rustspark, duckdb, reference):
            data["results"][0]["sql_sha256"] = "g" * 64
        with self.assertRaisesRegex(ValueError, "SHA-256"):
            compare.compare(rustspark, duckdb, reference)

    def test_no_cold_vs_warm_comparison(self):
        cold = report(20, "duckdb")
        cold["metric"] = "cold CLI SQL"
        with self.assertRaisesRegex(ValueError, "warm"):
            compare.compare(report(10, "rs"), cold, verification())

    def test_verification_must_be_for_the_same_sql_files(self):
        reference = verification()
        reference["results"][0]["sql_sha256"] = "b" * 64
        with self.assertRaisesRegex(ValueError, "Reference verification uses different SQL"):
            compare.compare(report(10, "rs"), report(20, "duckdb"), reference)

    def test_unfingerprinted_reference_report_cannot_authorize_comparison(self):
        reference = verification()
        del reference["results"][0]["sql_sha256"]
        with self.assertRaisesRegex(ValueError, "Reference verification uses different SQL"):
            compare.compare(report(10, "rs"), report(20, "duckdb"), reference)


if __name__ == "__main__":
    unittest.main()
