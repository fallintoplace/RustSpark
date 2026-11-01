"""Extreme finite latency inputs must not generate Infinity JSON ratios."""
import json
import unittest
import sql_report_compare as compare


def result(ms):
    return {"sql": "SELECT 1", "status": "executed",
            "result_sha256": "a" * 64, "result_rows": 1, "median_ms": ms}


class SqlRatioOverflowTests(unittest.TestCase):
    def test_overflow_is_reported_and_can_be_flagged_as_regression(self):
        timing, error, slowed = compare._compare_query(
            1, result(5e-324), result(1e308), 1.2,
        )
        self.assertIsNone(error)
        self.assertTrue(slowed)
        self.assertTrue(timing["ratio_overflow"])
        self.assertIsNone(timing["slowdown"])
        json.dumps(timing, allow_nan=False)

    def test_huge_integer_duration_is_rejected(self):
        self.assertFalse(compare.is_duration(10**500))
        self.assertFalse(compare.is_duration(-10**500))
        self.assertTrue(compare.is_duration(0))
        self.assertTrue(compare.is_duration(1e308))

    def test_normal_ratio_preserved(self):
        timing, error, slowed = compare._compare_query(2, result(10), result(12), 1.3)
        self.assertIsNone(error)
        self.assertFalse(slowed)
        self.assertFalse(timing["ratio_overflow"])
        self.assertAlmostEqual(timing["slowdown"], 1.2)



class TotalMetadataTests(unittest.TestCase):
    def test_rejects_total_smaller_than_attempted(self):
        report = {"queries_attempted": 1, "queries_total": 0, "results": [{"id": 1}]}
        with self.assertRaisesRegex(ValueError, "queries_total"):
            compare.index_results(report, "Candidate")

    def test_rejects_ids_outside_declared_total(self):
        report = {"queries_attempted": 1, "queries_total": 1, "results": [{"id": 2}]}
        with self.assertRaisesRegex(ValueError, "queries_total"):
            compare.index_results(report, "Candidate")

    def test_different_declared_totals_cannot_compare(self):
        entry = result(1)
        entry["id"] = 1
        base = {"query_file_sha256": "a" * 64, "queries_attempted": 1,
                "queries_total": 2, "results": [entry]}
        candidate = {**base, "queries_total": 3}
        with self.assertRaisesRegex(ValueError, "different queries_total"):
            compare.compare_reports(base, candidate)

if __name__ == "__main__":
    unittest.main()
