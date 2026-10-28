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

    def test_normal_ratio_preserved(self):
        timing, error, slowed = compare._compare_query(2, result(10), result(12), 1.3)
        self.assertIsNone(error)
        self.assertFalse(slowed)
        self.assertFalse(timing["ratio_overflow"])
        self.assertAlmostEqual(timing["slowdown"], 1.2)


if __name__ == "__main__":
    unittest.main()
