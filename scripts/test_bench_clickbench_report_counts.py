"""Reject self-contradictory ClickBench coverage metrics."""
import unittest
import bench_clickbench_report as report

class ClickbenchCoverageTests(unittest.TestCase):
    def test_consistent_partial_run_is_valid(self):
        data = {"results": [{"id": 1, "status": "executed", "median_ms": 1.0, "p95_ms": 1.0}],
                "queries_attempted": 1, "queries_executed": 1, "queries_total": 43}
        self.assertIn("1/1", report.render(data))

    def test_bad_reported_counts_fail(self):
        valid = {"results": [{"id": 2, "status": "error"}], "queries_total": 43}
        for key, value in (("queries_attempted", 2), ("queries_executed", 1),
                           ("queries_total", 1), ("queries_total", True)):
            with self.subTest(key=key, value=value), self.assertRaisesRegex(ValueError, key):
                report.render({**valid, key: value})

if __name__ == "__main__":
    unittest.main()
