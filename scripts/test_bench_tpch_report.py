"""Coverage and latency reporting."""

import unittest

import bench_tpch_report as report


class ReportTests(unittest.TestCase):
    def test_shows_failed_query(self):
        result = report.render({"results": [
            {"id": 1, "status": "ok", "median_ms": 1.0, "p95_ms": 2.0},
            {"id": 2, "status": "error", "error": "SQL not supported"},
        ]})
        self.assertIn("1/2 queries successful", result)
        self.assertIn("Q02: SQL not supported", result)
        self.assertIn("no", result)

    def test_full_success_is_reported(self):
        items = [{"id": i, "status": "ok", "median_ms": 1.0, "p95_ms": 1.0} for i in range(1, 23)]
        self.assertIn("Complete 22-query run:** yes", report.render({"results": items}))

    def test_rejects_invalid_times_and_ids(self):
        row = {"id": 1, "status": "ok", "median_ms": -1, "p95_ms": 2}
        with self.assertRaises(ValueError):
            report.render({"results": [row]})
        with self.assertRaisesRegex(ValueError, "duplicate"):
            report.render({"results": [{"id": 1, "status": "error"}, {"id": 1, "status": "error"}]})


if __name__ == "__main__":
    unittest.main()
