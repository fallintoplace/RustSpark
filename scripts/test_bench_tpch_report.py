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

    def test_complete_report_shows_top_query_shares(self):
        rows = [{"id": n, "status": "ok", "median_ms": n, "p95_ms": n}
                for n in range(1, 23)]
        markdown = report.render({"results": rows})
        self.assertIn("## Latency concentration", markdown)
        self.assertIn("- Q22:", markdown)
        self.assertIn("- Q21:", markdown)
        self.assertIn("- Q20:", markdown)
        self.assertLess(markdown.index("Q22:"), markdown.index("Q21:"))

    def test_rejects_invalid_times_and_ids(self):
        row = {"id": 1, "status": "ok", "median_ms": -1, "p95_ms": 2}
        with self.assertRaises(ValueError):
            report.render({"results": [row]})
        with self.assertRaisesRegex(ValueError, "duplicate"):
            report.render({"results": [{"id": 1, "status": "error"}, {"id": 1, "status": "error"}]})

    def test_rejects_non_object_rows(self):
        for row in (None, "wrong", 5, []):
            with self.subTest(row=row), self.assertRaisesRegex(ValueError, "JSON objects"):
                report.render({"results": [row]})


if __name__ == "__main__":
    unittest.main()
