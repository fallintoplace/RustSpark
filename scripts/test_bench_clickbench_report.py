"""ClickBench report should expose query failures."""

import unittest

import bench_clickbench_report as reporter


class ReportTests(unittest.TestCase):
    def test_status_and_failure_render(self):
        result = reporter.render({"results": [
            {"id": 1, "status": "executed", "median_ms": 1.25, "p95_ms": 2.25},
            {"id": 2, "status": "error", "error": "unsupported function"},
        ]})
        self.assertIn("1/2", result)
        self.assertIn("Q02: unsupported function", result)

    def test_bad_timings_rejected(self):
        with self.assertRaisesRegex(ValueError, "Invalid"):
            reporter.render({"results": [
                {"id": 1, "status": "executed", "median_ms": -1, "p95_ms": 0},
            ]})

    def test_duplicate_ids_rejected(self):
        with self.assertRaisesRegex(ValueError, "duplicated"):
            reporter.render({"results": [
                {"id": 1, "status": "error"}, {"id": 1, "status": "error"},
            ]})


if __name__ == "__main__":
    unittest.main()
