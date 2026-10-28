"""Malformed summary reports should get actionable validation errors."""

import unittest

import bench_tpch_summary as summary


class SummaryInputShapeTests(unittest.TestCase):
    def test_rejects_non_object_reports(self):
        for report in (None, [], 42, "not a report"):
            with self.subTest(report=report):
                with self.assertRaisesRegex(ValueError, "JSON object"):
                    summary.summarize(report)


if __name__ == "__main__":
    unittest.main()
