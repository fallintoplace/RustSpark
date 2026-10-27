import copy
import json
import tempfile
import unittest
from pathlib import Path

import sql_result_verify


def report(*ids):
    return {
        "query_file_sha256": "a" * 64,
        "queries_attempted": len(ids),
        "results": [
            {
                "id": number,
                "sql": f"SELECT {number}",
                "status": "executed",
                "result_sha256": "b" * 64,
                "result_rows": 1,
            }
            for number in ids
        ],
    }


class SqlResultVerificationTests(unittest.TestCase):
    def test_complete_matching_reports_pass(self):
        self.assertEqual(sql_result_verify.verify_reports(report(1, 2), report(1, 2)), [])

    def test_missing_actual_query_fails_even_with_consistent_counts(self):
        issues = sql_result_verify.verify_reports(report(1), report(1, 2))
        self.assertEqual(issues, ["Query 2: missing actual result"])

    def test_missing_reference_query_fails(self):
        issues = sql_result_verify.verify_reports(report(1, 2), report(1))
        self.assertEqual(issues, ["Query 2: missing reference result"])

    def test_multiple_missing_query_ids_have_stable_order(self):
        issues = sql_result_verify.verify_reports(report(3), report(1, 2, 3))
        self.assertEqual(issues, [
            "Query 1: missing actual result",
            "Query 2: missing actual result",
        ])

    def test_mismatched_result_hash_fails(self):
        actual = report(1)
        actual["results"][0]["result_sha256"] = "c" * 64
        self.assertEqual(
            sql_result_verify.verify_reports(actual, report(1)),
            ["Query 1: result hash differs"],
        )

    def test_different_query_sql_fails(self):
        actual = report(1)
        actual["results"][0]["sql"] = "SELECT 99"
        self.assertEqual(
            sql_result_verify.verify_reports(actual, report(1)),
            ["Query 1: SQL differs from reference"],
        )

    def test_duplicate_ids_rejected(self):
        actual = report(1, 1)
        with self.assertRaisesRegex(ValueError, "duplicate query ID"):
            sql_result_verify.verify_reports(actual, report(1))

    def test_count_cannot_hide_missing_query(self):
        actual = report(1)
        actual["queries_attempted"] = 2
        with self.assertRaisesRegex(ValueError, "queries_attempted"):
            sql_result_verify.verify_reports(actual, report(1, 2))

    def test_load_report_rejects_duplicate_json_keys(self):
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / "report.json"
            path.write_text('{"results": [], "results": []}', encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "Duplicate JSON key"):
                sql_result_verify.load_report(path)


if __name__ == "__main__":
    unittest.main()
