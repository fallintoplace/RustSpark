"""Reject inconsistent SQL query total metadata during verification."""
import unittest
import sql_result_verify as verify


def sample(total):
    return {"query_file_sha256": "a" * 64, "queries_attempted": 1,
            "queries_total": total,
            "results": [{"id": 2, "sql": "SELECT 2", "status": "executed",
                         "result_rows": 1, "result_sha256": "b" * 64}]}


class TotalCoverageTests(unittest.TestCase):
    def test_total_less_than_attempted_fails(self):
        with self.assertRaisesRegex(ValueError, "queries_total"):
            verify.index_queries(sample(0), "Actual")

    def test_query_id_exceeding_total_fails(self):
        with self.assertRaisesRegex(ValueError, "exceeds"):
            verify.index_queries(sample(1), "Actual")

    def test_different_declared_totals_fail(self):
        with self.assertRaisesRegex(ValueError, "totals differ"):
            verify.verify_reports(sample(4), sample(3))

    def test_matching_totals_pass(self):
        self.assertEqual(verify.verify_reports(sample(3), sample(3)), [])


if __name__ == "__main__":
    unittest.main()
