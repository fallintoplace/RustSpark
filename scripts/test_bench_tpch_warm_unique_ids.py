"""Warm shell timings must not mix two SQL files under the same query ID."""

from pathlib import Path
import tempfile
import unittest

import bench_tpch_warm as warm


class WarmQueryIdTests(unittest.TestCase):
    def test_rejects_duplicate_and_invalid_ids_before_shell_launch(self):
        with tempfile.TemporaryDirectory() as folder:
            sql = Path(folder) / "01.sql"
            sql.write_text("SELECT 1;", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "duplicate"):
                warm.requests_for([(1, sql), (1, sql)], 1, 2)
            for invalid in (True, 0, 23):
                with self.subTest(invalid=invalid):
                    with self.assertRaisesRegex(ValueError, "Invalid"):
                        warm.requests_for([(invalid, sql)], 1, 2)

    def test_unique_query_ids_keep_expected_attempt_counts(self):
        with tempfile.TemporaryDirectory() as folder:
            sql = Path(folder) / "01.sql"
            sql.write_text("SELECT 1;", encoding="utf-8")
            self.assertEqual(len(warm.requests_for([(1, sql)], 1, 2)), 3)


if __name__ == "__main__":
    unittest.main()
