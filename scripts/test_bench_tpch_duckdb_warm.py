"""Unit-test TPC-H DuckDB timing without the optional duckdb package."""

from pathlib import Path
import tempfile
import unittest
from unittest import mock

import bench_tpch_duckdb_warm as warm


class FakeConnection:
    description = [("answer",)]
    def __init__(self):
        self.calls = 0
    def execute(self, sql):
        self.calls += 1
        return self
    def fetchall(self):
        return [(42,)]


class DuckdbWarmTests(unittest.TestCase):
    def test_warmup_is_not_measured_and_answers_are_stable(self):
        con = FakeConnection()
        result = warm.measure_query(con, "SELECT 42 AS answer", 2, 3)
        self.assertEqual(con.calls, 5)
        self.assertEqual(len(result["samples_ms"]), 3)
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["result_rows"], 1)

    def test_rejects_duplicates_and_nondeterminism(self):
        con = FakeConnection()
        con.description = [("answer",), ("answer",)]
        with self.assertRaisesRegex(ValueError, "Duplicate"):
            warm.measure_query(con, "SELECT 1", 0, 1)
        con = FakeConnection()
        def alternate():
            con.calls += 1
            return [(con.calls,)]
        con.fetchall = alternate
        with self.assertRaisesRegex(ValueError, "Nondeterministic"):
            warm.measure_query(con, "SELECT 1", 0, 2)

    def test_report_uses_explicit_thread_count(self):
        with tempfile.TemporaryDirectory() as tmp:
            sql = Path(tmp) / "01.sql"
            sql.write_text("SELECT 42 AS answer;")
            con = FakeConnection()
            con.close = mock.Mock()
            with mock.patch.object(warm, "reference_connection", return_value=con):
                report = warm.benchmark(Path(tmp), [(1, sql)], warmup=0, repeat=1, threads=4)
            self.assertEqual(report["threads"], 4)
            self.assertEqual(report["results"][0]["status"], "ok")
            con.close.assert_called_once()
            with self.assertRaises(ValueError):
                warm.benchmark(Path(tmp), [(1, sql)], threads=0)

    def test_duckdb_query_error_does_not_skip_later_queries(self):
        import sys
        import types

        class FakeDatabaseError(Exception):
            pass

        class Connection(FakeConnection):
            def execute(self, sql):
                if sql == "SELECT missing":
                    raise FakeDatabaseError("missing table")
                return super().execute(sql)
            def close(self):
                pass

        with tempfile.TemporaryDirectory() as tmp:
            first = Path(tmp) / "01.sql"
            second = Path(tmp) / "02.sql"
            first.write_text("SELECT missing", encoding="utf-8")
            second.write_text("SELECT 42 AS answer", encoding="utf-8")
            fake_duckdb = types.SimpleNamespace(Error=FakeDatabaseError)
            with mock.patch.dict(sys.modules, {"duckdb": fake_duckdb}):
                with mock.patch.object(warm, "reference_connection", return_value=Connection()):
                    result = warm.benchmark(Path(tmp), [(1, first), (2, second)],
                                            warmup=0, repeat=1)
            self.assertEqual([row["status"] for row in result["results"]],
                             ["error", "ok"])
            self.assertIn("missing table", result["results"][0]["error"])


if __name__ == "__main__":
    unittest.main()
