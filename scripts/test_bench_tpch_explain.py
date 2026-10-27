"""Verify that EXPLAIN reports per-query failures without stopping."""

from pathlib import Path
import tempfile
import unittest
from unittest import mock

import bench_tpch_explain as explain
from tpch_run import TABLES


class ExplainTests(unittest.TestCase):
    def test_command_does_not_execute_explain_analyze(self):
        cmd = explain.command("rs", ["--parquet-table", "t=file"], "01.sql", 3)
        self.assertEqual(cmd[-1], "--explain")
        self.assertNotIn("--explain-analyze", cmd)

    def test_failed_and_successful_plans(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            parquet = directory / "data"
            parquet.mkdir()
            for table in TABLES:
                (parquet / f"{table}.parquet").touch()
            first = mock.Mock(returncode=1, stdout="", stderr="planning failed")
            second = mock.Mock(returncode=0, stdout="AggregateExec", stderr="")
            with mock.patch.object(explain.subprocess, "run", side_effect=[first, second]):
                result = explain.capture("rs", parquet, [(1, directory / "01.sql"), (2, directory / "02.sql")], directory / "plans")
            self.assertEqual([entry["status"] for entry in result], ["error", "ok"])
            self.assertEqual((directory / "plans/02.txt").read_text(), "AggregateExec")


if __name__ == "__main__":
    unittest.main()
