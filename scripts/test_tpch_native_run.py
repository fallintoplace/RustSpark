import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

import tpch_native_run as native


class NativeTpchTests(unittest.TestCase):
    def make_fixture(self, root):
        parquet = root / "data"
        parquet.mkdir()
        for table in native.input_arguments.__globals__["TABLES"]:
            (parquet / f"{table}.parquet").touch()
        queries = root / "queries"
        queries.mkdir()
        (queries / "01.sql").write_text("SELECT 1", encoding="utf-8")
        (queries / "02.sql").write_text("SELECT FROM", encoding="utf-8")
        return parquet, queries

    def test_uses_native_supported_flags_without_legacy_tuning_options(self):
        with tempfile.TemporaryDirectory() as path:
            parquet, queries = self.make_fixture(Path(path))
            success = mock.Mock(returncode=0, stdout='{"answer":1}\n', stderr="")
            failure = mock.Mock(returncode=2, stdout="", stderr="unsupported SQL")
            with mock.patch.object(native.subprocess, "run", side_effect=[success, failure]) as run:
                report = native.run_suite("rustspark", native.query_paths(queries), parquet, 10)
            self.assertEqual([item["status"] for item in report["results"]],
                             ["executed", "error"])
            self.assertEqual(report["results"][0]["result_rows"], 1)
            self.assertIn("execution only", report["validation"].lower().replace("sql ", ""))
            self.assertEqual(len(run.call_args_list), 2)
            command = run.call_args_list[0].args[0]
            self.assertIn("--parquet-table", command)
            self.assertIn("--stdout-format", command)
            for legacy in ("--target-partitions", "--batch-size",
                           "--memory-limit-mb", "--parquet-pushdown-filters"):
                self.assertNotIn(legacy, command)

    def test_require_all_does_not_treat_two_executed_queries_as_complete(self):
        with tempfile.TemporaryDirectory() as path:
            root = Path(path)
            parquet, queries = self.make_fixture(root)
            fake = mock.Mock(returncode=0, stdout='{"n":1}\n', stderr="")
            with mock.patch.object(native.subprocess, "run", return_value=fake):
                result = native.main([
                    "--parquet-dir", str(parquet), "--queries-dir", str(queries),
                    "--json-out", str(root / "report.json"), "--require-all",
                ])
            self.assertEqual(result, 1)
            report = json.loads((root / "report.json").read_text())
            self.assertEqual(len(report["results"]), 2)


if __name__ == "__main__":
    unittest.main()
