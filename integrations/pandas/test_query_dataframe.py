import subprocess
import sys
from contextlib import nullcontext
from types import SimpleNamespace
from unittest.mock import patch
import unittest
from pathlib import Path
from query_dataframe import build_command, query_dataframe


class DataFrameCommandTests(unittest.TestCase):
    def test_limits_result_rows(self):
        args = build_command("SELECT 1", Path("/tmp/x.arrow"), max_rows=25)
        self.assertIn("--output-arrow", args)
        self.assertEqual(args[args.index("--max-rows") + 1], "25")

    def test_refuses_unbounded_and_conflicting_options(self):
        with self.assertRaises(ValueError):
            build_command("SELECT 1", Path("r.arrow"), max_rows=0)
        with self.assertRaises(ValueError):
            build_command("SELECT 1", Path("r.arrow"), csv="a.csv", parquet="b.parquet")

    def test_persistent_database_path_is_forwarded(self):
        args = build_command("SELECT * FROM sales", Path("result.arrow"),
                             database_dir="/tmp/warehouse with spaces")
        self.assertEqual(args[args.index("--database-dir") + 1],
                         "/tmp/warehouse with spaces")
        self.assertNotIn("--csv", args)
        self.assertIn("--max-rows", args)


    def test_combined_json_named_tables_and_typed_parameters(self):
        command = build_command(
            "SELECT * FROM users JOIN orders ON users.id = orders.id",
            Path("out.arrow"), csv_tables={"users": "users.csv"},
            parquet_tables={"orders": Path("orders.parquet")},
            named_params={"min_id": "int64:10"}, database_dir="/tmp/database",
        )
        self.assertIn("users=users.csv", command)
        self.assertIn("orders=orders.parquet", command)
        self.assertIn("min_id=int64:10", command)
        self.assertIn("--database-dir", command)
        json_command = build_command("SELECT * FROM events", Path("out.arrow"),
                                     json_input="events.json")
        self.assertEqual(json_command[json_command.index("--json") + 1], "events.json")
        for options in (
            {"csv": "a.csv", "json_input": "b.json"},
            {"csv": "a.csv", "csv_tables": {"users": "users.csv"}},
            {"csv_tables": {"bad-name": "users.csv"}},
            {"named_params": {"bad-name": "int64:1"}},
            {"named_params": {"valid": "not-typed"}},
            {"max_rows": True},
        ):
            with self.subTest(options=options), self.assertRaises(ValueError):
                build_command("SELECT 1", Path("out.arrow"), **options)

    def test_process_timeout_bounds_entire_arrow_export(self):
        for invalid in (0, -1, True, float("nan"), float("inf")):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                query_dataframe("SELECT 1", process_timeout=invalid)
        mock_table = SimpleNamespace(to_pandas=lambda: "frame")
        fake_pa = SimpleNamespace(
            memory_map=lambda *args: nullcontext(None),
            ipc=SimpleNamespace(open_file=lambda source:
                SimpleNamespace(read_all=lambda: mock_table)),
        )
        with patch.dict(sys.modules, {"pyarrow": fake_pa}), \
             patch("query_dataframe.subprocess.run") as run:
            run.return_value = subprocess.CompletedProcess([], 0, "", "")
            self.assertEqual(query_dataframe("SELECT 1", process_timeout=3), "frame")
            self.assertEqual(run.call_args.kwargs["timeout"], 3)
            self.assertEqual(run.call_args.kwargs["stdout"], subprocess.DEVNULL)
            self.assertEqual(run.call_args.kwargs["stderr"], subprocess.PIPE)
            run.side_effect = subprocess.TimeoutExpired("rustspark", 3)
            with self.assertRaisesRegex(RuntimeError, "process timeout"):
                query_dataframe("SELECT 1", process_timeout=3)


class NamedJsonPandasTests(unittest.TestCase):
    def test_named_json_tables_are_bound_as_cli_arguments(self):
        command = build_command("SELECT * FROM events", Path("out.arrow"),
                                json_tables={"events": "/tmp/my events.json"})
        self.assertIn("--json-table", command)
        self.assertIn("events=/tmp/my events.json", command)
        with self.assertRaisesRegex(ValueError, "Named and unnamed"):
            build_command("SELECT 1", Path("out.arrow"), csv="input.csv",
                          json_tables={"events": "events.json"})
        with self.assertRaises(ValueError):
            build_command("SELECT 1", Path("out.arrow"),
                          json_tables={"invalid-name": "events.json"})


if __name__ == "__main__":
    unittest.main()
