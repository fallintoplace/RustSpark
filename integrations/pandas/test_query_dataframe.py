import subprocess
import tempfile
import os
import signal
import sys
from contextlib import nullcontext
from types import SimpleNamespace
from unittest.mock import patch
import unittest
from pathlib import Path
from query_dataframe import build_command, query_dataframe, _export_arrow


class DataFrameCommandTests(unittest.TestCase):
    def test_limits_result_rows(self):
        args = build_command("SELECT 1", Path("/tmp/x.arrow"), max_rows=25)
        self.assertIn("--output-arrow", args)
        self.assertEqual(args[args.index("--max-rows") + 1], "25")

    def test_nontext_sql_uses_validation_error(self):
        for sql in (None, 1, False, [], ""):
            with self.subTest(sql=sql), self.assertRaisesRegex(ValueError, "SQL cannot be empty"):
                build_command(sql, Path("out.arrow"))

    def test_refuses_unbounded_and_conflicting_options(self):
        with self.assertRaises(ValueError):
            build_command("SELECT 1", Path("r.arrow"), max_rows=0)
        with self.assertRaises(ValueError):
            build_command("SELECT 1", Path("r.arrow"), csv="a.csv", parquet="b.parquet")

    def test_invalid_text_paths_are_rejected_before_export(self):
        for option in ("csv", "parquet", "json_input", "database_dir"):
            for value in ("", b"bytes.csv", 42):
                with self.subTest(option=option, value=value), self.assertRaises(ValueError):
                    build_command("SELECT 1", Path("out.arrow"), **{option: value})
        command = build_command("SELECT 1", Path("out.arrow"), csv=Path("input.csv"))
        self.assertEqual(command[command.index("--csv") + 1], "input.csv")

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

    def test_named_parameter_byte_limit_uses_utf8(self):
        accepted = "utf8:" + "é" * 4093
        command = build_command("SELECT $payload", Path("out.arrow"),
                                named_params={"payload": accepted})
        self.assertIn("payload=" + accepted, command)
        with self.assertRaisesRegex(ValueError, "bounded typed"):
            build_command("SELECT $payload", Path("out.arrow"),
                          named_params={"payload": "utf8:" + "é" * 4094})

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
             patch("query_dataframe.subprocess.Popen") as spawn:
            proc = spawn.return_value
            proc.wait.return_value = 0
            proc.poll.return_value = 0
            self.assertEqual(query_dataframe("SELECT 1", process_timeout=3), "frame")
            self.assertEqual(proc.wait.call_args.kwargs["timeout"], 3)
            self.assertEqual(spawn.call_args.kwargs["stdout"], subprocess.DEVNULL)
            self.assertNotEqual(spawn.call_args.kwargs["stderr"], subprocess.PIPE)
            self.assertEqual(spawn.call_args.kwargs["start_new_session"], os.name == "posix")
            proc.wait.side_effect = [subprocess.TimeoutExpired("rustspark", 3), -9]
            proc.poll.return_value = None
            with patch("query_dataframe.os.killpg") as kill_group:
                with self.assertRaisesRegex(RuntimeError, "process timeout"):
                    query_dataframe("SELECT 1", process_timeout=3)
                if os.name == "posix":
                    kill_group.assert_called_once_with(proc.pid, signal.SIGKILL)


class BoundedPandasErrorTests(unittest.TestCase):
    def test_query_failure_reports_only_the_tail_of_spooled_diagnostics(self):
        fake_pa = SimpleNamespace()
        def backend_failure(command, *, stderr, **kwargs):
            self.assertEqual(kwargs["stdout"], subprocess.DEVNULL)
            stderr.write(b"verbose log" * 16_000 + b"\\nFINAL SQL ERROR")
            return SimpleNamespace(wait=lambda timeout=None: 1)

        with patch.dict(sys.modules, {"pyarrow": fake_pa}), patch(
            "query_dataframe.subprocess.Popen", side_effect=backend_failure
        ):
            with self.assertRaisesRegex(RuntimeError, "FINAL SQL ERROR") as raised:
                query_dataframe("SELECT 1")
        self.assertNotIn("verbose log" * 8_000, str(raised.exception))


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



class FailedPandasExportStartupTests(unittest.TestCase):
    def test_missing_binary_is_a_readable_runtime_error(self):
        with patch.dict(sys.modules, {"pyarrow": SimpleNamespace()}), patch(
            "query_dataframe.subprocess.Popen", side_effect=FileNotFoundError("no executable")
        ):
            with self.assertRaisesRegex(RuntimeError, "Unable to start"):
                query_dataframe("SELECT 1")



class ArrowDiskBudgetTests(unittest.TestCase):
    def test_rejects_invalid_arrow_limits(self):
        for invalid in (0, -1, True, 1.5, "1024"):
            with self.subTest(invalid=invalid), self.assertRaisesRegex(ValueError, "max_arrow_bytes"):
                query_dataframe("SELECT 1", max_arrow_bytes=invalid)

    @unittest.skipUnless(os.name == "posix", "process group cleanup requires POSIX")
    def test_kills_exporter_after_temporary_output_exceeds_budget(self):
        with tempfile.TemporaryDirectory() as directory, tempfile.TemporaryFile() as diagnostics:
            output = Path(directory) / "result.arrow"
            script = (
                "from pathlib import Path; import sys, time;"
                "Path(sys.argv[1]).write_bytes(b'x' * 8192);time.sleep(3)"
            )
            with self.assertRaisesRegex(RuntimeError, "max_arrow_bytes"):
                _export_arrow([sys.executable, "-c", script, str(output)],
                              diagnostics, 2, output, 1024)

    def test_exact_size_export_fits(self):
        with tempfile.TemporaryDirectory() as directory, tempfile.TemporaryFile() as diagnostics:
            output = Path(directory) / "result.arrow"
            script = (
                "from pathlib import Path; import sys;"
                "Path(sys.argv[1]).write_bytes(b'x' * 128)"
            )
            self.assertEqual(_export_arrow([sys.executable, "-c", script, str(output)],
                                           diagnostics, 2, output, 128), 0)


if __name__ == "__main__":
    unittest.main()
