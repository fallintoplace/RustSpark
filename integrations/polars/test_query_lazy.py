import subprocess
import tempfile
import os
import signal
import sys
from types import SimpleNamespace
from unittest.mock import patch
import unittest
from pathlib import Path
from query_lazy import build_command, rustspark_lazy_frame, _export_arrow


class PolarsCommandTests(unittest.TestCase):
    def test_generates_ipc_file_without_shell(self):
        command = build_command("SELECT 1", Path("/tmp/x y.arrow"), csv="/tmp/a b.csv")
        self.assertEqual(command[0], "rustspark")
        self.assertIn("/tmp/x y.arrow", command)
        self.assertIn("/tmp/a b.csv", command)
        self.assertIn("--output-arrow", command)

    def test_invalid_sql_types_are_rejected(self):
        for sql in (None, 1, False, [], ""):
            with self.subTest(sql=sql), self.assertRaisesRegex(ValueError, "SQL cannot be blank"):
                build_command(sql, Path("out.arrow"))

    def test_rejects_conflicting_sources(self):
        with self.assertRaises(ValueError):
            build_command("SELECT 1", Path("r.arrow"), csv="a.csv", parquet="b.parquet")

    def test_rejects_invalid_unnamed_and_database_paths_before_launch(self):
        for invalid in ("", b"data.csv", 123, None):
            for flag in ("csv", "parquet", "json_input"):
                if invalid is None:
                    continue
                with self.subTest(flag=flag, value=invalid), self.assertRaisesRegex(
                        ValueError, "Unnamed sources"):
                    build_command("SELECT 1", Path("out.arrow"), **{flag: invalid})
        for invalid in ("", b"/tmp/database", 123):
            with self.subTest(database_dir=invalid), self.assertRaisesRegex(
                    ValueError, "database_dir"):
                build_command("SELECT 1", Path("out.arrow"), database_dir=invalid)

    def test_persistent_database_directory_is_passed_as_argv(self):
        command = build_command("SELECT * FROM accounts", Path("result.arrow"),
                                database_dir="/tmp/rustspark db")
        self.assertEqual(command[command.index("--database-dir") + 1], "/tmp/rustspark db")
        self.assertNotIn("--csv", command)
        self.assertNotIn("--parquet", command)


    def test_combined_json_named_tables_and_row_limit(self):
        command = build_command(
            "SELECT * FROM users JOIN orders ON users.id = orders.id",
            Path("out.arrow"), csv_tables={"users": "users.csv"},
            parquet_tables={"orders": Path("orders.parquet")},
            max_rows=25, database_dir="/tmp/database",
        )
        self.assertIn("users=users.csv", command)
        self.assertIn("orders=orders.parquet", command)
        self.assertEqual(command[command.index("--max-rows") + 1], "25")
        self.assertIn("--database-dir", command)
        json_command = build_command("SELECT * FROM events", Path("out.arrow"),
                                     json_input="events.json")
        self.assertEqual(json_command[json_command.index("--json") + 1], "events.json")
        for options in (
            {"csv": "a.csv", "json_input": "b.json"},
            {"csv": "a.csv", "csv_tables": {"users": "users.csv"}},
            {"csv_tables": {"bad-name": "users.csv"}},
            {"max_rows": 0},
            {"max_rows": True},
            {"max_rows": 1.5},
        ):
            with self.subTest(options=options), self.assertRaises(ValueError):
                build_command("SELECT 1", Path("out.arrow"), **options)

    def test_process_timeout_bounds_entire_arrow_export(self):
        for invalid in (0, -1, True, float("nan"), float("inf")):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                with rustspark_lazy_frame("SELECT 1", process_timeout=invalid):
                    pass
        fake_pl = SimpleNamespace(scan_ipc=lambda path: "lazy frame")
        with patch.dict(sys.modules, {"polars": fake_pl}), \
             patch("query_lazy.subprocess.Popen") as spawn:
            proc = spawn.return_value
            proc.wait.return_value = 0
            proc.poll.return_value = 0
            with rustspark_lazy_frame("SELECT 1", process_timeout=3) as frame:
                self.assertEqual(frame, "lazy frame")
            self.assertEqual(proc.wait.call_args.kwargs["timeout"], 3)
            self.assertEqual(spawn.call_args.kwargs["stdout"], subprocess.DEVNULL)
            self.assertNotEqual(spawn.call_args.kwargs["stderr"], subprocess.PIPE)
            self.assertEqual(spawn.call_args.kwargs["start_new_session"], os.name == "posix")
            proc.wait.side_effect = [subprocess.TimeoutExpired("rustspark", 3), -9]
            proc.poll.return_value = None
            with patch("query_lazy.os.killpg") as kill_group:
                with self.assertRaisesRegex(RuntimeError, "process timeout"):
                    with rustspark_lazy_frame("SELECT 1", process_timeout=3):
                        pass
                if os.name == "posix":
                    kill_group.assert_called_once_with(proc.pid, signal.SIGKILL)

    def test_named_json_tables_are_forwarded_and_mixed_sources_rejected(self):
        command = build_command("SELECT * FROM events", Path("out.arrow"),
                                json_tables={"events": Path("events.json")})
        self.assertEqual(command[command.index("--json-table") + 1], "events=events.json")
        with self.assertRaisesRegex(ValueError, "Named and unnamed"):
            build_command("SELECT 1", Path("out.arrow"), csv="events.csv",
                          json_tables={"events": "events.json"})
        with self.assertRaises(ValueError):
            build_command("SELECT 1", Path("out.arrow"), json_tables={"bad-name": "x.json"})



class BoundedPolarsErrorTests(unittest.TestCase):
    def test_failed_native_query_reports_bounded_stderr_tail(self):
        def backend_failure(command, *, stderr, **kwargs):
            self.assertEqual(kwargs["stdout"], subprocess.DEVNULL)
            stderr.write(b"noisy log" * 20_000 + b"\\nFINAL QUERY ERROR")
            return SimpleNamespace(wait=lambda timeout=None: 1)

        with patch.dict(sys.modules, {"polars": SimpleNamespace()}), patch(
            "query_lazy.subprocess.Popen", side_effect=backend_failure
        ):
            with self.assertRaisesRegex(RuntimeError, "FINAL QUERY ERROR") as raised:
                with rustspark_lazy_frame("SELECT 1"):
                    pass
        self.assertNotIn("noisy log" * 8_000, str(raised.exception))



class FailedPolarsExportStartupTests(unittest.TestCase):
    def test_missing_binary_is_a_readable_runtime_error(self):
        with patch.dict(sys.modules, {"polars": SimpleNamespace()}), patch(
            "query_lazy.subprocess.Popen", side_effect=FileNotFoundError("no executable")
        ):
            with self.assertRaisesRegex(RuntimeError, "Unable to start"):
                with rustspark_lazy_frame("SELECT 1"):
                    pass


class PolarsArrowDiskBudgetTests(unittest.TestCase):
    def test_rejects_invalid_disk_limits(self):
        for invalid in (0, -1, True, 1.5, "1024"):
            with self.subTest(invalid=invalid), self.assertRaisesRegex(ValueError, "max_arrow_bytes"):
                with rustspark_lazy_frame("SELECT 1", max_arrow_bytes=invalid):
                    pass

    @unittest.skipUnless(os.name == "posix", "process group cleanup requires POSIX")
    def test_terminates_exporter_that_exceeds_disk_budget(self):
        with tempfile.TemporaryDirectory() as directory, tempfile.TemporaryFile() as diagnostics:
            output = Path(directory) / "result.arrow"
            script = (
                "from pathlib import Path; import sys, time;"
                "Path(sys.argv[1]).write_bytes(b'x' * 8192);time.sleep(3)"
            )
            with self.assertRaisesRegex(RuntimeError, "max_arrow_bytes"):
                _export_arrow([sys.executable, "-c", script, str(output)],
                              diagnostics, 2, output, 1024)

    def test_allows_exact_file_budget(self):
        with tempfile.TemporaryDirectory() as directory, tempfile.TemporaryFile() as diagnostics:
            output = Path(directory) / "result.arrow"
            script = (
                "from pathlib import Path; import sys;"
                "Path(sys.argv[1]).write_bytes(b'x' * 128)"
            )
            self.assertEqual(_export_arrow([sys.executable, "-c", script, str(output)],
                                           diagnostics, 2, output, 128), 0)



class NamedSQLParameterTests(unittest.TestCase):
    def test_typed_params_have_stable_order_and_bounded_utf8_values(self):
        build = lambda params: build_command("SELECT $a", Path("result.arrow"), named_params=params)
        command = build({"z": "int64:9", "a": "utf8:hello world"})
        pairs = [command[i+1] for i, arg in enumerate(command[:-1]) if arg == "--named-param"]
        self.assertEqual(pairs, ["a=utf8:hello world", "z=int64:9"])
        self.assertIn("a=utf8:hello world", command)
        self.assertIn("p=" + "utf8:" + "é" * 4093 + "a",
                      build({"p": "utf8:" + "é" * 4093 + "a"}))
        for invalid in (
            {"bad-name": "int64:1"},
            {"x": "untyped"},
            {"x": 123},
            {"x": "utf8:" + "é" * 4093 + "aa"},
            {"p" + str(i): "int64:1" for i in range(65)},
        ):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                build(invalid)

    def test_lazy_export_forwards_named_parameters(self):
        captured = []
        def launch(args, **kwargs):
            captured.extend(args)
            return SimpleNamespace(wait=lambda timeout=None: 0, poll=lambda: 0)
        fake_polars = SimpleNamespace(scan_ipc=lambda path: "frame")
        with patch.dict(sys.modules, {"polars": fake_polars}), patch(
            "query_lazy.subprocess.Popen", side_effect=launch
        ):
            with rustspark_lazy_frame("SELECT $p",
                                     named_params={"p": "int64:7"}) as frame:
                self.assertEqual(frame, "frame")
        self.assertIn("p=int64:7", captured)


class ExportDiagnosticsBudgetTests(unittest.TestCase):
    @unittest.skipUnless(os.name == "posix", "process group cleanup requires POSIX")
    def test_large_stderr_stops_live_export_before_timeout(self):
        with tempfile.TemporaryDirectory() as directory, tempfile.TemporaryFile() as diagnostics:
            output = Path(directory) / "result.arrow"
            script = (
                "import sys,time;"
                "sys.stderr.write('x'*8192);"
                "sys.stderr.flush();time.sleep(4)"
            )
            with self.assertRaisesRegex(RuntimeError, "max_stderr_bytes"):
                _export_arrow([sys.executable, "-c", script],
                              diagnostics, 2, output, 1024, max_stderr_bytes=512)

    @unittest.skipUnless(os.name == "posix", "process group cleanup requires POSIX")
    def test_failed_size_metadata_check_cancels_export(self):
        with tempfile.TemporaryDirectory() as directory, tempfile.TemporaryFile() as diagnostics:
            output = Path(directory) / "result.arrow"
            with patch.object(Path, "stat", side_effect=PermissionError("denied")):
                with self.assertRaisesRegex(RuntimeError, "inspect"):
                    _export_arrow([sys.executable, "-c", "import time;time.sleep(4)"],
                                  diagnostics, 2, output, 1024)

    def test_invalid_diagnostics_limit_fails_before_launch(self):
        with tempfile.TemporaryFile() as diagnostics:
            with self.assertRaisesRegex(ValueError, "max_stderr_bytes"):
                _export_arrow([sys.executable, "-c", "print('unused')"],
                              diagnostics, 2, Path("never.arrow"), 1024,
                              max_stderr_bytes=0)

if __name__ == "__main__":
    unittest.main()
