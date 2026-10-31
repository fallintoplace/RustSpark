import subprocess
import sys
from types import SimpleNamespace
from unittest.mock import patch
import unittest
from pathlib import Path
from query_lazy import build_command, rustspark_lazy_frame


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
             patch("query_lazy.subprocess.run") as run:
            run.return_value = subprocess.CompletedProcess([], 0, "", "")
            with rustspark_lazy_frame("SELECT 1", process_timeout=3) as frame:
                self.assertEqual(frame, "lazy frame")
            self.assertEqual(run.call_args.kwargs["timeout"], 3)
            self.assertEqual(run.call_args.kwargs["stdout"], subprocess.DEVNULL)
            self.assertNotEqual(run.call_args.kwargs["stderr"], subprocess.PIPE)
            run.side_effect = subprocess.TimeoutExpired("rustspark", 3)
            with self.assertRaisesRegex(RuntimeError, "process timeout"):
                with rustspark_lazy_frame("SELECT 1", process_timeout=3):
                    pass

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
            return subprocess.CompletedProcess(command, 1)

        with patch.dict(sys.modules, {"polars": SimpleNamespace()}), patch(
            "query_lazy.subprocess.run", side_effect=backend_failure
        ):
            with self.assertRaisesRegex(RuntimeError, "FINAL QUERY ERROR") as raised:
                with rustspark_lazy_frame("SELECT 1"):
                    pass
        self.assertNotIn("noisy log" * 8_000, str(raised.exception))


if __name__ == "__main__":
    unittest.main()
