import unittest
import io
import subprocess
import sys
from contextlib import nullcontext
from types import SimpleNamespace
from unittest.mock import patch
from pathlib import Path
from stream import build_command, iter_arrow_batches


class ArrowStreamCommandTests(unittest.TestCase):
    def test_command_uses_arrow_ipc_and_literal_file_paths(self):
        args = build_command("SELECT * FROM data", csv=Path("/tmp/a b.csv"), max_rows=50)
        self.assertEqual(args[:4], ["rustspark", "--sql", "SELECT * FROM data", "--stdout-format"])
        self.assertIn("arrow", args)
        self.assertIn("/tmp/a b.csv", args)
        self.assertIn("50", args)

    def test_nontext_sql_raises_validation_error(self):
        for sql in (None, 2, False, [], ""):
            with self.subTest(sql=sql), self.assertRaisesRegex(ValueError, "SQL must not be empty"):
                build_command(sql)

    def test_rejects_conflicting_sources(self):
        with self.assertRaises(ValueError):
            build_command("SELECT 1", csv="a.csv", parquet="b.parquet")
        with self.assertRaises(ValueError):
            build_command(" ", csv="a.csv")
        with self.assertRaises(ValueError):
            build_command("SELECT 1", query_timeout=0)

    def test_named_tables_enable_multi_file_queries(self):
        command = build_command("SELECT * FROM users JOIN orders ON users.id = orders.uid",
                                csv_tables={"users": "/a b/users.csv"},
                                parquet_tables={"orders": "/data/orders.parquet"})
        self.assertIn("users=/a b/users.csv", command)
        self.assertIn("orders=/data/orders.parquet", command)
        self.assertIn("--csv-table", command)
        with self.assertRaisesRegex(ValueError, "Named and unnamed"):
            build_command("SELECT 1", csv="a.csv", csv_tables={"users": "b.csv"})
        for name in ("bad-name", "a b", "1bad", "é"):
            with self.subTest(name=name), self.assertRaisesRegex(ValueError, "table name"):
                build_command("SELECT 1", json_tables={name: "data.json"})


    def test_database_directory_is_forwarded_without_shell(self):
        args = build_command("SELECT * FROM events", database_dir=Path("/tmp/data set"))
        self.assertEqual(args[args.index("--database-dir") + 1], "/tmp/data set")

    def test_integer_limits_are_checked_before_launch(self):
        for invalid in (True, 1.5, float("nan"), -1):
            with self.subTest(max_rows=invalid), self.assertRaisesRegex(ValueError, "max_rows"):
                build_command("SELECT 1", max_rows=invalid)
        for invalid in (False, 1.5, float("inf"), -1):
            with self.subTest(query_timeout=invalid), self.assertRaisesRegex(ValueError, "query_timeout"):
                build_command("SELECT 1", query_timeout=invalid)
        self.assertIn("0", build_command("SELECT 1", max_rows=0))

    def test_spooled_stderr_does_not_deadlock_and_keeps_error_tail(self):
        class FakeProcess:
            def __init__(self):
                self.stdout = io.BytesIO()

            def poll(self):
                return 1

            def wait(self, timeout=None):
                return 1

        def start(command, *, stdout, stderr):
            self.assertEqual(stdout, subprocess.PIPE)
            self.assertNotEqual(stderr, subprocess.PIPE)
            self.assertEqual(command[command.index("--database-dir") + 1], "/tmp/my db")
            stderr.write(b"ignored log" * 2000 + b"FINAL ERROR")
            stderr.flush()
            return FakeProcess()

        arrow = SimpleNamespace(ipc=SimpleNamespace(open_stream=lambda stream: nullcontext([])))
        with patch.dict(sys.modules, {"pyarrow": arrow}), patch(
            "stream.subprocess.Popen", side_effect=start
        ):
            with self.assertRaisesRegex(RuntimeError, "FINAL ERROR"):
                list(iter_arrow_batches("SELECT 1", database_dir="/tmp/my db"))

    def test_mixed_type_table_keys_fail_with_validation_error(self):
        for name in (None, 3):
            with self.subTest(name=name), self.assertRaisesRegex(ValueError, "table name"):
                build_command("SELECT 1", csv_tables={"valid": "good.csv", name: "bad.csv"})



class NamedInputTextPathTests(unittest.TestCase):
    def test_rejects_bytes_and_empty_named_paths(self):
        for source in (b"events.json", ""):
            with self.subTest(source=source), self.assertRaisesRegex(ValueError, "paths"):
                build_command("SELECT 1", json_tables={"events": source})
        self.assertIn("events=events.json", build_command("SELECT 1", json_tables={"events": "events.json"}))


if __name__ == "__main__":
    unittest.main()
