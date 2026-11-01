import unittest
import io
import os
import signal
import subprocess
import sys
from contextlib import nullcontext
from types import SimpleNamespace
from unittest.mock import patch
from pathlib import Path
from stream import build_command, iter_arrow_batches, _BudgetedIPCStream


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

    def test_rejects_invalid_unnamed_and_database_paths_early(self):
        for invalid in ("", b"data.csv", 123):
            for flag in ("csv", "parquet", "json_input"):
                with self.subTest(flag=flag, value=invalid), self.assertRaisesRegex(
                        ValueError, "Unnamed input paths"):
                    build_command("SELECT 1", **{flag: invalid})
            with self.subTest(database_dir=invalid), self.assertRaisesRegex(
                    ValueError, "database_dir"):
                build_command("SELECT 1", database_dir=invalid)

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

        def start(command, *, stdout, stderr, start_new_session):
            self.assertEqual(start_new_session, os.name == "posix")
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



class EarlyCloseCleanupTests(unittest.TestCase):
    @unittest.skipUnless(os.name == "posix", "process groups require POSIX")
    def test_closing_iterator_kills_child_process_group(self):
        class FakeProcess:
            pid = 123456
            stdout = io.BytesIO()

            def poll(self):
                return None

            def wait(self, timeout=None):
                return -9

        process = FakeProcess()
        def start(command, *, stdout, stderr, start_new_session):
            self.assertTrue(start_new_session)
            self.assertEqual(stdout, subprocess.PIPE)
            return process

        arrow = SimpleNamespace(ipc=SimpleNamespace(
            open_stream=lambda stream: nullcontext(iter(["batch"]))
        ))
        with patch.dict(sys.modules, {"pyarrow": arrow}), patch(
            "stream.subprocess.Popen", side_effect=start
        ), patch("stream.os.killpg") as kill_group:
            batches = iter_arrow_batches("SELECT 1")
            self.assertEqual(next(batches), "batch")
            batches.close()
            kill_group.assert_called_once_with(process.pid, signal.SIGKILL)


class NamedInputTextPathTests(unittest.TestCase):
    def test_rejects_bytes_and_empty_named_paths(self):
        for source in (b"events.json", "", None, 123):
            with self.subTest(source=source), self.assertRaisesRegex(ValueError, "paths"):
                build_command("SELECT 1", json_tables={"events": source})
        self.assertIn("events=events.json", build_command("SELECT 1", json_tables={"events": "events.json"}))



class StreamDeadlineTests(unittest.TestCase):
    def test_process_timeout_is_validated_before_spawning(self):
        for value in (0, -1, True, float("inf"), float("nan"), "2"):
            with self.subTest(value=value), self.assertRaisesRegex(ValueError, "process_timeout"):
                list(iter_arrow_batches("SELECT 1", process_timeout=value))

    def test_watchdog_reports_timeout_after_killing_child_group(self):
        class FakeProcess:
            pid = 123456
            stdout = io.BytesIO()

            def poll(self):
                return None

            def wait(self, timeout=None):
                return -9

        class ImmediateTimer:
            def __init__(self, _timeout, callback):
                self.callback = callback
                self.daemon = False

            def start(self):
                self.callback()

            def cancel(self):
                pass

        process = FakeProcess()
        arrow = SimpleNamespace(ipc=SimpleNamespace(open_stream=lambda _stream: nullcontext([])))
        with patch.dict(sys.modules, {"pyarrow": arrow}), patch(
            "stream.subprocess.Popen", return_value=process
        ), patch("stream.threading.Timer", ImmediateTimer), patch(
            "stream.os.killpg"
        ) as kill_group:
            with self.assertRaisesRegex(TimeoutError, "process timeout"):
                list(iter_arrow_batches("SELECT 1", process_timeout=0.01))
            if os.name == "posix":
                kill_group.assert_called()

    @unittest.skipUnless(os.name == "posix", "requires POSIX process groups")
    def test_deadline_kills_descendants_after_parent_exits(self):
        class ExitedParent:
            pid = 123457
            stdout = io.BytesIO()

            def poll(self):
                return 0

            def wait(self, timeout=None):
                return 0

        class ImmediateTimer:
            def __init__(self, _timeout, callback):
                self.callback = callback
                self.daemon = False

            def start(self):
                self.callback()

            def cancel(self):
                pass

        arrow = SimpleNamespace(ipc=SimpleNamespace(open_stream=lambda _stream: nullcontext([])))
        with patch.dict(sys.modules, {"pyarrow": arrow}), patch(
            "stream.subprocess.Popen", return_value=ExitedParent()
        ), patch("stream.threading.Timer", ImmediateTimer), patch(
            "stream.os.killpg"
        ) as kill_group:
            with self.assertRaisesRegex(TimeoutError, "process timeout"):
                list(iter_arrow_batches("SELECT 1", process_timeout=0.01))
            kill_group.assert_called_with(ExitedParent.pid, signal.SIGKILL)


class ArrowIPCByteBudgetTests(unittest.TestCase):
    def test_small_reads_and_exact_budget_eof(self):
        stream = _BudgetedIPCStream(io.BytesIO(b"abcdef"), 6)
        self.assertEqual(stream.read(2), b"ab")
        buffer = bytearray(4)
        self.assertEqual(stream.readinto(buffer), 4)
        self.assertEqual(buffer, b"cdef")
        self.assertEqual(stream.read(4096), b"")

    def test_oversized_arrow_stream_fails_before_unbounded_read(self):
        stream = _BudgetedIPCStream(io.BytesIO(b"x" * 100_000), 20)
        self.assertEqual(stream.read(19), b"x" * 19)
        with self.assertRaisesRegex(ValueError, "byte budget"):
            stream.read(-1)
        for size in (0, -1, 1.5, True):
            with self.subTest(size=size), self.assertRaisesRegex(ValueError, "max_ipc_bytes"):
                list(iter_arrow_batches("SELECT 1", max_ipc_bytes=size))

    def test_ipc_budget_failure_terminates_early_stream(self):
        class FakeProcess:
            pid = 76543
            stdout = io.BytesIO(b"x" * 32)
            def poll(self):
                return None
            def wait(self, timeout=None):
                return -9
        def open_stream(source):
            source.read(20)
            return nullcontext([])
        arrow = SimpleNamespace(ipc=SimpleNamespace(open_stream=open_stream))
        with patch.dict(sys.modules, {"pyarrow": arrow}), patch(
            "stream.subprocess.Popen", return_value=FakeProcess()
        ), patch("stream.os.killpg") as kill_group:
            with self.assertRaisesRegex(ValueError, "byte budget"):
                list(iter_arrow_batches("SELECT 1", max_ipc_bytes=10))
            if os.name == "posix":
                kill_group.assert_called_once_with(FakeProcess.pid, signal.SIGKILL)

if __name__ == "__main__":
    unittest.main()
