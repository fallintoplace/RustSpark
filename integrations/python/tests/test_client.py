import subprocess
import sys
import os
import time

from rustspark_local.subprocess_budget import run_bounded
import unittest
from unittest.mock import patch

from rustspark_local import RustSpark, RustSparkError


class RustSparkTests(unittest.TestCase):
    @patch("rustspark_local.run_bounded")
    def test_named_bindings_are_arguments_not_interpolated_sql(self, run):
        run.return_value = subprocess.CompletedProcess([], 0, '[{"city":"a","n":2}]', "")
        client = RustSpark(binary="/opt/rustspark", database_dir="/tmp/warehouse")
        rows = client.query(
            "SELECT * FROM sales WHERE n > $minimum",
            csv_tables={"sales": "/tmp/my files/sales.csv"},
            named_params={"minimum": "int64:2"},
            max_rows=10,
        )
        self.assertEqual(rows, [{"city": "a", "n": 2}])
        command = run.call_args.args[0]
        self.assertEqual(command[0], "/opt/rustspark")
        self.assertIn("sales=/tmp/my files/sales.csv", command)
        self.assertIn("minimum=int64:2", command)
        self.assertEqual(command.count("--sql"), 1)
        self.assertNotIn("int64:2", command[command.index("--sql") + 1])

    @patch("rustspark_local.run_bounded")
    def test_reports_cli_errors(self, run):
        run.return_value = subprocess.CompletedProcess([], 2, "", "SQL error: unknown table")
        with self.assertRaisesRegex(RustSparkError, "unknown table"):
            RustSpark().query("SELECT * FROM missing")

    @patch("rustspark_local.run_bounded")
    def test_rejects_invalid_json(self, run):
        run.return_value = subprocess.CompletedProcess([], 0, "not-json", "")
        with self.assertRaisesRegex(RustSparkError, "invalid JSON"):
            RustSpark().query("SELECT 1")

    @patch("rustspark_local.run_bounded")
    def test_rejects_duplicate_keys_and_nonfinite_json(self, run):
        for output in ('[{"v":1,"v":2}]', '[{"v":NaN}]',
                       '[{"v":Infinity}]', '[{"v":1e999}]'):
            with self.subTest(output=output):
                run.return_value = subprocess.CompletedProcess([], 0, output, "")
                with self.assertRaisesRegex(RustSparkError, "invalid JSON"):
                    RustSpark().query("SELECT 1")
        run.return_value = subprocess.CompletedProcess([], 0, '[{"v":1.25}]', "")
        self.assertEqual(RustSpark().query("SELECT 1"), [{"v": 1.25}])

    @patch("rustspark_local.run_bounded")
    def test_deeply_nested_json_returns_client_error(self, run):
        deep_json = '[{"nested":' + "[" * 20000 + "0" + "]" * 20000 + "}]"
        run.return_value = subprocess.CompletedProcess([], 0, deep_json, "")
        with self.assertRaisesRegex(RustSparkError, "invalid JSON"):
            RustSpark().query("SELECT 1")

    def test_rejects_unsafe_or_conflicting_options_early(self):
        client = RustSpark()
        with self.assertRaises(ValueError):
            client.query("SELECT 1", csv="a.csv", parquet="b.parquet")
        with self.assertRaises(ValueError):
            client.query("SELECT 1", csv_tables={"bad=name": "file.csv"})
        with self.assertRaises(ValueError):
            client.query("SELECT $1", named_params={"1bad": "int64:3"})
        with self.assertRaises(ValueError):
            client.query("  ")
        with self.assertRaises(ValueError):
            client.query("SELECT 1", max_rows=-1)
        with self.assertRaises(ValueError):
            client.query("SELECT 1", process_timeout=0)

    def test_rejects_invalid_numeric_options_before_subprocess(self):
        client = RustSpark()
        for bad in (True, 1.5, float("nan"), -1):
            with self.subTest(option="max_rows", bad=bad):
                with self.assertRaisesRegex(ValueError, "max_rows"):
                    client.query("SELECT 1", max_rows=bad)
        for bad in (False, 1.5, float("inf")):
            with self.subTest(option="query_timeout", bad=bad):
                with self.assertRaisesRegex(ValueError, "query_timeout"):
                    client.query("SELECT 1", query_timeout=bad)
        for bad in (True, float("nan"), float("inf"), -1):
            with self.subTest(option="process_timeout", bad=bad):
                with self.assertRaisesRegex(ValueError, "process_timeout"):
                    client.query("SELECT 1", process_timeout=bad)

    @patch("rustspark_local.run_bounded", side_effect=subprocess.TimeoutExpired("rustspark", 3))
    def test_deadline_is_reported(self, run):
        with self.assertRaisesRegex(RustSparkError, "process timeout"):
            RustSpark().query("SELECT 1", process_timeout=3)


    @patch("rustspark_local.run_bounded")
    def test_sql_file_query_does_not_interpolate_text(self, run):
        run.return_value = subprocess.CompletedProcess([], 0, "[]", "")
        client = RustSpark()
        self.assertEqual(client.query(None, sql_file="/tmp/queries/report.sql"), [])
        argv = run.call_args.args[0]
        self.assertEqual(argv[argv.index("--sql-file") + 1], "/tmp/queries/report.sql")
        self.assertNotIn("--sql", argv)
        with self.assertRaisesRegex(ValueError, "exactly one"):
            client.query("SELECT 1", sql_file="report.sql")
        with self.assertRaisesRegex(ValueError, "exactly one"):
            client.query(None, sql_file="")

    def test_sql_identifier_validation_matches_native_cli(self):
        for name in ("has-dash", "with spaces", "é", "1bad", ""):
            with self.subTest(name=name), self.assertRaises(ValueError):
                RustSpark().query("SELECT 1", csv_tables={name: "data.csv"})
            with self.subTest(name=name), self.assertRaises(ValueError):
                RustSpark().query("SELECT 1", named_params={name: "int64:1"})
        with self.assertRaises(ValueError):
            RustSpark().query("SELECT 1", named_params={"valid_name": 12})

    @patch("rustspark_local.run_bounded")
    def test_query_one_bounds_rows_and_rejects_multiple(self, run):
        run.return_value = subprocess.CompletedProcess([], 0, '[{"id":1}]', "")
        client = RustSpark()
        self.assertEqual(client.query_one("SELECT id FROM t"), {"id": 1})
        argv = run.call_args.args[0]
        self.assertEqual(argv[argv.index("--max-rows") + 1], "2")
        run.return_value = subprocess.CompletedProcess([], 0, '[]', "")
        self.assertIsNone(client.query_one("SELECT id FROM t"))
        run.return_value = subprocess.CompletedProcess([], 0, '[{"id":1},{"id":2}]', "")
        with self.assertRaisesRegex(RustSparkError, "at most one"):
            client.query_one("SELECT id FROM t")
        with self.assertRaises(ValueError):
            client.query_one("SELECT id FROM t", max_rows=1)

    def test_mixed_type_named_table_keys_raise_value_error(self):
        for invalid in (None, 3):
            with self.subTest(invalid=invalid), self.assertRaisesRegex(ValueError, "Invalid table name"):
                RustSpark().query("SELECT 1", csv_tables={"good": "good.csv", invalid: "bad.csv"})



class DeterministicNamedParamsTests(unittest.TestCase):
    @patch("rustspark_local.run_bounded")
    def test_parameter_order_is_stable_and_invalid_types_fail_early(self, run):
        run.return_value = subprocess.CompletedProcess([], 0, "[]", "")
        RustSpark().query("SELECT 1", named_params={"z": "int64:9", "a": "int64:1"})
        argv = run.call_args.args[0]
        self.assertLess(argv.index("a=int64:1"), argv.index("z=int64:9"))
        with self.assertRaises(ValueError):
            RustSpark().query("SELECT 1", named_params={1: "int64:1"})



class TypedParameterAdmissionTests(unittest.TestCase):
    @patch("rustspark_local.run_bounded")
    def test_bounded_parameter_payloads(self, run):
        client = RustSpark()
        with self.assertRaisesRegex(ValueError, "64"):
            client.query("SELECT 1", named_params={
                f"p{i}": "int64:1" for i in range(65)
            })
        with self.assertRaisesRegex(ValueError, "bounded"):
            client.query("SELECT 1", named_params={
                "payload": "utf8:" + "x" * 8192
            })
        run.assert_not_called()
        run.return_value = subprocess.CompletedProcess([], 0, "[]", "")
        self.assertEqual(client.query("SELECT 1", named_params={
            "payload": "utf8:" + "x" * (8192 - len("utf8:"))
        }), [])


    @patch("rustspark_local.run_bounded")
    def test_utf8_typed_parameter_budget_is_measured_in_bytes(self, run):
        client = RustSpark()
        with self.assertRaisesRegex(ValueError, "bounded"):
            client.query("SELECT 1", named_params={"payload": "utf8:" + "é" * 4094})
        run.assert_not_called()
        run.return_value = subprocess.CompletedProcess([], 0, "[]", "")
        self.assertEqual(client.query(
            "SELECT 1", named_params={"payload": "utf8:" + "é" * 4093}
        ), [])
        argv = run.call_args.args[0]
        self.assertIn("payload=" + "utf8:" + "é" * 4093, argv)

    def test_named_table_paths_are_nonempty_text_paths(self):
        for path in ("", b"/tmp/events.json", 17):
            with self.subTest(path=path), self.assertRaisesRegex(ValueError, "Named source paths"):
                RustSpark().query("SELECT 1", json_tables={"events": path})


class BoundedProcessCaptureTests(unittest.TestCase):
    def test_stdout_stderr_limits_and_valid_output(self):
        output = run_bounded(
            [sys.executable, "-c", "print('ready')"],
            timeout=5, max_output_bytes=1024,
        )
        self.assertEqual(output.stdout, "ready\n")
        self.assertEqual(output.returncode, 0)
        with self.assertRaisesRegex(ValueError, "limit"):
            run_bounded(
                [sys.executable, "-c", "print('x' * 200000)"],
                timeout=5, max_output_bytes=1024,
            )
        with self.assertRaisesRegex(ValueError, "limit"):
            run_bounded(
                [sys.executable, "-c", "import sys; sys.stderr.write('x' * 200000)"],
                timeout=5, max_output_bytes=1024, max_error_bytes=1024,
            )

    def test_direct_capture_rejects_invalid_budgets_before_spawn(self):
        import rustspark_local.subprocess_budget as capture

        command = [sys.executable, "-c", "print('should not run')"]
        with patch.object(capture.subprocess, "Popen") as spawn:
            for invalid in (True, 0, -1, 1.5):
                with self.subTest(output_limit=invalid), self.assertRaises(ValueError):
                    run_bounded(command, timeout=5, max_output_bytes=invalid)
                with self.subTest(error_limit=invalid), self.assertRaises(ValueError):
                    run_bounded(command, timeout=5, max_output_bytes=1024,
                                max_error_bytes=invalid)
            for invalid in (True, 0, -1, float("nan"), float("inf"), "5"):
                with self.subTest(timeout=invalid), self.assertRaisesRegex(ValueError, "timeout"):
                    run_bounded(command, timeout=invalid, max_output_bytes=1024)
            spawn.assert_not_called()

    def test_deadline_kills_descendants_holding_stdout_open(self):
        if os.name != "posix":
            self.skipTest("requires POSIX process groups")
        script = (
            "import subprocess,sys,time;"
            "subprocess.Popen([sys.executable,'-c','import time;time.sleep(6)']);"
            "print('started',flush=True);time.sleep(6)"
        )
        start = time.monotonic()
        with self.assertRaises(subprocess.TimeoutExpired):
            run_bounded([sys.executable, "-c", script],
                        timeout=0.4, max_output_bytes=1024)
        self.assertLess(time.monotonic() - start, 3.0)

    def test_invalid_max_output_bytes_rejected_before_process(self):
        for invalid in [0, -1, True, 1.5]:
            with self.subTest(invalid=invalid), self.assertRaisesRegex(ValueError, "max_output_bytes"):
                RustSpark().query("SELECT 1", max_output_bytes=invalid)


    @unittest.skipUnless(os.name == "posix", "process groups require POSIX")
    def test_keyboard_interrupt_terminates_process_and_pipe_workers(self):
        import rustspark_local.subprocess_budget as capture
        original_popen = subprocess.Popen
        started = []

        def record_process(*args, **kwargs):
            process = original_popen(*args, **kwargs)
            started.append(process)
            return process

        with patch.object(capture.subprocess, "Popen", side_effect=record_process):
            with patch.object(capture.threading.Thread, "join", side_effect=KeyboardInterrupt):
                with self.assertRaises(KeyboardInterrupt):
                    run_bounded(
                        [sys.executable, "-c", "import time; time.sleep(30)"],
                        timeout=5, max_output_bytes=1024,
                    )
        self.assertEqual(len(started), 1)
        self.assertIsNotNone(started[0].poll(), "interrupted SQL child must be reaped")



class StreamingPythonQueryTests(unittest.TestCase):
    @unittest.skipUnless(os.name == "posix", "helper binary requires POSIX shell")
    def test_streams_typed_rows_without_buffering_the_result(self):
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as directory:
            script = Path(directory) / "fake-rustspark"
            script.write_text(
                """#!/bin/sh
printf '%s\\n' '{"id":9223372036854775807}' '{"id":2}'
""",
                encoding="utf-8",
            )
            script.chmod(0o700)
            rows = list(RustSpark(binary=str(script)).iter_rows("SELECT 1"))
            self.assertEqual(rows, [{"id": 9223372036854775807}, {"id": 2}])

    @unittest.skipUnless(os.name == "posix", "helper binary requires POSIX shell")
    def test_invalid_rows_and_max_bytes_reject_without_hanging(self):
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as directory:
            script = Path(directory) / "fake-rustspark"
            script.write_text(
                """#!/bin/sh
printf '%s\\n' '{"x":1,"x":2}'
""",
                encoding="utf-8",
            )
            script.chmod(0o700)
            with self.assertRaisesRegex(RustSparkError, "Invalid"):
                list(RustSpark(binary=str(script)).iter_rows("SELECT 1"))
            script.write_text(
                """#!/bin/sh
printf '%s\\n' '{"x":123456789}'
""",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(RustSparkError, "max_row_bytes"):
                list(RustSpark(binary=str(script)).iter_rows("SELECT 1", max_row_bytes=5))

    def test_invalid_streaming_budgets_reject_before_spawn(self):
        for invalid in (-1, 0, True, 1.5, 64 * 1024 * 1024 + 1):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                list(RustSpark().iter_rows("SELECT 1", max_row_bytes=invalid))


class SourcePathPreflightTests(unittest.TestCase):
    def test_rejects_invalid_binary_sql_file_and_input_paths(self):
        for value in ("", b"bytes", 42):
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    RustSpark(binary=value)._command("SELECT 1")
                with self.assertRaises(ValueError):
                    RustSpark()._command(None, sql_file=value)
                with self.assertRaises(ValueError):
                    RustSpark(database_dir=value)._command("SELECT 1")
                for source in ("input", "csv", "parquet", "json_input"):
                    with self.assertRaises(ValueError):
                        RustSpark()._command("SELECT 1", **{source: value})

    def test_pathlib_sources_remain_supported(self):
        from pathlib import Path
        args = RustSpark(binary=Path("rustspark"), database_dir=Path("db"))._command(
            "SELECT 1", csv=Path("rows.csv"))
        self.assertIn("rows.csv", args)
        self.assertIn("db", args)


if __name__ == "__main__":
    unittest.main()
