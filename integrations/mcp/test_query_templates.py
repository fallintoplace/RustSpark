import tempfile
from pathlib import Path
import unittest

from query_templates import list_templates, make_command, parse_query_rows, template_path, pinned_template, MAX_TEMPLATE_BYTES


class TemplatesTests(unittest.TestCase):
    def test_names_are_allowlisted_and_cannot_escape_directory(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            (base / "safe.sql").write_text("SELECT 1", encoding="utf-8")
            (base / "bad.name.sql").write_text("SELECT 2", encoding="utf-8")
            self.assertEqual(list_templates(base), ["safe"])
            self.assertEqual(template_path(base, "safe").name, "safe.sql")
            for name in ["../secret", "bad.name", "", ".hidden", "some/name", None, 42]:
                with self.subTest(name=name), self.assertRaises(ValueError):
                    template_path(base, name)

    def test_params_are_bounded_and_bound_outside_sql(self):
        command = make_command("rustspark", Path("/db"), Path("orders.sql"),
                               {"region": "utf8:North America"})
        self.assertIn("region=utf8:North America", command)
        self.assertIn("--sql-file", command)
        with self.assertRaises(ValueError):
            make_command("rustspark", Path("/db"), Path("q.sql"),
                         {"bad-name": "int64:3"})
        with self.assertRaises(ValueError):
            make_command("rustspark", Path("/db"), Path("q.sql"),
                         {"value": "utf8:" + "é" * 2047})
        accepted = make_command("rustspark", Path("/db"), Path("q.sql"),
                                {"value": "utf8:" + "é" * 2045})
        self.assertIn("value=" + "utf8:" + "é" * 2045, accepted)

    def test_configurable_execution_bounds_are_strict(self):
        command = make_command("rustspark", Path("/db"), Path("q.sql"), {},
                               max_rows=25, query_timeout=5)
        self.assertEqual(command[command.index("--max-rows") + 1], "25")
        self.assertEqual(command[command.index("--query-timeout") + 1], "5")
        for opts in ({"max_rows": 0}, {"max_rows": 201},
                     {"max_rows": True}, {"query_timeout": 21},
                     {"query_timeout": False}):
            with self.subTest(opts=opts), self.assertRaises(ValueError):
                make_command("rustspark", Path("/db"), Path("q.sql"), {}, **opts)

    def test_query_results_require_json_object_rows(self):
        self.assertEqual(parse_query_rows('[{"x":1},{"x":null}]'), [{"x": 1}, {"x": None}])
        self.assertEqual(parse_query_rows("[]"), [])
        with self.assertRaisesRegex(ValueError, "Nonfinite"):
            parse_query_rows('[{"x":1e999}]')
        for text in ("{}", "[null]", "[1]", "[[]]", "[NaN]", "invalid"):
            with self.subTest(text=text), self.assertRaises(ValueError):
                parse_query_rows(text)

    def test_deeply_nested_json_result_has_a_bounded_failure(self):
        deep = '[{"v":' + "[" * 20000 + "0" + "]" * 20000 + "}]"
        with self.assertRaisesRegex(ValueError, "nesting limit"):
            parse_query_rows(deep)

    def test_mixed_type_parameter_keys_are_rejected_before_sorting(self):
        for invalid in (None, 3):
            with self.subTest(invalid=invalid), self.assertRaisesRegex(ValueError, "parameter name"):
                make_command("rustspark", Path("/db"), Path("q.sql"),
                             {"good": "int64:1", invalid: "int64:2"})



class DuplicateResultKeysTests(unittest.TestCase):
    def test_nested_and_top_level_duplicate_keys_are_rejected(self):
        for text in ('[{"id":1,"id":2}]', '[{"outer":{"name":"a","name":"b"}}]'):
            with self.subTest(text=text), self.assertRaisesRegex(ValueError, "Duplicate SQL"):
                parse_query_rows(text)
        self.assertEqual(parse_query_rows('[{"outer":{"x":1,"y":2}}]'),
                         [{"outer": {"x": 1, "y": 2}}])



class BoundedMcpBackendTests(unittest.TestCase):
    def test_rejects_nonfinite_or_noninteger_budgets_before_spawn(self):
        from bounded_backend import run_bounded
        import sys

        command = [sys.executable, "-c", "print('ok')"]
        for timeout in (float("nan"), float("inf"), float("-inf"), 0, -1, True, "1"):
            with self.subTest(timeout=timeout), self.assertRaisesRegex(ValueError, "budget"):
                run_bounded(command, timeout=timeout, max_bytes=100)
        for max_bytes in (float("nan"), float("inf"), 0, -1, True, 1.5, "100"):
            with self.subTest(max_bytes=max_bytes), self.assertRaisesRegex(ValueError, "budget"):
                run_bounded(command, timeout=1, max_bytes=max_bytes)

    def test_invalid_backend_utf8_is_reported_as_checked_failure(self):
        import sys
        from bounded_backend import run_bounded

        with self.assertRaisesRegex(ValueError, "UTF-8"):
            run_bounded([
                sys.executable, "-c",
                "import sys; sys.stdout.buffer.write(bytes([255]))",
            ], timeout=5, max_bytes=1024)

    def test_output_is_bounded_without_buffering_unlimited_backend_data(self):
        import subprocess
        import sys
        from bounded_backend import run_bounded

        good = run_bounded([sys.executable, "-c", "print('ok')"],
                           timeout=5, max_bytes=128)
        self.assertEqual(good.stdout, "ok\n")
        with self.assertRaisesRegex(ValueError, "budget"):
            run_bounded([sys.executable, "-c", "print('x' * 65536)"],
                        timeout=5, max_bytes=128)
        with self.assertRaises(subprocess.TimeoutExpired):
            run_bounded([sys.executable, "-c", "import time; time.sleep(2)"],
                        timeout=0.1, max_bytes=128)


class ProcessGroupCleanupTests(unittest.TestCase):
    def test_timeout_terminates_descendants_inheriting_the_pipe(self):
        import os
        import subprocess
        import sys
        import time
        from bounded_backend import run_bounded

        if os.name != "posix":
            self.skipTest("process-group signaling requires POSIX")
        script = (
            "import subprocess,sys,time;"
            "subprocess.Popen([sys.executable,'-c','import time;time.sleep(6)']);"
            "print('started',flush=True);time.sleep(6)"
        )
        start = time.monotonic()
        with self.assertRaises(subprocess.TimeoutExpired):
            run_bounded([sys.executable, "-c", script],
                        timeout=0.4, max_bytes=1024)
        self.assertLess(time.monotonic() - start, 3.0)


class PinnedTemplateTests(unittest.TestCase):
    def test_fifo_template_is_rejected_without_waiting_for_a_writer(self):
        import os
        if not hasattr(os, "mkfifo") or not hasattr(os, "O_NONBLOCK"):
            self.skipTest("FIFO and nonblocking open require POSIX")
        with tempfile.TemporaryDirectory() as directory:
            fifo = Path(directory) / "waiting.sql"
            os.mkfifo(fifo)
            with self.assertRaisesRegex(ValueError, "regular file"):
                with pinned_template(fifo):
                    pass

    def test_snapshot_does_not_follow_a_replaced_template(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            approved = root / "report.sql"
            private = root / "private.sql"
            approved.write_text("SELECT 1", encoding="utf-8")
            private.write_text("SELECT 999", encoding="utf-8")
            inspected = template_path(root, "report")
            with pinned_template(inspected) as copy:
                approved.unlink()
                approved.symlink_to(private)
                self.assertEqual(copy.read_text(encoding="utf-8"), "SELECT 1")
            with self.assertRaises(ValueError):
                with pinned_template(inspected):
                    pass

    def test_rejects_oversize_templates_and_preserves_private_permissions(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "too_big.sql"
            source.write_bytes(b"X" * (MAX_TEMPLATE_BYTES + 1))
            with self.assertRaisesRegex(ValueError, "bounded"):
                with pinned_template(source):
                    pass
            source.write_text("SELECT 1", encoding="utf-8")
            with pinned_template(source) as copy:
                self.assertEqual(copy.read_text(encoding="utf-8"), "SELECT 1")
                self.assertEqual(copy.stat().st_mode & 0o077, 0)

class BackendInterruptCleanupTests(unittest.TestCase):
    @unittest.skipUnless(__import__("os").name == "posix", "process groups require POSIX")
    def test_interrupt_during_reader_setup_reaps_backend(self):
        import subprocess
        import sys
        from unittest.mock import patch
        import bounded_backend

        original = subprocess.Popen
        started = []

        def tracked(*args, **kwargs):
            child = original(*args, **kwargs)
            started.append(child)
            return child

        with patch("bounded_backend.subprocess.Popen", side_effect=tracked):
            with patch("bounded_backend.threading.Thread", side_effect=KeyboardInterrupt):
                with self.assertRaises(KeyboardInterrupt):
                    bounded_backend.run_bounded(
                        [sys.executable, "-c", "import time; time.sleep(30)"],
                        timeout=5, max_bytes=1024,
                    )
        self.assertEqual(len(started), 1)
        self.assertIsNotNone(started[0].poll(), "interrupted process must be reaped")


if __name__ == "__main__":
    unittest.main()
