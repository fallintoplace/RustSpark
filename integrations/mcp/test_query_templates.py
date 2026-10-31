import tempfile
from pathlib import Path
import unittest

from query_templates import list_templates, make_command, parse_query_rows, template_path


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


if __name__ == "__main__":
    unittest.main()
