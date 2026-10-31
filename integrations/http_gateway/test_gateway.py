import json
import unittest
import tempfile
import subprocess
import sys
import os
import time
from pathlib import Path
from server import command_for_template, list_query_templates, validate_params, parse_result_rows, run_bounded_backend


class GatewayTests(unittest.TestCase):
    def test_typed_parameters_not_substituted_into_sql(self):
        args = command_for_template(
            "rustspark", Path("/db"), Path("/queries/orders.sql"),
            {"customer": "utf8:Alice Smith", "limit": "int64:5"}, 100, 10,
        )
        self.assertIn("customer=utf8:Alice Smith", args)
        self.assertIn("--sql-file", args)
        self.assertNotIn("Alice Smith", args)
        self.assertEqual(args[args.index("--sql-file") + 1], "/queries/orders.sql")

    def test_input_validation(self):
        self.assertEqual(validate_params(b'{"params":{"id":"int64:123"}}'), {"id": "int64:123"})
        invalid = [
            b'{"sql":"DROP TABLE users"}', b'{"params":[]}',
            b'{"params":{"1x":"int64:4"}}', b'{"params":{"x":"4"}}',
            b'{"params":{"x":4}}', b'invalid',
        ]
        for body in invalid:
            with self.subTest(body=body), self.assertRaises((ValueError, json.JSONDecodeError)):
                validate_params(body)


    def test_multibyte_parameters_obey_utf8_byte_budget(self):
        accepted = {"params": {"name": "utf8:" + "é" * 4093}}
        rejected = {"params": {"name": "utf8:" + "é" * 4094}}
        self.assertEqual(validate_params(json.dumps(accepted).encode("utf-8")),
                         accepted["params"])
        with self.assertRaisesRegex(ValueError, "typed strings"):
            validate_params(json.dumps(rejected, ensure_ascii=False).encode("utf-8"))

    def test_duplicate_json_request_keys_are_not_accepted(self):
        for body in (
            b'{"params":{"id":"int64:1","id":"int64:2"}}',
            b'{"params":{},"params":{"id":"int64:2"}}',
        ):
            with self.subTest(body=body), self.assertRaises(ValueError):
                validate_params(body)

    def test_backend_rows_must_be_json_objects(self):
        self.assertEqual(parse_result_rows('[{"city":"Berlin"},{"amount":3}]'),
                         [{"city": "Berlin"}, {"amount": 3}])
        self.assertEqual(parse_result_rows("[]"), [])
        for invalid in ('{}', '[1]', '[null]', '[{},"text"]', 'invalid'):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                parse_result_rows(invalid)

    def test_excessively_nested_request_is_invalid(self):
        deep = b'{"params":' + b"[" * 20000 + b"0" + b"]" * 20000 + b"}"
        with self.assertRaisesRegex(ValueError, "nesting limit"):
            validate_params(deep)

    def test_only_safe_regular_templates_are_listed(self):
        with tempfile.TemporaryDirectory() as directory:
            queries = Path(directory)
            (queries / "first.sql").write_text("SELECT 1")
            (queries / "second.sql").write_text("SELECT 2")
            (queries / "hidden.txt").write_text("SELECT 3")
            (queries / "bad.name.sql").write_text("SELECT 4")
            (queries / "link.sql").symlink_to(queries / "first.sql")
            self.assertEqual(list_query_templates(queries), ["first", "second"])


class InvalidBackendJSONTests(unittest.TestCase):
    def test_excessively_nested_backend_rows_are_invalid(self):
        deep = '[{"value":' + "[" * 20000 + "0" + "]" * 20000 + "}]"
        with self.assertRaisesRegex(ValueError, "nesting limit"):
            parse_result_rows(deep)

    def test_rejects_nonfinite_numbers_and_duplicate_result_keys(self):
        for payload in ('[{"n":NaN}]', '[{"n":Infinity}]', '[{"n":1e999}]',
                        '[{"id":1,"id":2}]', '[{"nested":{"x":1,"x":2}}]'):
            with self.subTest(payload=payload), self.assertRaises(ValueError):
                parse_result_rows(payload)
        self.assertEqual(parse_result_rows('[{"n":1.25}]'), [{"n": 1.25}])



class BoundedBackendTests(unittest.TestCase):
    def test_rejects_invalid_resource_limits_without_starting_backend(self):
        command = [sys.executable, "-c", "print('ok')"]
        for timeout in (float("nan"), float("inf"), float("-inf"), -1, 0, True, "10"):
            with self.subTest(timeout=timeout), self.assertRaisesRegex(ValueError, "budget"):
                run_bounded_backend(command, timeout=timeout, max_bytes=1024)
        for size in (float("nan"), float("inf"), -1, 0, True, 1.5, "1024"):
            with self.subTest(max_bytes=size), self.assertRaisesRegex(ValueError, "budget"):
                run_bounded_backend(command, timeout=5, max_bytes=size)

    def test_backend_output_budget_and_timeout(self):
        valid = run_bounded_backend(
            [sys.executable, "-c", "print('ok')"], timeout=5, max_bytes=32
        )
        self.assertEqual(valid.stdout, "ok\n")
        self.assertEqual(valid.returncode, 0)
        with self.assertRaisesRegex(ValueError, "budget"):
            run_bounded_backend(
                [sys.executable, "-c", "print('x' * 1024)"],
                timeout=5, max_bytes=32,
            )
        with self.assertRaises(subprocess.TimeoutExpired):
            run_bounded_backend(
                [sys.executable, "-c", "import time; time.sleep(2)"],
                timeout=0.1, max_bytes=32,
            )


class BackendDescendantCleanupTests(unittest.TestCase):
    @unittest.skipUnless(os.name == "posix", "process-group signaling requires POSIX")
    def test_timeout_closes_inherited_stdout_from_grandchildren(self):
        script = (
            "import subprocess,sys,time;"
            "subprocess.Popen([sys.executable,'-c','import time;time.sleep(6)']);"
            "print('started',flush=True);time.sleep(6)"
        )
        started = time.monotonic()
        with self.assertRaises(subprocess.TimeoutExpired):
            run_bounded_backend([sys.executable, "-c", script],
                                timeout=0.4, max_bytes=1024)
        self.assertLess(time.monotonic() - started, 3.0)


if __name__ == "__main__":
    unittest.main()
