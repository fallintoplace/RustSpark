import json
import unittest
import tempfile
import subprocess
import sys
import os
import time
import socket
import threading
from http.server import HTTPServer
from pathlib import Path
from unittest.mock import patch
from http.client import HTTPConnection
from server import command_for_template, list_query_templates, validate_params, parse_result_rows, run_bounded_backend, make_handler, BoundedThreadingHTTPServer


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


class RequestFramingTests(unittest.TestCase):
    def test_ambiguous_content_length_and_chunked_encoding_are_rejected(self):
        with tempfile.TemporaryDirectory() as root:
            directory = Path(root)
            (directory / "safe.sql").write_text("SELECT 1", encoding="utf-8")
            server = HTTPServer(("127.0.0.1", 0), make_handler(
                secret="test-secret" * 3, queries=directory, database=directory,
                binary="nonexistent-rustspark", max_rows=10, timeout=1,
            ))
            worker = threading.Thread(target=server.serve_forever, daemon=True)
            worker.start()
            try:
                headers = (
                    b"POST /v1/query/safe HTTP/1.1\r\n"
                    b"Host: localhost\r\n"
                    b"Authorization: Bearer test-secrettest-secrettest-secret\r\n"
                    b"Content-Type: application/json\r\n"
                )
                for framing in (
                    b"Content-Length: 2\r\nContent-Length: 2\r\n",
                    b"Content-Length: 2\r\nTransfer-Encoding: chunked\r\n",
                    b"Transfer-Encoding: chunked\r\n",
                    b"Content-Length: +2\r\n",
                    b"Content-Length: 2, 2\r\n",
                ):
                    with self.subTest(framing=framing), socket.create_connection(
                        server.server_address, timeout=2,
                    ) as connection:
                        connection.settimeout(2)
                        connection.sendall(headers + framing + b"\r\n{}")
                        response = connection.recv(4096).split(b"\r\n", 1)[0]
                        self.assertIn(b"400", response)
            finally:
                server.shutdown()
                server.server_close()
                worker.join(timeout=2)


if __name__ == "__main__":
    unittest.main()

class GatewayConcurrentQueryTests(unittest.TestCase):
    def test_worker_budget_rejects_invalid_values(self):
        with self.assertRaisesRegex(ValueError, "worker limit"):
            BoundedThreadingHTTPServer(("127.0.0.1", 0), make_handler(
                secret="test-secret" * 3, queries=Path("."), database=Path("."),
                binary="unused", max_rows=1, timeout=1), max_workers=0)

    def test_two_independent_queries_can_run_at_once(self):
        with tempfile.TemporaryDirectory() as root:
            directory = Path(root)
            (directory / "safe.sql").write_text("SELECT 1", encoding="utf-8")
            handler = make_handler(
                secret="test-secret" * 3, queries=directory, database=directory,
                binary="unused", max_rows=1, timeout=5,
            )
            server = BoundedThreadingHTTPServer(
                ("127.0.0.1", 0), handler, max_workers=2,
            )
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            first_started = threading.Event()
            both_started = threading.Event()
            release = threading.Event()
            running = 0
            lock = threading.Lock()

            def fake_backend(*args, **kwargs):
                nonlocal running
                with lock:
                    running += 1
                    if running == 1:
                        first_started.set()
                    if running == 2:
                        both_started.set()
                if not release.wait(5):
                    raise RuntimeError("SQL worker timed out in concurrency test")
                with lock:
                    running -= 1
                return subprocess.CompletedProcess(args[0], 0, "[]", "")

            results = []
            def request():
                connection = HTTPConnection("127.0.0.1", server.server_port, timeout=5)
                try:
                    connection.request("POST", "/v1/query/safe", body=b'{"params":{}}',
                        headers={"Authorization": "Bearer " + "test-secret" * 3,
                                 "Content-Type": "application/json"})
                    response = connection.getresponse()
                    results.append((response.status, response.read()))
                finally:
                    connection.close()

            thread.start()
            try:
                with patch("server.run_bounded_backend", side_effect=fake_backend):
                    clients = [threading.Thread(target=request) for _ in range(2)]
                    for client in clients:
                        client.start()
                    try:
                        self.assertTrue(first_started.wait(3))
                        self.assertTrue(both_started.wait(3),
                            "A serial HTTPServer blocks the second query")
                    finally:
                        release.set()
                        for client in clients:
                            client.join(timeout=5)
                self.assertEqual(results, [(200, b'{"rows":[]}')] * 2)
            finally:
                release.set()
                server.shutdown()
                server.server_close()
                thread.join(timeout=3)
