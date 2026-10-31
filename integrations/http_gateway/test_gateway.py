import json
import unittest
import tempfile
import subprocess
import sys
import os
import time
import socket
import threading
import urllib.request
from http.server import HTTPServer
from pathlib import Path
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


class BoundedGatewayResponsesTests(unittest.TestCase):
    def test_json_encoder_rejects_escaped_output_expansion(self):
        from server import encode_response_bounded
        self.assertEqual(encode_response_bounded({"rows": [{"value": "ok"}]}, 100),
                         b'{"rows":[{"value":"ok"}]}')
        with self.assertRaisesRegex(ValueError, "output budget"):
            encode_response_bounded({"rows": [{"text": "\\u0000" * 100}]}, 200)

    def test_gateway_rejects_oversized_escaped_success_responses(self):
        from unittest.mock import patch
        from urllib.error import HTTPError
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "safe.sql").write_text("SELECT 1", encoding="utf-8")
            server = HTTPServer(("127.0.0.1", 0), make_handler(
                secret="token-" * 5, queries=root, database=root,
                binary="rustspark", max_rows=10, timeout=1,
            ))
            worker = threading.Thread(target=server.serve_forever, daemon=True)
            worker.start()
            try:
                body = b'{"params":{}}'
                request = urllib.request.Request(
                    f"http://127.0.0.1:{server.server_port}/v1/query/safe",
                    data=body,
                    headers={"Authorization": "Bearer " + "token-" * 5,
                             "Content-Type": "application/json"},
                )
                with patch("server.run_bounded_backend",
                           return_value=subprocess.CompletedProcess(
                               [], 0, '[{"text":"' + "é" * 2_000_000 + '"}]', "",
                           )):
                    with self.assertRaises(HTTPError) as raised:
                        urllib.request.urlopen(request, timeout=5)
                    self.assertEqual(raised.exception.code, 502)
                    self.assertIn("output budget", raised.exception.read().decode())
            finally:
                server.shutdown()
                server.server_close()
                worker.join(timeout=2)


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
                    b"Content-Length: " + b"9" * 5000 + b"\r\n",
                ):
                    with self.subTest(framing=framing), socket.create_connection(
                        server.server_address, timeout=2,
                    ) as connection:
                        connection.settimeout(2)
                        connection.sendall(headers + framing + b"\r\n{}")
                        response = connection.recv(4096).split(b"\r\n", 1)[0]
                        self.assertTrue(b"400" in response or b"413" in response)
            finally:
                server.shutdown()
                server.server_close()
                worker.join(timeout=2)

class BoundedGatewayConcurrencyTests(unittest.TestCase):
    def test_slow_partial_body_does_not_block_health_and_times_out(self):
        with tempfile.TemporaryDirectory() as directory:
            queries = Path(directory)
            (queries / "safe.sql").write_text("SELECT 1", encoding="utf-8")
            handler = make_handler(
                secret="secret-token-" * 3, queries=queries, database=queries,
                binary="nonexistent-rustspark", max_rows=10, timeout=1,
                read_timeout=0.3,
            )
            server = BoundedThreadingHTTPServer(("127.0.0.1", 0), handler, max_clients=2)
            worker = threading.Thread(target=server.serve_forever, daemon=True)
            worker.start()
            try:
                with socket.create_connection(server.server_address, timeout=2) as connection:
                    connection.settimeout(2)
                    connection.sendall(
                        b"POST /v1/query/safe HTTP/1.1\r\nHost: localhost\r\n"
                        b"Authorization: Bearer " + b"secret-token-" * 3 +
                        b"\r\nContent-Type: application/json\r\n"
                        b"Content-Length: 100\r\n\r\n{}"
                    )
                    with urllib.request.urlopen(
                        f"http://127.0.0.1:{server.server_port}/healthz", timeout=2,
                    ) as response:
                        self.assertEqual(response.status, 200)
                    self.assertIn(b"408", connection.recv(4096).split(b"\r\n", 1)[0])
            finally:
                server.shutdown()
                server.server_close()
                worker.join(timeout=2)

    def test_connection_limit_returns_503_without_starting_backend(self):
        with tempfile.TemporaryDirectory() as directory:
            queries = Path(directory)
            (queries / "safe.sql").write_text("SELECT 1", encoding="utf-8")
            handler = make_handler(
                secret="secret-token-" * 3, queries=queries, database=queries,
                binary="nonexistent-rustspark", max_rows=10, timeout=1,
                read_timeout=1.0,
            )
            server = BoundedThreadingHTTPServer(("127.0.0.1", 0), handler, max_clients=1)
            worker = threading.Thread(target=server.serve_forever, daemon=True)
            worker.start()
            try:
                with socket.create_connection(server.server_address, timeout=2) as slow:
                    slow.sendall(b"POST /v1/query/safe HTTP/1.1\r\nHost: localhost\r\n")
                    # Wait until the slow request occupies the only worker.
                    time.sleep(0.1)
                    with socket.create_connection(server.server_address, timeout=2) as other:
                        other.settimeout(2)
                        other.sendall(b"GET /healthz HTTP/1.1\r\nHost: localhost\r\n\r\n")
                        self.assertIn(b"503", other.recv(4096).split(b"\r\n", 1)[0])
            finally:
                server.shutdown()
                server.server_close()
                worker.join(timeout=2)


if __name__ == "__main__":
    unittest.main()
