#!/usr/bin/env python3
"""Loopback-only, authenticated gateway for operator-owned RustSpark SQL files.

Experimental integration: not a public, multi-tenant SQL service.
"""
from __future__ import annotations

import argparse
import hmac
import json
import math
import os
import signal
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
import re
import subprocess
import threading
import time

IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
TEMPLATE = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9_-]{0,63}$")
MAX_REQUEST_BYTES = 64 * 1024
MAX_BACKEND_OUTPUT_BYTES = 8 * 1024 * 1024


def _reject_duplicate_json_fields(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate JSON request field")
        result[key] = value
    return result


def parse_result_rows(text: str) -> list[dict]:
    def finite_float(number):
        parsed = float(number)
        if not math.isfinite(parsed):
            raise ValueError("Nonfinite SQL result number")
        return parsed

    def reject_constant(value):
        raise ValueError(f"Nonfinite SQL result constant: {value}")

    try:
        rows = json.loads(text, object_pairs_hook=_reject_duplicate_json_fields,
                          parse_constant=reject_constant, parse_float=finite_float)
    except RecursionError as error:
        raise ValueError("SQL result exceeds JSON nesting limit") from error
    if not isinstance(rows, list) or not all(isinstance(row, dict) for row in rows):
        raise ValueError("SQL result must be a list of objects")
    return rows


def validate_params(body: bytes) -> dict[str, str]:
    try:
        payload = json.loads(body, object_pairs_hook=_reject_duplicate_json_fields)
    except RecursionError as error:
        raise ValueError("JSON request exceeds nesting limit") from error
    if not isinstance(payload, dict) or set(payload) != {"params"}:
        raise ValueError("Expected JSON object containing only params")
    params = payload["params"]
    if not isinstance(params, dict) or len(params) > 64:
        raise ValueError("params must be an object with at most 64 entries")
    for name, typed_value in params.items():
        if not isinstance(name, str) or not IDENTIFIER.fullmatch(name):
            raise ValueError("Invalid parameter name")
        if not isinstance(typed_value, str) or ":" not in typed_value or len(typed_value.encode("utf-8")) > 8192:
            raise ValueError("Parameters must be typed strings such as int64:123")
    return params


def list_query_templates(directory: Path) -> list[str]:
    return sorted(
        path.stem for path in directory.iterdir()
        if path.is_file() and not path.is_symlink()
        and path.suffix == ".sql" and TEMPLATE.fullmatch(path.stem)
    )


def command_for_template(binary: str, database: Path, template: Path,
                         params: dict[str, str], max_rows: int, timeout: int) -> list[str]:
    args = [
        binary, "--database-dir", str(database), "--sql-file", str(template),
        "--stdout-format", "json-array", "--max-rows", str(max_rows),
        "--query-timeout", str(timeout),
    ]
    for name, value in params.items():
        args.extend(["--named-param", f"{name}={value}"])
    return args





def _kill_backend_group(process: subprocess.Popen) -> None:
    """Kill inherited pipe holders as well as the immediate SQL process."""
    if os.name == "posix":
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
    else:
        try:
            process.kill()
        except ProcessLookupError:
            pass

def run_bounded_backend(command: list[str], timeout: float,
                        max_bytes: int = MAX_BACKEND_OUTPUT_BYTES) -> subprocess.CompletedProcess:
    """Drain stdout with a hard memory limit. Discard backend diagnostics."""
    if (type(timeout) not in (int, float) or not math.isfinite(timeout)
            or timeout <= 0 or type(max_bytes) is not int or max_bytes <= 0):
        raise ValueError("Invalid query execution budget")
    output = bytearray()
    exceeded = threading.Event()
    with subprocess.Popen(command, stdout=subprocess.PIPE,
                          stderr=subprocess.DEVNULL,
                          start_new_session=(os.name == "posix")) as process:
        assert process.stdout is not None

        def drain():
            while True:
                chunk = process.stdout.read(65536)
                if not chunk:
                    return
                if len(output) + len(chunk) > max_bytes:
                    exceeded.set()
                    _kill_backend_group(process)
                    return
                output.extend(chunk)

        thread = threading.Thread(target=drain, daemon=True)
        thread.start()
        deadline = time.monotonic() + timeout
        thread.join(max(0.0, deadline - time.monotonic()))
        if thread.is_alive():
            _kill_backend_group(process)
            thread.join()
            raise subprocess.TimeoutExpired(command, timeout)
        try:
            process.wait(timeout=max(0.0, deadline - time.monotonic()))
        except subprocess.TimeoutExpired:
            _kill_backend_group(process)
            process.wait()
            raise
        if exceeded.is_set():
            raise ValueError("SQL result exceeds backend output budget")
        return subprocess.CompletedProcess(
            command, process.returncode, output.decode("utf-8"), ""
        )

def make_handler(*, secret: str, queries: Path, database: Path,
                 binary: str, max_rows: int, timeout: int):
    class Handler(BaseHTTPRequestHandler):
        def send_json(self, status, payload):
            data = json.dumps(payload, separators=(",", ":")).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def do_GET(self):
            if self.path == "/healthz":
                self.send_json(200, {"status": "ok"})
            elif self.path == "/v1/queries":
                token = self.headers.get("Authorization", "")
                if not hmac.compare_digest(token, "Bearer " + secret):
                    self.send_json(401, {"error": "Unauthorized"})
                    return
                self.send_json(200, {"queries": list_query_templates(queries)})
            else:
                self.send_json(404, {"error": "Not found"})

        def do_POST(self):
            match = re.fullmatch(r"/v1/query/([A-Za-z0-9_-]+)", self.path)
            if not match or not TEMPLATE.fullmatch(match.group(1)):
                self.send_json(404, {"error": "Not found"})
                return
            token = self.headers.get("Authorization", "")
            if not hmac.compare_digest(token, "Bearer " + secret):
                self.send_json(401, {"error": "Unauthorized"})
                return
            try:
                length = int(self.headers.get("Content-Length", ""))
            except ValueError:
                self.send_json(411, {"error": "Content-Length required"})
                return
            if not 0 <= length <= MAX_REQUEST_BYTES:
                self.send_json(413, {"error": "Request too large"})
                return
            if self.headers.get_content_type() != "application/json":
                self.send_json(415, {"error": "application/json required"})
                return
            try:
                params = validate_params(self.rfile.read(length))
            except (ValueError, UnicodeDecodeError, json.JSONDecodeError):
                self.send_json(400, {"error": "Invalid request"})
                return
            template = queries / (match.group(1) + ".sql")
            if not template.is_file() or template.is_symlink():
                self.send_json(404, {"error": "Unknown query"})
                return
            try:
                result = run_bounded_backend(
                    command_for_template(binary, database, template, params, max_rows, timeout),
                    timeout + 10,
                )
                if result.returncode:
                    self.send_json(422, {"error": "Query failed"})
                    return
                rows = parse_result_rows(result.stdout)
                self.send_json(200, {"rows": rows})
            except subprocess.TimeoutExpired:
                self.send_json(504, {"error": "Query timeout"})
            except (OSError, ValueError, json.JSONDecodeError):
                self.send_json(502, {"error": "Query backend error"})

        def log_message(self, format, *args):
            # Do not log auth tokens, typed parameter values or SQL contents.
            pass
    return Handler


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database-dir", type=Path, required=True)
    parser.add_argument("--queries-dir", type=Path, required=True)
    parser.add_argument("--rustspark", default="rustspark")
    parser.add_argument("--port", type=int, default=8787)
    parser.add_argument("--max-rows", type=int, default=500)
    parser.add_argument("--query-timeout", type=int, default=20)
    args = parser.parse_args()
    secret = os.environ.get("RUSTSPARK_GATEWAY_TOKEN", "")
    if len(secret) < 24:
        parser.error("RUSTSPARK_GATEWAY_TOKEN must contain at least 24 characters")
    if not args.database_dir.is_dir() or not args.queries_dir.is_dir():
        parser.error("Database and queries directories must already exist")
    if not 1 <= args.port <= 65535 or args.max_rows < 1 or args.query_timeout < 1:
        parser.error("Invalid port, max rows, or query timeout")
    handler = make_handler(
        secret=secret, queries=args.queries_dir.resolve(),
        database=args.database_dir.resolve(), binary=args.rustspark,
        max_rows=args.max_rows, timeout=args.query_timeout,
    )
    # Intentionally loopback only; use TLS and a trusted reverse proxy for remote use.
    with HTTPServer(("127.0.0.1", args.port), handler) as server:
        print(f"RustSpark SQL template gateway listening on 127.0.0.1:{args.port}")
        server.serve_forever()


if __name__ == "__main__":
    main()
