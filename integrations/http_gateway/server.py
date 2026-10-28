#!/usr/bin/env python3
"""Loopback-only, authenticated gateway for operator-owned RustSpark SQL files.

Experimental integration: not a public, multi-tenant SQL service.
"""
from __future__ import annotations

import argparse
import hmac
import json
import os
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
import re
import subprocess

IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
TEMPLATE = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9_-]{0,63}$")
MAX_REQUEST_BYTES = 64 * 1024


def validate_params(body: bytes) -> dict[str, str]:
    payload = json.loads(body)
    if not isinstance(payload, dict) or set(payload) != {"params"}:
        raise ValueError("Expected JSON object containing only params")
    params = payload["params"]
    if not isinstance(params, dict) or len(params) > 64:
        raise ValueError("params must be an object with at most 64 entries")
    for name, typed_value in params.items():
        if not isinstance(name, str) or not IDENTIFIER.fullmatch(name):
            raise ValueError("Invalid parameter name")
        if not isinstance(typed_value, str) or ":" not in typed_value or len(typed_value) > 8192:
            raise ValueError("Parameters must be typed strings such as int64:123")
    return params


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
                result = subprocess.run(
                    command_for_template(binary, database, template, params, max_rows, timeout),
                    capture_output=True, text=True, timeout=timeout + 10, check=False,
                )
                if result.returncode:
                    self.send_json(422, {"error": "Query failed"})
                    return
                rows = json.loads(result.stdout)
                if not isinstance(rows, list):
                    raise ValueError("Unexpected result")
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
