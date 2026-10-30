"""Resource-bounded subprocess capture for local SQL integrations."""
from __future__ import annotations

import subprocess
import threading
import time

MAX_SQL_RESULT_BYTES = 8 * 1024 * 1024


def run_bounded(command: list[str], *, timeout: float,
                max_bytes: int = MAX_SQL_RESULT_BYTES) -> subprocess.CompletedProcess:
    """Kill a backend that exceeds its stdout budget or wall clock deadline."""
    if max_bytes <= 0 or timeout <= 0:
        raise ValueError("Invalid backend resource budget")
    output = bytearray()
    overflow = threading.Event()
    with subprocess.Popen(command, stdout=subprocess.PIPE,
                          stderr=subprocess.DEVNULL) as process:
        assert process.stdout is not None

        def collect():
            while True:
                chunk = process.stdout.read(65536)
                if not chunk:
                    return
                if len(output) + len(chunk) > max_bytes:
                    overflow.set()
                    try:
                        process.kill()
                    except ProcessLookupError:
                        pass
                    return
                output.extend(chunk)

        worker = threading.Thread(target=collect, daemon=True)
        deadline = time.monotonic() + timeout
        worker.start()
        worker.join(max(0, deadline - time.monotonic()))
        if worker.is_alive():
            try:
                process.kill()
            except ProcessLookupError:
                pass
            worker.join()
            raise subprocess.TimeoutExpired(command, timeout)
        try:
            process.wait(timeout=max(0, deadline - time.monotonic()))
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
            raise
        if overflow.is_set():
            raise ValueError("SQL backend output budget exceeded")
        return subprocess.CompletedProcess(
            command, process.returncode, output.decode("utf-8"), ""
        )
