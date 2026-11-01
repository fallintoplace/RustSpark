"""Resource-bounded subprocess capture for local SQL integrations."""
from __future__ import annotations

import subprocess
import os
import signal
import threading
import time
import math

MAX_SQL_RESULT_BYTES = 8 * 1024 * 1024




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

def run_bounded(command: list[str], *, timeout: float,
                max_bytes: int = MAX_SQL_RESULT_BYTES) -> subprocess.CompletedProcess:
    """Kill a backend that exceeds its stdout budget or wall clock deadline."""
    if (type(max_bytes) is not int or max_bytes <= 0
            or type(timeout) not in (int, float)
            or not math.isfinite(timeout) or timeout <= 0):
        raise ValueError("Invalid backend resource budget")
    output = bytearray()
    overflow = threading.Event()
    process = subprocess.Popen(
        command, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
        start_new_session=(os.name == "posix"),
    )
    worker = None
    try:
        assert process.stdout is not None

        def collect():
            while True:
                chunk = process.stdout.read(65536)
                if not chunk:
                    return
                if len(output) + len(chunk) > max_bytes:
                    overflow.set()
                    _kill_backend_group(process)
                    return
                output.extend(chunk)

        worker = threading.Thread(target=collect, daemon=True)
        deadline = time.monotonic() + timeout
        worker.start()
        worker.join(max(0, deadline - time.monotonic()))
        if worker.is_alive():
            _kill_backend_group(process)
            worker.join()
            raise subprocess.TimeoutExpired(command, timeout)
        try:
            process.wait(timeout=max(0, deadline - time.monotonic()))
        except subprocess.TimeoutExpired:
            _kill_backend_group(process)
            process.wait()
            raise
        if overflow.is_set():
            raise ValueError("SQL backend output budget exceeded")
        try:
            stdout = output.decode("utf-8")
        except UnicodeDecodeError as error:
            raise ValueError("SQL backend output is not valid UTF-8") from error
        return subprocess.CompletedProcess(
            command, process.returncode, stdout, ""
        )
    finally:
        # Popen's context manager waits but does not kill the child on an
        # interrupt. Always stop descendants before joining the pipe worker.
        _kill_backend_group(process)
        process.wait()
        if worker is not None and worker.is_alive():
            worker.join(timeout=1)
        if process.stdout is not None and (worker is None or not worker.is_alive()):
            process.stdout.close()
