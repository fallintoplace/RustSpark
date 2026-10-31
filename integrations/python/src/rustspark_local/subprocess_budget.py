"""Bounded process I/O for the local RustSpark client."""
from __future__ import annotations

import os
import signal
import subprocess
import threading
import time

MAX_STDERR_BYTES = 64 * 1024


def _kill_process_group(process: subprocess.Popen) -> None:
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


def run_bounded(
    command: list[str], *, timeout: float | None,
    max_output_bytes: int,
    max_error_bytes: int = MAX_STDERR_BYTES,
) -> subprocess.CompletedProcess[str]:
    """Stream both output pipes with caps, including on queries that fail."""
    if max_output_bytes <= 0 or max_error_bytes <= 0:
        raise ValueError("invalid process output limit")
    stdout = bytearray()
    stderr = bytearray()
    exceeded = threading.Event()
    process = subprocess.Popen(
        command, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        start_new_session=(os.name == "posix"),
    )
    workers = []
    try:
        assert process.stdout is not None and process.stderr is not None

        def collect(pipe, target: bytearray, limit: int):
            while True:
                chunk = pipe.read(65536)
                if not chunk:
                    return
                if len(target) + len(chunk) > limit:
                    exceeded.set()
                    _kill_process_group(process)
                    return
                target.extend(chunk)

        workers = [
            threading.Thread(target=collect, args=(process.stdout, stdout, max_output_bytes),
                             daemon=True),
            threading.Thread(target=collect, args=(process.stderr, stderr, max_error_bytes),
                             daemon=True),
        ]
        for worker in workers:
            worker.start()
        deadline = time.monotonic() + timeout if timeout is not None else None
        for worker in workers:
            remaining = None if deadline is None else max(0.0, deadline - time.monotonic())
            worker.join(remaining)
            if worker.is_alive():
                _kill_process_group(process)
                for unfinished in workers:
                    unfinished.join()
                raise subprocess.TimeoutExpired(command, timeout)
        try:
            remaining = None if deadline is None else max(0.0, deadline - time.monotonic())
            process.wait(timeout=remaining)
        except subprocess.TimeoutExpired:
            _kill_process_group(process)
            process.wait()
            raise
        if exceeded.is_set():
            raise ValueError("SQL backend stdout or stderr byte limit exceeded")
        try:
            output = stdout.decode("utf-8")
            errors = stderr.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ValueError("SQL backend returned invalid UTF-8") from exc
        return subprocess.CompletedProcess(command, process.returncode, output, errors)
    finally:
        # Popen.__exit__ waits for the child without killing it. An interrupt
        # while collecting pipes must stop the entire process group first.
        if process.poll() is None:
            _kill_process_group(process)
        for pipe in (process.stdout, process.stderr):
            if pipe is not None:
                pipe.close()
        process.wait()
        for worker in workers:
            if worker.is_alive():
                worker.join(timeout=1)
