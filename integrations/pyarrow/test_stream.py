import unittest
import io
import sys
from contextlib import nullcontext
from types import SimpleNamespace
from unittest.mock import patch
from pathlib import Path
from stream import build_command, iter_arrow_batches


class ArrowStreamCommandTests(unittest.TestCase):
    def test_command_uses_arrow_ipc_and_literal_file_paths(self):
        args = build_command("SELECT * FROM data", csv=Path("/tmp/a b.csv"), max_rows=50)
        self.assertEqual(args[:4], ["rustspark", "--sql", "SELECT * FROM data", "--stdout-format"])
        self.assertIn("arrow", args)
        self.assertIn("/tmp/a b.csv", args)
        self.assertIn("50", args)

    def test_rejects_conflicting_sources(self):
        with self.assertRaises(ValueError):
            build_command("SELECT 1", csv="a.csv", parquet="b.parquet")
        with self.assertRaises(ValueError):
            build_command(" ", csv="a.csv")
        with self.assertRaises(ValueError):
            build_command("SELECT 1", query_timeout=0)

    def test_nonzero_exit_reads_bounded_spooled_stderr(self):
        class FakeProcess:
            def __init__(self):
                self.stdout = io.BytesIO()
            def poll(self):
                return 2
            def wait(self, timeout=None):
                return 2
        def start(command, *, stdout, stderr):
            self.assertIsNotNone(stdout)
            stderr.write(b"query failed without blocking the Arrow pipe")
            stderr.flush()
            return FakeProcess()
        arrow = SimpleNamespace(ipc=SimpleNamespace(open_stream=lambda stream: nullcontext([])))
        with patch.dict(sys.modules, {"pyarrow": arrow}), patch("stream.subprocess.Popen", side_effect=start):
            with self.assertRaisesRegex(RuntimeError, "query failed"):
                list(iter_arrow_batches("SELECT 1"))


if __name__ == "__main__":
    unittest.main()
