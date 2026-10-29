import unittest
import io
import subprocess
import sys
import types
from contextlib import nullcontext
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

    def test_stderr_does_not_use_a_pipe_during_arrow_iteration(self):
        class Process:
            stdout = io.BytesIO()
            def wait(self, timeout=None):
                return 1
            def poll(self):
                return 1

        def start(command, *, stdout, stderr):
            self.assertEqual(stdout, subprocess.PIPE)
            self.assertNotEqual(stderr, subprocess.PIPE)
            stderr.write(b"diagnostic: spilled stderr")
            stderr.flush()
            return Process()

        fake_pyarrow = types.ModuleType("pyarrow")
        fake_pyarrow.ipc = types.SimpleNamespace(open_stream=lambda _: nullcontext([]))
        with patch.dict(sys.modules, {"pyarrow": fake_pyarrow}), patch(
            "stream.subprocess.Popen", side_effect=start,
        ):
            with self.assertRaisesRegex(RuntimeError, "spilled stderr"):
                list(iter_arrow_batches("SELECT 1"))


if __name__ == "__main__":
    unittest.main()
