import unittest
import subprocess
import sys
from unittest.mock import patch
from pathlib import Path
from types import SimpleNamespace
from query_lazy import build_command, rustspark_lazy_frame


class PolarsCommandTests(unittest.TestCase):
    def test_generates_ipc_file_without_shell(self):
        command = build_command("SELECT 1", Path("/tmp/x y.arrow"), csv="/tmp/a b.csv")
        self.assertEqual(command[0], "rustspark")
        self.assertIn("/tmp/x y.arrow", command)
        self.assertIn("/tmp/a b.csv", command)
        self.assertIn("--output-arrow", command)

    def test_rejects_conflicting_sources(self):
        with self.assertRaises(ValueError):
            build_command("SELECT 1", Path("r.arrow"), csv="a.csv", parquet="b.parquet")

    def test_rejects_invalid_process_timeout_before_loading_polars(self):
        for invalid in (0, -1, True, float("nan"), float("inf")):
            with self.subTest(invalid=invalid), self.assertRaisesRegex(ValueError, "process_timeout"):
                with rustspark_lazy_frame("SELECT 1", process_timeout=invalid):
                    pass

    @patch("query_lazy.subprocess.run")
    def test_export_timeout_covers_full_subprocess(self, run):
        fake_pl = SimpleNamespace(scan_ipc=lambda file: "lazy frame")
        run.return_value = subprocess.CompletedProcess([], 0, "", "")
        with patch.dict(sys.modules, {"polars": fake_pl}):
            with rustspark_lazy_frame("SELECT 1", process_timeout=3) as frame:
                self.assertEqual(frame, "lazy frame")
            self.assertEqual(run.call_args.kwargs["timeout"], 3)
            run.side_effect = subprocess.TimeoutExpired("rustspark", 3)
            with self.assertRaisesRegex(RuntimeError, "process timeout"):
                with rustspark_lazy_frame("SELECT 1", process_timeout=3):
                    pass


if __name__ == "__main__":
    unittest.main()
