import unittest
import subprocess
import sys
from contextlib import nullcontext
from unittest.mock import patch
from pathlib import Path
from types import SimpleNamespace
from query_dataframe import build_command, query_dataframe


class DataFrameCommandTests(unittest.TestCase):
    def test_limits_result_rows(self):
        args = build_command("SELECT 1", Path("/tmp/x.arrow"), max_rows=25)
        self.assertIn("--output-arrow", args)
        self.assertEqual(args[args.index("--max-rows") + 1], "25")

    def test_refuses_unbounded_and_conflicting_options(self):
        with self.assertRaises(ValueError):
            build_command("SELECT 1", Path("r.arrow"), max_rows=0)
        with self.assertRaises(ValueError):
            build_command("SELECT 1", Path("r.arrow"), csv="a.csv", parquet="b.parquet")

    def test_process_timeout_is_validated_without_loading_arrow(self):
        for invalid in (0, -1, True, float("nan"), float("inf")):
            with self.subTest(invalid=invalid), self.assertRaisesRegex(ValueError, "process_timeout"):
                query_dataframe("SELECT 1", process_timeout=invalid)

    @patch("query_dataframe.subprocess.run")
    def test_process_timeout_covers_the_entire_export(self, run):
        mock_table = SimpleNamespace(to_pandas=lambda: "frame")
        fake_pa = SimpleNamespace(
            memory_map=lambda *args: nullcontext(None),
            ipc=SimpleNamespace(open_file=lambda source: SimpleNamespace(
                read_all=lambda: mock_table,
            )),
        )
        run.return_value = subprocess.CompletedProcess([], 0, "", "")
        with patch.dict(sys.modules, {"pyarrow": fake_pa}):
            self.assertEqual(query_dataframe("SELECT 1", process_timeout=2), "frame")
            self.assertEqual(run.call_args.kwargs["timeout"], 2)
            run.side_effect = subprocess.TimeoutExpired("rustspark", 2)
            with self.assertRaisesRegex(RuntimeError, "process timeout"):
                query_dataframe("SELECT 1", process_timeout=2)


if __name__ == "__main__":
    unittest.main()
