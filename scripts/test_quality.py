from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock

import quality


class QualityRunnerTests(unittest.TestCase):
    def test_all_python_test_modules_have_exactly_one_suite(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name in ("test_bench_foo.py", "test_tpch_run.py", "test_spark_compat.py"):
                (root / name).touch()
            with mock.patch.object(quality, "SCRIPTS", root):
                manifest = quality.test_manifest()
            self.assertEqual(manifest["bench"], ["test_bench_foo.py", "test_tpch_run.py"])
            self.assertEqual(manifest["compat"], ["test_spark_compat.py"])

    def test_unregistered_tests_fail_instead_of_disappearing_from_ci(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "test_unregistered.py").touch()
            with mock.patch.object(quality, "SCRIPTS", root):
                with self.assertRaisesRegex(ValueError, "missing=.*test_unregistered"):
                    quality.test_manifest()

    def test_double_assigned_tests_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "test_bench_stats.py").touch()
            with mock.patch.object(quality, "SCRIPTS", root), mock.patch.object(
                quality, "TEST_PATTERNS", {"bench": ("test_bench_*.py",),
                                          "compat": ("test_bench_stats.py",)}
            ):
                with self.assertRaisesRegex(ValueError, "duplicate=.*test_bench_stats"):
                    quality.test_manifest()

    def test_bench_passes_arguments_and_propagates_failure(self):
        with mock.patch.object(quality.subprocess, "call", return_value=7) as call:
            self.assertEqual(quality.main([
                "bench", "cold", "--rows", "100", "--engines", "rustspark", "duckdb",
            ]), 7)
        self.assertEqual(call.call_args.args[0], [
            sys.executable, str(quality.SCRIPTS / "bench_compare.py"),
            "--rows", "100", "--engines", "rustspark", "duckdb",
        ])

    def test_billion_tpch_benchmark_is_available_through_quality_cli(self):
        with mock.patch.object(quality.subprocess, "call", return_value=0) as call:
            self.assertEqual(quality.main([
                "bench", "billion-tpch", "--fixture", "/data/tpch-sf167",
                "--repeat", "3", "--target-partitions", "16",
            ]), 0)
        self.assertEqual(call.call_args.args[0], [
            sys.executable, str(quality.SCRIPTS / "bench_billion_tpch.py"),
            "--fixture", "/data/tpch-sf167",
            "--repeat", "3", "--target-partitions", "16",
        ])

    def test_bench_supports_separator_for_help_forwarding(self):
        with mock.patch.object(quality.subprocess, "call", return_value=0) as call:
            self.assertEqual(quality.main(["bench", "local", "--", "--help"]), 0)
        self.assertEqual(call.call_args.args[0][-1], "--help")

    def test_suite_selection_and_listing(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "test_bench_fake.py").write_text(
                "import unittest\nclass TestExample(unittest.TestCase):\n"
                "    def test_good(self): self.assertEqual(2 + 2, 4)\n", encoding="utf-8")
            with mock.patch.object(quality, "SCRIPTS", root):
                with redirect_stdout(StringIO()) as output:
                    self.assertEqual(quality.run_tests("bench", list_only=True), 0)
                self.assertEqual(output.getvalue().strip(), "test_bench_fake.py")
                self.assertEqual(quality.run_tests("bench"), 0)

    def test_test_unknown_flags_are_not_silently_ignored(self):
        with self.assertRaises(SystemExit) as error, redirect_stdout(StringIO()):
            quality.main(["test", "--suite", "bench", "--typ0"])
        self.assertEqual(error.exception.code, 2)


if __name__ == "__main__":
    unittest.main()
