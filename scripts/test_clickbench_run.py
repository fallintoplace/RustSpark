import argparse
import importlib.util
import pathlib
import subprocess
import tempfile
import unittest
from unittest import mock

MODULE = pathlib.Path(__file__).with_name("clickbench_run.py")
spec = importlib.util.spec_from_file_location("clickbench_run", MODULE)
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)


class QueryRunnerTests(unittest.TestCase):
    def test_validates_queries_and_selects_original_query_ids(self):
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / "queries.sql"
            path.write_text("SELECT 1;\n\nSELECT 2;\n", encoding="utf-8")
            queries = runner.read_queries(path)
        self.assertEqual(runner.choose_queries(queries, "2,1"), [(2, "SELECT 2;"), (1, "SELECT 1;")])
        for only in ["0", "3", "x", "1,1", ""]:
            if only == "":
                continue
            with self.assertRaises(ValueError):
                runner.choose_queries(queries, only)

    def test_query_number_parser_preserves_order_and_rejects_duplicates(self):
        self.assertEqual(runner.parse_query_numbers("3, 1,2", 3), [3, 1, 2])
        for value in ["0", "4", "1,1", "1,", ",1", "1,x"]:
            with self.subTest(value=value), self.assertRaises(ValueError):
                runner.parse_query_numbers(value, 3)

    def test_rejects_unsafe_or_multiline_queries(self):
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / "queries.sql"
            for contents in ["", "DELETE FROM hits;", "SELECT 1", "SELECT 1;\nDROP TABLE hits;", "SELECT 1; DROP TABLE hits;"]:
                path.write_text(contents, encoding="utf-8")
                with self.assertRaises(ValueError):
                    runner.read_queries(path)

    def test_command_uses_parquet_view_and_unquoted_shell_arguments(self):
        args = runner.command("binary", pathlib.Path("hits.parquet"), "SELECT 1;")
        self.assertEqual(args[0], "binary")
        self.assertIn("--parquet-binary-as-string", args)
        self.assertIn("--sql-view", args)
        self.assertIn("FROM hits_raw", args[args.index("--sql-view") + 1])
        self.assertEqual(args[args.index("--sql") + 1], "SELECT 1;")

    def test_signature_preserves_multiplicity_but_ignores_row_order(self):
        one = '{"a":1}\n{"a":2}\n{"a":1}\n'
        two = '{"a":1}\n{"a":1}\n{"a":2}\n'
        three = '{"a":1}\n{"a":2}\n'
        self.assertEqual(runner.result_signature(one), runner.result_signature(two))
        self.assertNotEqual(runner.result_signature(one), runner.result_signature(three))
        with self.assertRaises(ValueError):
            runner.result_signature('1\n')

    def test_rejects_ambiguous_or_nonfinite_json_results(self):
        for value in ['{"x":1,"x":2}', '{"x":NaN}', '{"x":Infinity}']:
            with self.subTest(value=value), self.assertRaises(ValueError):
                runner.result_signature(value)

    def test_ordered_result_signatures_check_row_order(self):
        first = '{"n":1}\n{"n":2}\n'
        reversed_rows = '{"n":2}\n{"n":1}\n'
        self.assertEqual(runner.result_signature(first), runner.result_signature(reversed_rows))
        self.assertNotEqual(
            runner.result_signature(first, ordered=True),
            runner.result_signature(reversed_rows, ordered=True),
        )

    def test_order_by_detection_ignores_quoted_and_nested_keywords(self):
        self.assertTrue(runner.has_top_level_order_by("SELECT n FROM t ORDER /* comment */ BY n"))
        self.assertTrue(runner.has_top_level_order_by("SELECT n FROM t order by n DESC"))
        self.assertFalse(runner.has_top_level_order_by("SELECT 'ORDER BY' AS label"))
        self.assertFalse(runner.has_top_level_order_by(
            'SELECT n FROM (SELECT n FROM t ORDER BY n) AS nested'
        ))
        self.assertFalse(runner.has_top_level_order_by('SELECT "ORDER BY" FROM t'))

    def test_ordered_queries_reject_changed_row_order(self):
        first = subprocess.CompletedProcess([], 0, '{"n":1}\n{"n":2}\n', "")
        second = subprocess.CompletedProcess([], 0, '{"n":2}\n{"n":1}\n', "")
        with mock.patch.object(runner.subprocess, "run", side_effect=[first, second]):
            result = runner.measure_query(
                "bin", pathlib.Path("input"), 1, "SELECT n FROM hits ORDER BY n;", 2, 0, 10,
            )
        self.assertEqual(result["status"], "nondeterministic")

    def test_measure_query_collects_warmup_and_measured_samples(self):
        completed = subprocess.CompletedProcess([], 0, '{"n":1}\n', "")
        with mock.patch.object(runner.subprocess, "run", return_value=completed) as run:
            result = runner.measure_query("bin", pathlib.Path("data"), 7, "SELECT 1;", 2, 1, 20)
        self.assertEqual(result["status"], "executed")
        self.assertEqual(len(result["samples_ms"]), 2)
        self.assertEqual(result["result_rows"], 1)
        self.assertEqual(run.call_count, 3)
        self.assertTrue(all(item >= 0 for item in result["samples_ms"]))

    def test_error_and_timeout_are_recorded_without_false_success(self):
        failed = subprocess.CompletedProcess([], 2, "", "SQL failed")
        with mock.patch.object(runner.subprocess, "run", return_value=failed):
            result = runner.measure_query("bin", pathlib.Path("data"), 1, "SELECT 1;", 1, 0, 20)
        self.assertEqual(result["status"], "error")
        self.assertIn("SQL failed", result["error"])
        with mock.patch.object(
            runner.subprocess, "run", side_effect=subprocess.TimeoutExpired(["bin"], 1),
        ):
            result = runner.measure_query("bin", pathlib.Path("data"), 2, "SELECT 1;", 1, 0, 1)
        self.assertIn("Timed out", result["error"])

    def test_invalid_utf8_output_is_a_query_error(self):
        decode_error = UnicodeDecodeError("utf-8", bytes([0xff]), 0, 1, "invalid start byte")
        with mock.patch.object(runner.subprocess, "run", side_effect=decode_error):
            result = runner.measure_query("bin", pathlib.Path("data"), 4, "SELECT 1;", 1, 0, 10)
        self.assertEqual(result["status"], "error")
        self.assertIn("decode", result["error"])

    def test_continues_after_failed_query_and_writes_failure_report(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            source = root / "hits.parquet"
            source.write_bytes(b"test only")
            queries = root / "queries.sql"
            queries.write_text("SELECT 1;\nSELECT 2;\n", encoding="utf-8")
            def measure(binary, parquet, number, sql, repeat, warmup, timeout, engine_args):
                return {"id": number, "status": "executed" if number == 1 else "error"}
            with mock.patch.object(runner, "measure_query", side_effect=measure) as measured:
                report = runner.benchmark("bin", source, queries, 1, 0, 1)
            self.assertEqual(measured.call_count, 2)
            self.assertEqual(report["queries_attempted"], 2)
            self.assertEqual(report["queries_executed"], 1)

    def test_nondeterministic_result_is_visible(self):
        samples = [
            {"elapsed_ms": 1.0, "sha256": "one", "result_rows": 1},
            {"elapsed_ms": 2.0, "sha256": "two", "result_rows": 1},
        ]
        with mock.patch.object(runner, "execute", side_effect=samples):
            result = runner.measure_query("bin", pathlib.Path("data"), 1, "SELECT 1;", 2, 0, 10)
        self.assertEqual(result["status"], "nondeterministic")

    def test_cli_engine_options_are_forwarded_without_shell_interpolation(self):
        with tempfile.TemporaryDirectory() as directory:
            options = argparse.Namespace(
                batch_size=4096,
                target_partitions=8,
                memory_limit_mb=512,
                spill_dir=pathlib.Path(directory),
            )
            extra = runner.build_engine_args(options)
            args = runner.command("bin", pathlib.Path("input.parquet"), "SELECT 1;", extra)
            self.assertEqual(extra[:6], (
                "--batch-size", "4096", "--target-partitions", "8",
                "--memory-limit-mb", "512",
            ))
            self.assertEqual(args[-len(extra):], list(extra))
            self.assertEqual(args[args.index("--sql") + 1], "SELECT 1;")

    def test_rejects_invalid_engine_limits(self):
        options = argparse.Namespace(
            batch_size=0, target_partitions=None, memory_limit_mb=None, spill_dir=None,
        )
        with self.assertRaisesRegex(ValueError, "--batch-size"):
            runner.build_engine_args(options)
        options.batch_size = None
        options.target_partitions = -1
        with self.assertRaisesRegex(ValueError, "--target-partitions"):
            runner.build_engine_args(options)
        options.target_partitions = None
        options.spill_dir = pathlib.Path("/path/that/does/not/exist")
        with self.assertRaisesRegex(ValueError, "--spill-dir"):
            runner.build_engine_args(options)

    def test_report_retains_engine_configuration(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            data = root / "input.parquet"
            queries = root / "queries.sql"
            data.write_bytes(b"test-only")
            queries.write_text("SELECT 1;\n", encoding="utf-8")
            with mock.patch.object(
                runner, "measure_query",
                return_value={"id": 1, "status": "executed"},
            ) as measure:
                report = runner.benchmark(
                    "bin", data, queries, 1, 0, 5,
                    engine_args=("--target-partitions", "4"),
                )
            self.assertEqual(report["engine_args"], ["--target-partitions", "4"])
            self.assertEqual(measure.call_args.args[-1], ("--target-partitions", "4"))

    def test_rejects_invalid_run_options(self):
        with self.assertRaisesRegex(ValueError, "repeat"):
            runner.benchmark("bin", pathlib.Path("missing"), pathlib.Path("missing"), 0, 0, 1)
        with self.assertRaisesRegex(ValueError, "timeout"):
            runner.benchmark("bin", pathlib.Path("missing"), pathlib.Path("missing"), 1, 0, float("nan"))


if __name__ == "__main__":
    unittest.main()
