import importlib.util
import pathlib
import tempfile
import unittest
from unittest import mock

SOURCE = pathlib.Path(__file__).with_name("bench_rustspark.py")
spec = importlib.util.spec_from_file_location("bench_rustspark", SOURCE)
bench = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bench)


class BenchmarkHarnessTests(unittest.TestCase):
    def test_benchmark_row_is_repeatable(self):
        self.assertEqual(bench.benchmark_row(0), ("city_0", 0))
        self.assertEqual(bench.benchmark_row(16), ("city_0", 92))
        self.assertEqual(bench.benchmark_row(33), ("city_1", 21))

    def test_rows_and_deterministic_generation(self):
        with tempfile.TemporaryDirectory() as directory:
            first = pathlib.Path(directory) / "first.csv"
            second = pathlib.Path(directory) / "second.csv"
            bench.write_csv(first, 33)
            bench.write_csv(second, 33)
            self.assertEqual(first.read_bytes(), second.read_bytes())
            self.assertEqual(len(first.read_text().splitlines()), 34)
            self.assertIn("32,city_0,", first.read_text())

    def test_csv_generation_preserves_rows_across_cycle_and_buffer_boundaries(self):
        for count in (0, 1, 399, 400, 401, 4096, 4097, 8001):
            with tempfile.TemporaryDirectory() as directory:
                path = pathlib.Path(directory) / "cycle.csv"
                bench.write_csv(path, count)
                with path.open(encoding="utf-8") as stream:
                    self.assertEqual(next(stream), "id,city,amount\n")
                    actual = stream.readlines()
                self.assertEqual(len(actual), count)
                for index in (0, 15, 399, 400, 4095, 4096, count - 1):
                    if 0 <= index < count:
                        city, amount = bench.benchmark_row(index)
                        self.assertEqual(actual[index], f"{index},{city},{amount}\n")

    def test_percentile_small_sets(self):
        self.assertEqual(bench.percentile([2], 0.95), 2)
        self.assertEqual(bench.percentile([9, 1, 3, 7], 0.95), 9)
        self.assertEqual(bench.percentile([9, 1, 3, 7], 0.5), 3)
        with self.assertRaises(ValueError):
            bench.percentile([], 0.95)

    def test_signature_ignores_unordered_rows(self):
        a, rows = bench.output_signature('{"id":2}\n{"id":1}\n')
        b, _ = bench.output_signature('{"id":1}\n{"id":2}\n')
        self.assertEqual(a, b)
        self.assertEqual(rows, [{"id": 2}, {"id": 1}])

    def test_measure_workload_keeps_only_measured_runs(self):
        values = [(1.0, "same", []), (2.0, "same", []), (3.0, "same", [])]
        with mock.patch.object(bench, "execute", side_effect=values) as execute:
            result = bench.measure_workload(
                "unused", pathlib.Path("unused"), "group_by", "SELECT 1", "same", 2, 1, 20
            )
        self.assertEqual(execute.call_count, 3)
        self.assertEqual(result["samples_ms"], [2.0, 3.0])
        self.assertEqual(result["median_ms"], 2.5)
        self.assertEqual(result["p95_ms"], 3.0)
        self.assertEqual(result["result_sha256"], "same")

    def test_measure_workload_rejects_mismatched_output(self):
        values = [(1.0, "first", []), (2.0, "second", [])]
        with mock.patch.object(bench, "execute", side_effect=values):
            with self.assertRaisesRegex(RuntimeError, "Nondeterministic"):
                bench.measure_workload(
                    "unused", pathlib.Path("unused"), "group_by", "SELECT 1", "first", 2, 0, 20
                )

    def test_expected_answers_include_zero_matches(self):
        self.assertEqual(bench.expected_results(1)["filter_sum"], [{"total": None}])
        expected = bench.expected_results(4)
        self.assertEqual(expected["scan_count"], [{"n": 4}])
        self.assertEqual(expected["filter_sum"], [{"total": 74}])
        self.assertEqual(expected["aggregate_join"], [{"pairs": 4}])

    def test_rejects_incorrect_benchmark_results(self):
        with mock.patch.object(bench, "execute", return_value=(1.0, "wrong", [])):
            with self.assertRaisesRegex(RuntimeError, "Incorrect benchmark results"):
                bench.benchmark("unused", 4, 1, 0, 20)

    def test_invalid_bench_options_fail_before_execution(self):
        with self.assertRaises(ValueError):
            bench.benchmark("unused", 0, 1, 0, 20)


if __name__ == "__main__":
    unittest.main()
