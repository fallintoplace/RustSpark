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
    def test_rows_and_deterministic_generation(self):
        with tempfile.TemporaryDirectory() as directory:
            first = pathlib.Path(directory) / "first.csv"
            second = pathlib.Path(directory) / "second.csv"
            bench.write_csv(first, 33)
            bench.write_csv(second, 33)
            self.assertEqual(first.read_bytes(), second.read_bytes())
            self.assertEqual(len(first.read_text().splitlines()), 34)
            self.assertIn("32,city_0,", first.read_text())

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
