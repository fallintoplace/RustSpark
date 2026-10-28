"""Tests for the paired JSON_VALID performance harness."""

import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

import bench_json_valid_perf as perf


class JsonValidPerfTests(unittest.TestCase):
    def test_expected_count_includes_partial_cycles(self):
        self.assertEqual(perf.expected_valid_count(1), 1)
        self.assertEqual(perf.expected_valid_count(6), 4)
        self.assertEqual(perf.expected_valid_count(14), 10)

    def test_fixture_is_deterministic_and_has_mixed_json(self):
        with tempfile.TemporaryDirectory() as directory:
            first = Path(directory) / "first.csv"
            second = Path(directory) / "second.csv"
            perf.write_fixture(first, 11)
            perf.write_fixture(second, 11)
            self.assertEqual(first.read_bytes(), second.read_bytes())
            self.assertIn(b"payload", first.read_bytes())
            self.assertGreater(first.stat().st_size, 500)

    def test_counts_mismatched_sql_output_as_error(self):
        output = mock.Mock(stdout='{"n": 100}\n')
        with mock.patch.object(perf.subprocess, "run", return_value=output):
            with self.assertRaisesRegex(ValueError, "Wrong JSON_VALID count"):
                perf.run_case("fake", Path("input.csv"), 5, 10)

    def test_paired_medians_and_speedup(self):
        with mock.patch.object(
            perf, "run_case",
            side_effect=lambda binary, csv_path, expected, timeout:
            100.0 if binary == "old" else 50.0,
        ) as measure:
            result = perf.compare("old", "new", rows=12, repeat=3, warmup=1)
        self.assertEqual(measure.call_count, 8)
        self.assertEqual(result["expected_valid_rows"], 8)
        self.assertEqual(result["baseline_median_ms"], 100.0)
        self.assertEqual(result["candidate_median_ms"], 50.0)
        self.assertEqual(result["speedup"], 2.0)
        self.assertEqual(result["latency_reduction_pct"], 50.0)
        self.assertIn("2.000x", perf.markdown(result))

    def test_bad_parameters_rejected(self):
        for rows, repeat, warmup, timeout in [
            (0, 1, 0, 10), (10, 0, 0, 10),
            (10, 1, -1, 10), (10, 1, 0, 0),
        ]:
            with self.assertRaises(ValueError):
                perf.compare("old", "new", rows, repeat, warmup, timeout)


if __name__ == "__main__":
    unittest.main()
