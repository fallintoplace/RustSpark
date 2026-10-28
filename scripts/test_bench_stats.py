import unittest

import bench_stats


class BenchmarkSummaryTests(unittest.TestCase):
    def test_median_p95_preserve_repetition_and_use_nearest_rank(self):
        result = bench_stats.summarize_samples([9.99999, 1.0, 3.12349, 7.0])
        self.assertEqual(result, {
            "samples_ms": [10.0, 1.0, 3.123, 7.0],
            "median_ms": 5.062,
            "p95_ms": 10.0,
        })

    def test_single_sample_and_zero_are_valid(self):
        self.assertEqual(bench_stats.summarize_samples([0]), {
            "samples_ms": [0.0], "median_ms": 0.0, "p95_ms": 0.0,
        })

    def test_invalid_samples_fail_closed(self):
        for value in (-1, float("nan"), float("inf"), -float("inf"), True, "5"):
            with self.subTest(value=value):
                with self.assertRaisesRegex(ValueError, "Invalid benchmark duration"):
                    bench_stats.summarize_samples([1, value])
        with self.assertRaisesRegex(ValueError, "at least one"):
            bench_stats.summarize_samples([])

    def test_statistics_use_unrounded_samples(self):
        result = bench_stats.summarize_samples([0.0001, 0.0005])
        self.assertEqual(result["samples_ms"], [0.0, 0.001])
        self.assertEqual(result["median_ms"], 0.0)
        self.assertEqual(result["p95_ms"], 0.001)


if __name__ == "__main__":
    unittest.main()
