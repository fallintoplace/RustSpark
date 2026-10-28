"""Timing iterators should have the same validation as timing lists."""

import unittest

import bench_stats


class BenchmarkIteratorTests(unittest.TestCase):
    def test_empty_iterator_raises_expected_error(self):
        with self.assertRaisesRegex(ValueError, "at least one"):
            bench_stats.summarize_samples(iter(()))

    def test_generator_samples_are_supported(self):
        actual = bench_stats.summarize_samples(value for value in (1.0, 3.0, 2.0))
        expected = bench_stats.summarize_samples([1.0, 3.0, 2.0])
        self.assertEqual(actual, expected)


if __name__ == "__main__":
    unittest.main()
