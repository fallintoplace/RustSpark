"""Reject non-finite cross-engine ratios before JSON report generation."""
import unittest
import bench_compare

class CrossEngineRatioTests(unittest.TestCase):
    def test_ratio_and_zero_denominator(self):
        self.assertEqual(bench_compare._median_ratio(20.0, 10.0), 2.0)
        self.assertIsNone(bench_compare._median_ratio(20.0, 0.0))

    def test_overflow_and_nonfinite_inputs_fail_closed(self):
        with self.assertRaisesRegex(ValueError, "overflow"):
            bench_compare._median_ratio(1e308, 5e-324)
        for value in (-1.0, float("inf"), float("nan"), True):
            with self.subTest(value=value), self.assertRaises(ValueError):
                bench_compare._median_ratio(value, 5.0)


if __name__ == "__main__":
    unittest.main()
