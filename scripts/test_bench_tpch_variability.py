"""Variation guard must distinguish stable timings from noisy samples."""

import unittest

import bench_tpch_variability as variation


def report(samples):
    from statistics import median
    return {"results": [{"id": 1, "status": "ok", "median_ms": median(samples),
                         "samples_ms": samples}]}


class VariabilityTests(unittest.TestCase):
    def test_stable_samples_pass(self):
        result = variation.inspect(report([10, 10, 10, 10]))
        self.assertEqual(result["unstable_queries"], [])
        self.assertEqual(result["results"][0]["coefficient_of_variation"], 0)

    def test_noisy_samples_flagged(self):
        result = variation.inspect(report([10, 20, 10, 20]))
        self.assertEqual(result["unstable_queries"], [1])
        self.assertGreater(result["results"][0]["coefficient_of_variation"], 0.3)

    def test_too_few_or_invalid_samples_rejected(self):
        for samples in ([1], [1, 2], [1, float("inf"), 2], [1, True, 2], [-1, 2, 3]):
            with self.subTest(samples=samples), self.assertRaises(ValueError):
                variation.inspect(report(samples))
        changed = report([1, 2, 3])
        changed["results"][0]["median_ms"] = 999
        with self.assertRaisesRegex(ValueError, "median"):
            variation.inspect(changed)

    def test_duplicate_query_ids_and_threshold_rejected(self):
        r = report([1, 2, 3])
        r["results"] *= 2
        with self.assertRaises(ValueError):
            variation.inspect(r)
        for value in (-1, float("nan"), True):
            with self.assertRaises(ValueError):
                variation.inspect(report([1, 1, 1]), value)


if __name__ == "__main__":
    unittest.main()
