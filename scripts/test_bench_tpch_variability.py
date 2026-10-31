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

    def test_reported_p95_must_match_samples(self):
        data = report([1, 1, 2, 4])
        data["results"][0]["p95_ms"] = 4.0
        self.assertEqual(variation.inspect(data)["unstable_queries"], [1])
        for invalid in (3.0, float("inf"), True):
            with self.subTest(invalid=invalid):
                data["results"][0]["p95_ms"] = invalid
                with self.assertRaisesRegex(ValueError, "p95"):
                    variation.inspect(data)

    def test_extreme_finite_samples_do_not_overflow_mean_or_stddev(self):
        stable = variation.inspect(report([1e308, 1e308, 1e308]))
        self.assertEqual(stable["results"][0]["coefficient_of_variation"], 0)
        self.assertEqual(stable["unstable_queries"], [])
        noisy = variation.inspect(report([5e307, 7.5e307, 1e308]))
        self.assertGreater(noisy["results"][0]["coefficient_of_variation"], 0.25)
        self.assertEqual(noisy["unstable_queries"], [1])

    def test_even_count_large_finite_samples_keep_finite_median(self):
        samples = [1e308] * 4
        data = {"results": [{
            "id": 1, "status": "ok",
            "median_ms": 1e308, "samples_ms": samples,
            "p95_ms": 1e308,
        }]}
        result = variation.inspect(data)
        self.assertEqual(result["unstable_queries"], [])
        self.assertEqual(result["results"][0]["median_absolute_deviation_ms"], 0)

    def test_noisy_samples_flagged(self):
        result = variation.inspect(report([10, 20, 10, 20]))
        self.assertEqual(result["unstable_queries"], [1])
        self.assertGreater(result["results"][0]["coefficient_of_variation"], 0.3)

    def test_too_few_or_invalid_samples_rejected(self):
        for samples in ([1], [1, 2], [1, float("inf"), 2], [1, True, 2], [-1, 2, 3], [0, 0, 0], [0, 2, 3]):
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

    def test_optional_robust_noise_threshold(self):
        sample = report([10, 10, 10, 12, 12, 12])
        permissive = variation.inspect(sample, threshold=1.0)
        self.assertEqual(permissive["unstable_queries"], [])
        constrained = variation.inspect(sample, threshold=1.0, max_relative_mad=0.05)
        self.assertEqual(constrained["unstable_queries"], [1])
        self.assertGreater(constrained["results"][0]["relative_median_absolute_deviation"], 0.05)
        for invalid in (True, -1, float("nan")):
            with self.subTest(invalid=invalid), self.assertRaisesRegex(ValueError, "Relative MAD"):
                variation.inspect(sample, max_relative_mad=invalid)


if __name__ == "__main__":
    unittest.main()
