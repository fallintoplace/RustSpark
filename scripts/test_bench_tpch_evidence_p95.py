"""The billion-row evidence gate must reject fabricated p95 timings."""

import unittest

import bench_tpch_evidence as evidence
from test_bench_tpch_evidence import fixtures


class EvidenceP95Tests(unittest.TestCase):
    def test_rejects_p95_not_supported_by_samples(self):
        manifest, benchmark, verification = fixtures()
        benchmark["results"][0]["p95_ms"] = 100.0
        with self.assertRaisesRegex(ValueError, "p95 is inconsistent"):
            evidence.qualify(manifest, benchmark, verification)

    def test_accepts_nearest_rank_p95(self):
        manifest, benchmark, verification = fixtures()
        benchmark["repeat"] = 4
        for row in benchmark["results"]:
            row["samples_ms"] = [row["median_ms"]] * 4
        benchmark["results"][0]["samples_ms"] = [1.0, 1.0, 1.0, 2.0]
        benchmark["results"][0]["p95_ms"] = 2.0
        evidence.qualify(manifest, benchmark, verification)


if __name__ == "__main__":
    unittest.main()
