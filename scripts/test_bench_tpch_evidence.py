"""Reject incomplete or misleading billion-row benchmark artifacts."""

import copy
import hashlib
import unittest

import bench_tpch_evidence as evidence


def digest(value):
    return hashlib.sha256(value.encode()).hexdigest()


def fixtures():
    query_hashes = {i: digest(f"SELECT {i};") for i in range(1, 23)}
    manifest = {
        "scale_factor": 167,
        "rows": {name: 100 for name in evidence.TABLES},
        "tables": {name: f"data/{name}.parquet" for name in evidence.TABLES},
        "table_sha256": {name: digest(name) for name in evidence.TABLES},
        "queries": [{"id": i, "sha256": query_hashes[i]} for i in range(1, 23)],
    }
    manifest["rows"]["lineitem"] = 1_000_000_100
    benchmark = {
        "binary_sha256": digest("binary"),
        "repeat": 3,
        "warmup": 1,
        "results": [{
            "id": i, "status": "ok", "sql_sha256": query_hashes[i],
            "result_sha256": digest(f"result:{i}"), "samples_ms": [float(i)] * 3,
            "median_ms": float(i), "p95_ms": float(i),
        } for i in range(1, 23)],
    }
    verification = {
        "results": [{
            "id": i, "status": "match", "sql_sha256": query_hashes[i],
            "expected_rows": 1, "actual_rows": 1,
        } for i in range(1, 23)],
    }
    return manifest, benchmark, verification


class EvidenceTests(unittest.TestCase):
    def test_valid_report_is_qualified_without_claiming_a_speedup(self):
        report = evidence.qualify(*fixtures())
        self.assertEqual(report["independent_duckdb_result_checks"], 22)
        self.assertEqual(report["samples_per_query"], 3)
        self.assertFalse(report["verified_fixture_file_hashes"])

    def test_rejects_small_or_incomplete_fixture(self):
        manifest, benchmark, verification = fixtures()
        manifest["rows"]["lineitem"] = 1000
        with self.assertRaisesRegex(ValueError, "one billion"):
            evidence.qualify(manifest, benchmark, verification)
        manifest, benchmark, verification = fixtures()
        manifest["queries"].pop()
        with self.assertRaisesRegex(ValueError, "22 unique"):
            evidence.qualify(manifest, benchmark, verification)

    def test_rejects_sql_mismatch_and_failed_reference_check(self):
        manifest, benchmark, verification = fixtures()
        benchmark["results"][0]["sql_sha256"] = digest("changed query")
        with self.assertRaisesRegex(ValueError, "differs"):
            evidence.qualify(manifest, benchmark, verification)
        manifest, benchmark, verification = fixtures()
        verification["results"][0]["status"] = "mismatch"
        with self.assertRaisesRegex(ValueError, "failed"):
            evidence.qualify(manifest, benchmark, verification)

    def test_rejects_missing_samples_and_bad_medians(self):
        manifest, benchmark, verification = fixtures()
        benchmark["results"][0]["samples_ms"] = [1.0]
        with self.assertRaisesRegex(ValueError, "samples"):
            evidence.qualify(manifest, benchmark, verification)
        manifest, benchmark, verification = fixtures()
        benchmark["results"][0]["median_ms"] = 9.0
        with self.assertRaisesRegex(ValueError, "inconsistent"):
            evidence.qualify(manifest, benchmark, verification)

    def test_rejects_duplicate_query_ids_and_false_rows(self):
        manifest, benchmark, verification = fixtures()
        benchmark["results"][0]["id"] = 2
        with self.assertRaisesRegex(ValueError, "unique"):
            evidence.qualify(manifest, benchmark, verification)
        manifest, benchmark, verification = fixtures()
        verification["results"][0]["actual_rows"] = 2
        with self.assertRaisesRegex(ValueError, "row counts"):
            evidence.qualify(manifest, benchmark, verification)


if __name__ == "__main__":
    unittest.main()
