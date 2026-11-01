"""Reject incomplete or misleading billion-row benchmark artifacts."""

import hashlib
from pathlib import Path
import tempfile
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
        "numeric_tolerance": "0.000001",
        "results": [{
            "id": i, "status": "match", "sql_sha256": query_hashes[i],
            "expected_rows": 1, "actual_rows": 1,
        } for i in range(1, 23)],
    }
    return manifest, benchmark, verification


class EvidenceTests(unittest.TestCase):
    def test_evidence_loader_rejects_duplicate_keys_and_nonfinite_numbers(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "evidence.json"
            for payload in ('{"rows": {}, "rows": {"lineitem": 1}}',
                            '{"samples_ms": [1e999]}',
                            '{"samples_ms": [NaN]}'):
                path.write_text(payload, encoding="utf-8")
                with self.subTest(payload=payload), self.assertRaises(ValueError):
                    evidence.load_evidence(path)
            path.write_text('{"value": 1.5}', encoding="utf-8")
            self.assertEqual(evidence.load_evidence(path), {"value": 1.5})

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

    def test_rejects_loose_or_missing_reference_tolerance(self):
        for value in ("0.1", "NaN", "-0.001", None):
            with self.subTest(value=value):
                manifest, benchmark, verification = fixtures()
                verification["numeric_tolerance"] = value
                with self.assertRaisesRegex(ValueError, "tolerance"):
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

    def test_large_medians_cannot_hide_absolute_errors(self):
        manifest, benchmark, verification = fixtures()
        entry = benchmark["results"][0]
        entry["samples_ms"] = [1e12] * 3
        entry["median_ms"] = 1e12 - 1
        entry["p95_ms"] = 1e12
        with self.assertRaisesRegex(ValueError, "median is inconsistent"):
            evidence.qualify(manifest, benchmark, verification)

    def test_unrepresentable_timing_values_fail_with_validation_errors(self):
        with self.assertRaisesRegex(ValueError, "positive finite"):
            evidence.checked_number(10**500, "Timing")
        manifest, benchmark, verification = fixtures()
        for row in benchmark["results"]:
            row["samples_ms"] = [1e308] * 3
            row["median_ms"] = 1e308
            row["p95_ms"] = 1e308
        with self.assertRaisesRegex(ValueError, "median total"):
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
