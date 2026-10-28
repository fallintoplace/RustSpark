"""Regression tests for billion-row benchmark preflight and SQL selection."""

import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import bench_billion_rows


class BillionRowBenchTests(unittest.TestCase):
    def fixture(self, directory, rows):
        root = Path(directory)
        (root / "data").mkdir()
        for table in ("customer", "lineitem", "nation", "orders",
                      "part", "partsupp", "region", "supplier"):
            (root / "data" / f"{table}.parquet").write_bytes(b"PAR1")
        (root / "manifest.json").write_text(
            json.dumps({"rows": {"lineitem": rows}}), encoding="utf-8"
        )

    def test_rejects_small_or_invalid_row_counts(self):
        with tempfile.TemporaryDirectory() as directory:
            self.fixture(directory, 999_999_999)
            with self.assertRaisesRegex(ValueError, "lineitem"):
                bench_billion_rows.validate_fixture(directory, 1_000_000_000)
            self.fixture_again = Path(directory) / "manifest.json"
            self.fixture_again.write_text(
                json.dumps({"rows": {"lineitem": True}}), encoding="utf-8"
            )
            with self.assertRaises(ValueError):
                bench_billion_rows.validate_fixture(directory, 1)

    def test_requires_all_eight_parquet_inputs(self):
        with tempfile.TemporaryDirectory() as directory:
            self.fixture(directory, 1_000_000_001)
            (Path(directory) / "data" / "orders.parquet").unlink()
            with self.assertRaisesRegex(ValueError, "orders"):
                bench_billion_rows.validate_fixture(directory, 1_000_000_000)

    def test_measures_full_column_scans_without_metadata_only_count(self):
        with tempfile.TemporaryDirectory() as directory:
            self.fixture(directory, 1_000_000_001)
            def fake_measure(command, repeat, warmup, timeout, ordered=False):
                sql = Path(command[command.index("--sql-file") + 1]).read_text()
                self.assertIn("lineitem", sql)
                self.assertNotIn("COUNT(", sql.upper())
                return {"status": "ok", "samples_ms": [1], "median_ms": 1,
                        "p95_ms": 1, "result_sha256": "sha", "result_rows": 1}
            with patch.object(bench_billion_rows, "measure", fake_measure):
                result = bench_billion_rows.benchmark(
                    "rustspark", directory, 1_000_000_000, 1, 0, 10, 2
                )
            self.assertEqual(len(result["results"]), 3)
            self.assertTrue(all(row["status"] == "ok" for row in result["results"]))

    def test_invalid_benchmark_timing_rejected(self):
        with self.assertRaises(ValueError):
            bench_billion_rows.benchmark("rustspark", "/missing", 1, 0, 0, 10, 2)


if __name__ == "__main__":
    unittest.main()
