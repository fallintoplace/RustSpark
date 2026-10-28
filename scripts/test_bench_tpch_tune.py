"""TPC-H tuning comparisons must preserve results across variants."""

from pathlib import Path
import tempfile
import unittest
from unittest import mock

import bench_tpch_tune as tune


class TuneTests(unittest.TestCase):
    def test_parse_variants_restricts_flags_and_duplicates(self):
        values = [{"name": "base", "partitions": 2},
                  {"name": "pushed", "partitions": 4,
                   "flags": ["--batch-size", "8192", "--parquet-pushdown-filters"]}]
        self.assertEqual(len(tune.parse_variants(values)), 2)
        bad = [
            [{"name": "base", "partitions": 1}],
            [{"name": "base", "partitions": 1}] * 2,
            [values[0], {"name": "unsafe", "partitions": 1, "flags": ["--sql", "DROP TABLE x"]}],
            [values[0], {"name": "bad", "partitions": 1, "flags": ["--batch-size", "0"]}],
            [values[0], {"name": "bad", "partitions": 1, "flags": ["--parquet-reorder-filters"]}],
        ]
        for input_ in bad:
            with self.subTest(input=input_), self.assertRaises(ValueError):
                tune.parse_variants(input_)

    def test_sweep_selects_fastest_only_with_full_query_coverage(self):
        with tempfile.TemporaryDirectory() as tmp:
            folder = Path(tmp)
            for table in __import__("tpch_run").TABLES:
                (folder / f"{table}.parquet").touch()
            sql = folder / "01.sql"
            sql.write_text("SELECT 42 AS result;")
            variants = tune.parse_variants([
                {"name": "base", "partitions": 1},
                {"name": "fast", "partitions": 2, "flags": ["--batch-size", "8192"]},
            ])
            def fake(command, repeat, warmup, timeout, ordered=False):
                return {"status": "ok", "result_sha256": "a" * 64,
                        "median_ms": 2.0 if "--batch-size" in command else 4.0}
            with mock.patch.object(tune, "measure", side_effect=fake):
                report = tune.run("rs", folder, [(1, sql)], variants)
            self.assertFalse(report["complete_22_queries"])
            self.assertIsNone(report["best_complete_variant"])
            self.assertEqual(report["configurations"][1]["total_median_ms"], 2.0)

    def test_rejects_invalid_fingerprints_and_nan_medians(self):
        with tempfile.TemporaryDirectory() as tmp:
            folder = Path(tmp)
            for table in __import__("tpch_run").TABLES:
                (folder / f"{table}.parquet").touch()
            sql = folder / "01.sql"
            sql.write_text("SELECT 42")
            variants = tune.parse_variants([
                {"name": "base", "partitions": 1},
                {"name": "candidate", "partitions": 2},
            ])
            for invalid in (
                {"status": "ok", "result_sha256": "not-a-hash", "median_ms": 1.0},
                {"status": "ok", "result_sha256": "a" * 64,
                 "median_ms": float("nan")},
            ):
                with self.subTest(invalid=invalid):
                    with mock.patch.object(tune, "measure", return_value=invalid):
                        with self.assertRaisesRegex(ValueError, "invalid .*"):
                            tune.run("rs", folder, [(1, sql)], variants)

    def test_mismatched_results_abort_tuning(self):
        with tempfile.TemporaryDirectory() as tmp:
            folder = Path(tmp)
            for table in __import__("tpch_run").TABLES:
                (folder / f"{table}.parquet").touch()
            sql = folder / "01.sql"
            sql.write_text("SELECT 42")
            variants = tune.parse_variants([
                {"name": "base", "partitions": 1},
                {"name": "changed", "partitions": 2},
            ])
            with mock.patch.object(tune, "measure", side_effect=[
                {"status": "ok", "result_sha256": "a" * 64, "median_ms": 2.0},
                {"status": "ok", "result_sha256": "b" * 64, "median_ms": 1.0},
            ]):
                with self.assertRaisesRegex(ValueError, "result changed"):
                    tune.run("rs", folder, [(1, sql)], variants)


if __name__ == "__main__":
    unittest.main()
