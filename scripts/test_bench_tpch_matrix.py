"""Partition matrix input validation."""

import unittest
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

import bench_tpch_matrix as matrix


class MatrixTests(unittest.TestCase):
    def test_matrix_rejects_non_integer_timing_budgets(self):
        with TemporaryDirectory() as tmp:
            folder = Path(tmp) / "results"
            for field, invalid in (
                ("repeat", True), ("repeat", 1.5),
                ("warmup", False), ("warmup", "1"),
            ):
                with self.subTest(field=field, invalid=invalid):
                    options = {"repeat": 1, "warmup": 0, field: invalid}
                    with self.assertRaisesRegex(ValueError, "Repeat and warmup"):
                        matrix.matrix("rs", "queries", "data", folder, [1], **options)
                    self.assertFalse(folder.exists())

    def test_run_configuration_rejects_ambiguous_json_evidence(self):
        with TemporaryDirectory() as tmp:
            folder = Path(tmp)
            def run(command, **kwargs):
                output = Path(command[command.index("--json-out") + 1])
                output.write_text('{"results":[],"results":[{"id":1,"status":"ok"}]}')
                return mock.Mock(returncode=0)
            with mock.patch.object(matrix.subprocess, "run", side_effect=run):
                with self.assertRaisesRegex(ValueError, "Duplicate"):
                    matrix.run_configuration("rs", "queries", "data", folder,
                                             1, 1, 0, {})

    def test_rejects_oversized_medians_and_nonfinite_suite_total(self):
        for median in (10 ** 500, 1e308):
            with self.subTest(median=median), TemporaryDirectory() as tmp:
                folder = Path(tmp)
                def run(command, **kwargs):
                    output = Path(command[command.index("--json-out") + 1])
                    rows = [
                        {"id": n, "status": "ok", "median_ms": median,
                         "result_sha256": "a" * 64} for n in range(1, 23)
                    ]
                    output.write_text(json.dumps({"results": rows}))
                    return mock.Mock(returncode=0)
                with mock.patch.object(matrix.subprocess, "run", side_effect=run):
                    with self.assertRaisesRegex(ValueError, "median|total"):
                        matrix.run_configuration("rs", "queries", "data", folder,
                                                 1, 1, 0, {})

    def test_rejects_invalid_partition_specification(self):
        for value in ("", "0", "1,0", "1,1", "one", "1,,2"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                matrix.parse_partitions(value)

    def test_preserves_order(self):
        self.assertEqual(matrix.parse_partitions("8,2,1"), [8, 2, 1])

    def test_rejects_results_that_change_between_partition_counts(self):
        with TemporaryDirectory() as tmp:
            def execute(command, **kwargs):
                output = Path(command[command.index("--json-out") + 1])
                partition = int(command[command.index("--target-partitions") + 1])
                digest = "a" * 64 if partition == 1 else "b" * 64
                output.write_text(json.dumps({"results": [
                    {"id": 1, "status": "ok", "result_sha256": digest}
                ]}))
                return mock.Mock(returncode=0)
            with mock.patch.object(matrix.subprocess, "run", side_effect=execute):
                with self.assertRaisesRegex(ValueError, "changed answers"):
                    matrix.matrix("rs", "queries", "parquet", Path(tmp), [1, 2])

    def test_selects_fastest_complete_partition_configuration(self):
        with TemporaryDirectory() as tmp:
            folder = Path(tmp)
            def run(command, **kwargs):
                output = Path(command[command.index("--json-out") + 1])
                partitions = int(command[command.index("--target-partitions") + 1])
                rows = [{"id": n, "status": "ok", "median_ms": 4.0 / partitions,
                         "result_sha256": "a" * 64} for n in range(1, 23)]
                output.write_text(json.dumps({"results": rows}), encoding="utf-8")
                return mock.Mock(returncode=0)
            with mock.patch.object(matrix.subprocess, "run", side_effect=run):
                report = matrix.matrix("rs", folder, folder, folder, [1, 2], repeat=1, warmup=0)
            self.assertEqual(report["best_complete_partitions"], 2)
            self.assertEqual(report["configurations"][1]["sum_median_ms"], 44.0)

    def test_sql_fingerprints_must_match_across_partitions(self):
        known = {}
        matrix.verify_sql_fingerprints([
            {"id": 1, "status": "ok", "sql_sha256": "a" * 64},
        ], known)
        with self.assertRaisesRegex(ValueError, "changed SQL"):
            matrix.verify_sql_fingerprints([
                {"id": 1, "status": "ok", "sql_sha256": "b" * 64},
            ], known)
        with self.assertRaisesRegex(ValueError, "lost its SQL fingerprint"):
            matrix.verify_sql_fingerprints([{"id": 1, "status": "ok"}], known)

    def test_sql_fingerprint_presence_cannot_change_across_partitions(self):
        known = {}
        matrix.verify_sql_fingerprints([{"id": 1, "status": "ok"}], known)
        self.assertIn(1, known)
        self.assertIsNone(known[1])
        matrix.verify_sql_fingerprints([{"id": 1, "status": "ok"}], known)
        with self.assertRaisesRegex(ValueError, "gained an SQL fingerprint"):
            matrix.verify_sql_fingerprints([
                {"id": 1, "status": "ok", "sql_sha256": "a" * 64},
            ], known)

    def test_creates_runner_command(self):
        args = matrix.run_command("rs", "queries", "data", 8, "report.json", 2, 1)
        self.assertEqual(args[args.index("--target-partitions") + 1], "8")
        self.assertEqual(args[args.index("--repeat") + 1], "2")
        self.assertIn("--json-out", args)

    def test_rejects_non_hex_result_fingerprints(self):
        with TemporaryDirectory() as tmp:
            output = Path(tmp)
            def execute(command, **kwargs):
                report = Path(command[command.index("--json-out") + 1])
                report.write_text(json.dumps({"results": [
                    {"id": 1, "status": "ok", "result_sha256": "x" * 64}
                ]}))
                return mock.Mock(returncode=0)
            with mock.patch.object(matrix.subprocess, "run", side_effect=execute):
                with self.assertRaisesRegex(ValueError, "fingerprint"):
                    matrix.run_configuration("rs", "queries", "parquet", output,
                                             1, 1, 0, {})


if __name__ == "__main__":
    unittest.main()
