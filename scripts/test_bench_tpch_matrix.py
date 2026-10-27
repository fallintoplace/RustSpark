"""Partition matrix input validation."""

import unittest
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

import bench_tpch_matrix as matrix


class MatrixTests(unittest.TestCase):
    def test_rejects_invalid_partition_specification(self):
        for value in ("", "0", "1,0", "1,1", "one", "1,,2"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                matrix.parse_partitions(value)

    def test_preserves_order(self):
        self.assertEqual(matrix.parse_partitions("8,2,1"), [8, 2, 1])

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

    def test_creates_runner_command(self):
        args = matrix.run_command("rs", "queries", "data", 8, "report.json", 2, 1)
        self.assertEqual(args[args.index("--target-partitions") + 1], "8")
        self.assertEqual(args[args.index("--repeat") + 1], "2")
        self.assertIn("--json-out", args)


if __name__ == "__main__":
    unittest.main()
