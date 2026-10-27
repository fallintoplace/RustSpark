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

    def test_creates_runner_command(self):
        args = matrix.run_command("rs", "queries", "data", 8, "report.json", 2, 1)
        self.assertEqual(args[args.index("--target-partitions") + 1], "8")
        self.assertEqual(args[args.index("--repeat") + 1], "2")
        self.assertIn("--json-out", args)


if __name__ == "__main__":
    unittest.main()
