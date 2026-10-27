"""Partition matrix input validation."""

import unittest

import bench_tpch_matrix as matrix


class MatrixTests(unittest.TestCase):
    def test_rejects_invalid_partition_specification(self):
        for value in ("", "0", "1,0", "1,1", "one", "1,,2"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                matrix.parse_partitions(value)

    def test_preserves_order(self):
        self.assertEqual(matrix.parse_partitions("8,2,1"), [8, 2, 1])

    def test_creates_runner_command(self):
        args = matrix.run_command("rs", "queries", "data", 8, "report.json", 2, 1)
        self.assertEqual(args[args.index("--target-partitions") + 1], "8")
        self.assertEqual(args[args.index("--repeat") + 1], "2")
        self.assertIn("--json-out", args)


if __name__ == "__main__":
    unittest.main()
