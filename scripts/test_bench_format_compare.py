"""Cross-format commands should preserve equivalent query semantics."""

import unittest

import bench_format_compare as comparison


class FormatTests(unittest.TestCase):
    def test_csv_command_explicit_schema(self):
        command = comparison.command("rs", "csv", "data.csv", "SELECT 1")
        self.assertIn("--csv-schema", command)
        self.assertEqual(command[command.index("--sql") + 1], "SELECT 1")
        self.assertIn("--stdout-format", command)

    def test_parquet_command(self):
        command = comparison.command("rs", "parquet", "data.parquet", "SELECT 1")
        self.assertIn("--parquet", command)
        self.assertNotIn("--csv-schema", command)

    def test_unknown_input_rejected(self):
        with self.assertRaisesRegex(ValueError, "Unknown"):
            comparison.command("rs", "pickle", "file", "SELECT 1")


if __name__ == "__main__":
    unittest.main()
