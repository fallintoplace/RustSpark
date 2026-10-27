"""Synthetic star joins should use deterministic inputs and expected results."""

from pathlib import Path
import tempfile
import unittest

import bench_star_schema as bench


class StarSchemaTests(unittest.TestCase):
    def test_answers_are_exact(self):
        expected = bench.answers(10, 3, 2)
        self.assertEqual(expected["fact_product_count"], [{"n": 10}])
        self.assertEqual(expected["three_table_sum"],
                         [{"total": sum(index * 17 % 100 for index in range(10))}])
        self.assertEqual(expected["filtered_join"], [{"n": sum(index * 17 % 100 >= 50 for index in range(10))}])
        self.assertEqual(
            sum(row["total"] for row in expected["region_revenue"]),
            expected["three_table_sum"][0]["total"],
        )

    def test_generates_three_named_csv_tables(self):
        with tempfile.TemporaryDirectory() as tmp:
            files = bench.fixtures(Path(tmp), 10, 3, 2)
            self.assertEqual(set(files), {"facts", "products", "regions"})
            self.assertEqual(len(files["facts"].read_text().splitlines()), 11)
            self.assertEqual(len(files["regions"].read_text().splitlines()), 3)
            command = bench.command("rs", files, "SELECT 1")
            self.assertEqual(command.count("--csv-table"), 3)
            self.assertIn("--csv-schema", command)
            self.assertEqual(command[command.index("--sql") + 1], "SELECT 1")

    def test_rejects_invalid_dimensions(self):
        for options in [(0, 1, 1), (10, 0, 2), (10, 2, -1)]:
            with self.subTest(options=options), self.assertRaises(ValueError):
                bench.answers(*options)


if __name__ == "__main__":
    unittest.main()
