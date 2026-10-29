import unittest
from pathlib import Path
from query_dataframe import build_command


class DataFrameCommandTests(unittest.TestCase):
    def test_limits_result_rows(self):
        args = build_command("SELECT 1", Path("/tmp/x.arrow"), max_rows=25)
        self.assertIn("--output-arrow", args)
        self.assertEqual(args[args.index("--max-rows") + 1], "25")

    def test_refuses_unbounded_and_conflicting_options(self):
        with self.assertRaises(ValueError):
            build_command("SELECT 1", Path("r.arrow"), max_rows=0)
        with self.assertRaises(ValueError):
            build_command("SELECT 1", Path("r.arrow"), csv="a.csv", parquet="b.parquet")

    def test_persistent_database_path_is_forwarded(self):
        args = build_command("SELECT * FROM sales", Path("result.arrow"),
                             database_dir="/tmp/warehouse with spaces")
        self.assertEqual(args[args.index("--database-dir") + 1],
                         "/tmp/warehouse with spaces")
        self.assertNotIn("--csv", args)
        self.assertIn("--max-rows", args)


if __name__ == "__main__":
    unittest.main()
