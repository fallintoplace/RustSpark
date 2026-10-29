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

    def test_typed_parameters_are_passed_without_sql_interpolation(self):
        sql = "SELECT * FROM data WHERE total > $minimum"
        args = build_command(sql, Path("out.arrow"),
                             named_params={"minimum": "int64:10"})
        self.assertEqual(args[args.index("--sql") + 1], sql)
        self.assertIn("minimum=int64:10", args)
        with self.assertRaises(ValueError):
            build_command(sql, Path("out.arrow"), named_params={"bad-name": "int64:10"})
        with self.assertRaises(ValueError):
            build_command(sql, Path("out.arrow"), named_params={"minimum": "10"})


if __name__ == "__main__":
    unittest.main()
