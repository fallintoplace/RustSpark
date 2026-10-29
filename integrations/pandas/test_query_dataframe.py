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

    def test_supports_json_input_without_mixing_sources(self):
        args = build_command("SELECT * FROM events", Path("r.arrow"),
                             json_input="/tmp/events.json")
        self.assertEqual(args[args.index("--json") + 1], "/tmp/events.json")
        with self.assertRaises(ValueError):
            build_command("SELECT 1", Path("r.arrow"), csv="x.csv", json_input="x.json")


if __name__ == "__main__":
    unittest.main()
