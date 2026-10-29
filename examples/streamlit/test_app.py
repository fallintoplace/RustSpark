import csv
import io
import json
import subprocess
import unittest
from unittest.mock import patch

from app import MAX_UPLOAD_BYTES, as_csv, as_json, query_csv


class StreamlitExampleTests(unittest.TestCase):
    @patch("app.subprocess.run")
    def test_query_uses_a_temporary_csv_and_bounded_json_output(self, run):
        run.return_value = subprocess.CompletedProcess([], 0, '[{"city":"Berlin"}]', "")
        rows = query_csv("rustspark", b"city\nBerlin\n", "SELECT * FROM data", 10)
        self.assertEqual(rows, [{"city": "Berlin"}])
        args = run.call_args.args[0]
        self.assertEqual(args[0], "rustspark")
        self.assertIn("--max-rows", args)
        self.assertEqual(args[args.index("--max-rows") + 1], "10")

    def test_upload_size_and_sql_are_checked_before_launch(self):
        with self.assertRaises(ValueError):
            query_csv("rustspark", b"x" * (MAX_UPLOAD_BYTES + 1), "SELECT 1")
        with self.assertRaises(ValueError):
            query_csv("rustspark", b"a\n1", "")

    def test_csv_export_escapes_delimiters(self):
        rows = [{"city": "New York, NY", "sales": 2}]
        output = list(csv.DictReader(io.StringIO(as_csv(rows))))
        self.assertEqual(output, [{"city": "New York, NY", "sales": "2"}])


    @patch("app.subprocess.run")
    def test_json_upload_selects_the_native_json_input(self, run):
        run.return_value = subprocess.CompletedProcess([], 0, '[{"id":1}]', "")
        rows = query_csv("rustspark", b'{"id":1}\\n', "SELECT * FROM data",
                         source_format="json")
        self.assertEqual(rows, [{"id": 1}])
        args = run.call_args.args[0]
        self.assertIn("--json", args)
        self.assertTrue(args[args.index("--json") + 1].endswith(".json"))
        with self.assertRaisesRegex(ValueError, "format"):
            query_csv("rustspark", b"x", "SELECT 1", source_format="parquet")

    def test_csv_export_preserves_keys_from_later_rows(self):
        rows = [{"city": "Berlin"}, {"city": "Paris", "extra": 7}]
        output = list(csv.DictReader(io.StringIO(as_csv(rows))))
        self.assertEqual(output, [
            {"city": "Berlin", "extra": ""},
            {"city": "Paris", "extra": "7"},
        ])

    def test_json_export_preserves_unicode_nulls_and_numbers(self):
        rows = [{"city": "Łódź", "count": 4, "fraction": 1.5, "missing": None}]
        self.assertEqual(json.loads(as_json(rows)), rows)
        self.assertIn("Łódź", as_json(rows))
        with self.assertRaises(ValueError):
            as_json([{"invalid": float("nan")}])


if __name__ == "__main__":
    unittest.main()
