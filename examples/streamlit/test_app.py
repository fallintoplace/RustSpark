import csv
import io
import subprocess
import unittest
from unittest.mock import patch

from app import MAX_UPLOAD_BYTES, as_csv, query_csv


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


if __name__ == "__main__":
    unittest.main()
