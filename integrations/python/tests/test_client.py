import subprocess
import unittest
from unittest.mock import patch

from rustspark_local import RustSpark, RustSparkError


class RustSparkTests(unittest.TestCase):
    @patch("rustspark_local.subprocess.run")
    def test_named_bindings_are_arguments_not_interpolated_sql(self, run):
        run.return_value = subprocess.CompletedProcess([], 0, '[{"city":"a","n":2}]', "")
        client = RustSpark(binary="/opt/rustspark", database_dir="/tmp/warehouse")
        rows = client.query(
            "SELECT * FROM sales WHERE n > $minimum",
            csv_tables={"sales": "/tmp/my files/sales.csv"},
            named_params={"minimum": "int64:2"},
            max_rows=10,
        )
        self.assertEqual(rows, [{"city": "a", "n": 2}])
        command = run.call_args.args[0]
        self.assertEqual(command[0], "/opt/rustspark")
        self.assertIn("sales=/tmp/my files/sales.csv", command)
        self.assertIn("minimum=int64:2", command)
        self.assertEqual(command.count("--sql"), 1)
        self.assertNotIn("int64:2", command[command.index("--sql") + 1])

    @patch("rustspark_local.subprocess.run")
    def test_reports_cli_errors(self, run):
        run.return_value = subprocess.CompletedProcess([], 2, "", "SQL error: unknown table")
        with self.assertRaisesRegex(RustSparkError, "unknown table"):
            RustSpark().query("SELECT * FROM missing")

    @patch("rustspark_local.subprocess.run")
    def test_rejects_invalid_json(self, run):
        run.return_value = subprocess.CompletedProcess([], 0, "not-json", "")
        with self.assertRaisesRegex(RustSparkError, "invalid JSON"):
            RustSpark().query("SELECT 1")

    def test_rejects_unsafe_or_conflicting_options_early(self):
        client = RustSpark()
        with self.assertRaises(ValueError):
            client.query("SELECT 1", csv="a.csv", parquet="b.parquet")
        with self.assertRaises(ValueError):
            client.query("SELECT 1", csv_tables={"bad=name": "file.csv"})
        with self.assertRaises(ValueError):
            client.query("SELECT $1", named_params={"1bad": "int64:3"})
        with self.assertRaises(ValueError):
            client.query("  ")
        with self.assertRaises(ValueError):
            client.query("SELECT 1", max_rows=-1)
        with self.assertRaises(ValueError):
            client.query("SELECT 1", process_timeout=0)

    @patch("rustspark_local.subprocess.run", side_effect=subprocess.TimeoutExpired("rustspark", 3))
    def test_deadline_is_reported(self, run):
        with self.assertRaisesRegex(RustSparkError, "process timeout"):
            RustSpark().query("SELECT 1", process_timeout=3)


if __name__ == "__main__":
    unittest.main()
