import csv
import io
import pathlib
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(pathlib.Path(__file__).parent))
import check_native_stream_grouped as benchmark


class NativeGroupedOracleTests(unittest.TestCase):
    def test_rows_are_deterministic_and_cover_groups(self):
        self.assertEqual(benchmark.fact_row(0), ("Berlin", "books", 0, 500))
        self.assertEqual(benchmark.fact_row(0), benchmark.fact_row(0))
        self.assertGreater(len({benchmark.fact_row(i)[:2] for i in range(4000)}), 20)

    def test_generated_sqlite_fixture_matches_file(self):
        with tempfile.TemporaryDirectory() as root:
            path = pathlib.Path(root) / "facts.csv"
            conn = sqlite3.connect(":memory:")
            try:
                benchmark.make_fixture(path, conn, 256)
                with path.open(newline="", encoding="utf-8") as handle:
                    content = list(csv.reader(handle))
                self.assertEqual(tuple(content[0]), benchmark.FIELDS)
                self.assertEqual(len(content), 257)
                self.assertEqual(tuple(content[1]), tuple(map(str, benchmark.fact_row(0))))
                self.assertEqual(conn.execute("SELECT COUNT(*) FROM events").fetchone(), (256,))
            finally:
                conn.close()

    def test_output_parser_checks_header_and_shape(self):
        good = "region,category,total,n\nBerlin,books,42,1\n"
        self.assertEqual(benchmark.decode_rustspark_output(good),
                         [("Berlin", "books", "42", "1")])
        for bad in ("", "wrong,category,total,n\n", "region,category,total,n\na,b,1\n"):
            with self.subTest(data=bad), self.assertRaises(ValueError):
                benchmark.decode_rustspark_output(bad)

    def test_subprocess_comparison_uses_actual_csv_and_query(self):
        expected = "region,category,total,n\n"
        completed = type("Completed", (), {"returncode": 0, "stdout": expected, "stderr": ""})()
        with patch.object(benchmark.subprocess, "run", return_value=completed) as run:
            _, rows = benchmark.run_engine("native", pathlib.Path("sample.csv"), 15)
        self.assertEqual(rows, [])
        call = run.call_args
        self.assertIn("GROUP BY region, category", call.args[0][-1])
        self.assertIn("region:text!", call.args[0])
        self.assertEqual(call.kwargs["timeout"], 15)


if __name__ == "__main__":
    unittest.main()
