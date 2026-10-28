"""Row count parser and Parquet registration options."""

from pathlib import Path
import tempfile
import unittest

import bench_tpch_rowcounts as counts


class CountTests(unittest.TestCase):
    def test_parse_exact_count(self):
        self.assertEqual(counts.parse_count('{"n":42}\n'), 42)
        for value in ('{"n":null}', '{"n":2.0}', '{"n":-1}', '1', '{"n":1}\n{"n":2}'):
            with self.subTest(value=value), self.assertRaises(ValueError):
                counts.parse_count(value)

    def test_duplicate_manifest_keys_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = Path(directory)
            (fixture / "manifest.json").write_text(
                '{"rows": {}, "rows": {}}', encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "Duplicate"):
                counts.check("rs", fixture)

    def test_named_table_command(self):
        with tempfile.TemporaryDirectory() as tmp:
            fixture = Path(tmp)
            (fixture / "data").mkdir()
            (fixture / "data/customer.parquet").touch()
            command = counts.count_command("rs", fixture, "customer")
            self.assertIn("SELECT COUNT(*) AS n FROM customer", command)
            self.assertIn("--stdout-format", command)

    def test_unknown_table(self):
        with self.assertRaises(ValueError):
            counts.count_command("rs", Path("."), "customers")


if __name__ == "__main__":
    unittest.main()
