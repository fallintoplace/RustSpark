"""Check full ClickBench corpus and Parquet header/footer."""

from pathlib import Path
import tempfile
import unittest

import bench_clickbench_preflight as check


class PreflightTests(unittest.TestCase):
    def test_full_corpus_and_parquet(self):
        with tempfile.TemporaryDirectory() as tmp:
            parquet, sql = Path(tmp) / "hits.parquet", Path(tmp) / "queries.sql"
            parquet.write_bytes(b"PAR1moredataPAR1")
            sql.write_text("\n".join(f"SELECT {i};" for i in range(1, 44)) + "\n")
            manifest = check.inspect(parquet, sql)
            self.assertEqual(manifest["queries"], 43)
            self.assertEqual(len(manifest["parquet_sha256"]), 64)
            parquet.write_bytes(b"BAD0moredataPAR1")
            with self.assertRaisesRegex(ValueError, "PAR1"):
                check.inspect(parquet, sql)

    def test_rejects_incomplete_corpus(self):
        with tempfile.TemporaryDirectory() as tmp:
            parquet, sql = Path(tmp) / "hits.parquet", Path(tmp) / "queries.sql"
            parquet.write_bytes(b"PAR1PAR1")
            sql.write_text("SELECT 1;\n")
            with self.assertRaisesRegex(ValueError, "43"):
                check.inspect(parquet, sql)


if __name__ == "__main__":
    unittest.main()
