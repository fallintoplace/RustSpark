"""ClickBench runner never truncates its SQL or Parquet fixtures."""
import contextlib
import io
from pathlib import Path
import tempfile
import unittest
from unittest import mock
import clickbench_run as runner

class ClickbenchRunInputSafetyTests(unittest.TestCase):
    def test_output_collisions_fail_before_benchmark(self):
        with tempfile.TemporaryDirectory() as tmp:
            parquet, sql = Path(tmp) / "hits.parquet", Path(tmp) / "queries.sql"
            parquet.write_bytes(b"PAR1testPAR1")
            sql.write_text("SELECT 1;\n")
            for dest in (parquet, sql):
                before = dest.read_bytes()
                with self.subTest(dest=dest), mock.patch.object(runner, "benchmark") as bench:
                    with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                        runner.main(["--parquet", str(parquet), "--queries", str(sql),
                                     "--json-out", str(dest)])
                    bench.assert_not_called()
                    self.assertEqual(dest.read_bytes(), before)

if __name__ == "__main__":
    unittest.main()
