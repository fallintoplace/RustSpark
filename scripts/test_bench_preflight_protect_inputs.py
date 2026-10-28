"""Preflight must not truncate its input when an output path collides."""
import contextlib
import io
from pathlib import Path
import tempfile
import unittest
from unittest import mock
import bench_clickbench_preflight as preflight

class PreflightInputProtectionTests(unittest.TestCase):
    def test_rejects_report_paths_matching_either_input(self):
        with tempfile.TemporaryDirectory() as tmp:
            parquet, sql = Path(tmp) / "input.parquet", Path(tmp) / "queries.sql"
            parquet.write_bytes(b"PAR1dataPAR1")
            sql.write_text("SELECT 1;")
            for target in (parquet, sql):
                before = target.read_bytes()
                with self.subTest(path=target), mock.patch.object(preflight, "inspect") as inspect:
                    with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                        preflight.main(["--parquet", str(parquet), "--queries", str(sql),
                                        "--json-out", str(target)])
                    inspect.assert_not_called()
                    self.assertEqual(target.read_bytes(), before)

if __name__ == "__main__":
    unittest.main()
