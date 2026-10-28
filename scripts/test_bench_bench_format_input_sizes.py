"""Expose physical file sizes alongside cross-format timings."""
import unittest
from unittest import mock
import bench_format_compare as formats

class CrossFormatSizesTests(unittest.TestCase):
    def test_report_has_both_input_sizes(self):
        def fake_csv(path, rows):
            path.write_bytes(b"x" * 19)
        def fake_parquet(csv, path):
            path.write_bytes(b"PAR1" * 3)
        with (mock.patch.object(formats, "write_csv", side_effect=fake_csv),
              mock.patch.object(formats, "convert_to_parquet", side_effect=fake_parquet),
              mock.patch.object(formats, "measure", return_value={"median_ms": 1.0})):
            report = formats.benchmark("unused", rows=1, repeat=1, warmup=0, timeout=1)
        self.assertEqual(report["csv_bytes"], 19)
        self.assertEqual(report["parquet_bytes"], 12)


if __name__ == "__main__":
    unittest.main()
