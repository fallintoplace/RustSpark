"""Only CLI-compatible ASCII digits are accepted for numeric flags."""

import unittest

import bench_tpch_tune as tune


class TuningAsciiFlagTests(unittest.TestCase):
    def test_unicode_numeric_flags_are_rejected(self):
        for raw in ("１２", "١٢", "१२"):
            with self.subTest(raw=raw):
                variants = [
                    {"name": "baseline", "partitions": 1},
                    {"name": "candidate", "partitions": 2,
                     "flags": ["--batch-size", raw]},
                ]
                with self.assertRaisesRegex(ValueError, "positive integer"):
                    tune.parse_variants(variants)

    def test_ascii_flags_are_accepted(self):
        variants = [
            {"name": "baseline", "partitions": 1},
            {"name": "candidate", "partitions": 2,
             "flags": ["--batch-size", "8192"]},
        ]
        self.assertEqual(tune.parse_variants(variants)[1]["flags"],
                         ["--batch-size", "8192"])


if __name__ == "__main__":
    unittest.main()
