"""Require portable ASCII numbers in performance experiments."""
import unittest
from bench_experiment import validate_variant

class ExperimentFlagTests(unittest.TestCase):
    def test_rejects_unicode_digits(self):
        for raw in ("١", "１２", "0", "-1", "1.2"):
            with self.subTest(raw=raw), self.assertRaisesRegex(ValueError, "positive integer"):
                validate_variant({"name": "test", "flags": ["--batch-size", raw]}, set())

    def test_accepts_ascii_positive_values(self):
        validate_variant({"name": "test", "flags": ["--batch-size", "1024"]}, set())


if __name__ == "__main__":
    unittest.main()
