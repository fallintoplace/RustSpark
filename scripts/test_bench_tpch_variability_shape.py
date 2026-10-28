"""Malformed report rows should fail as validation errors."""

import unittest

import bench_tpch_variability as variability


class VariabilityRowShapeTests(unittest.TestCase):
    def test_non_object_rows_are_rejected(self):
        for entry in (None, [], "row", 5, True):
            with self.subTest(entry=entry):
                with self.assertRaisesRegex(ValueError, "JSON objects"):
                    variability.inspect({"results": [entry]})


if __name__ == "__main__":
    unittest.main()
