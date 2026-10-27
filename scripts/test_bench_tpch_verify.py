"""Test value comparison independently of the optional DuckDB dependency."""

import datetime
from decimal import Decimal
import unittest
from unittest import mock

import bench_tpch_verify as verify


class ComparisonTests(unittest.TestCase):
    def test_unordered_rows_compare_as_multisets(self):
        a = [{"n": 1}, {"n": 2}, {"n": 2}]
        b = [{"n": 2}, {"n": 2}, {"n": 1}]
        self.assertTrue(verify.equal_rows(a, b, Decimal("0"), ordered=False))
        self.assertFalse(verify.equal_rows(a, b, Decimal("0"), ordered=True))

    def test_identical_large_multisets_avoid_pairwise_search(self):
        expected = [{"n": i % 37, "text": str(i % 5)} for i in range(2000)]
        actual = list(reversed(expected))
        with mock.patch.object(verify, "same_row", side_effect=AssertionError("slow path")):
            self.assertTrue(verify.equal_rows(actual, expected, Decimal("0.000001"), False))
        actual[0] = {"n": -1, "text": "-1"}
        self.assertFalse(verify.equal_rows(actual, expected, Decimal("0"), False))

    def test_unhashable_values_use_tolerance_aware_path(self):
        self.assertTrue(verify.equal_rows([{"a": [1, 2]}], [{"a": [1, 2]}], Decimal(0), False))

    def test_numeric_tolerance(self):
        self.assertTrue(verify.same_value(Decimal("1.000001"), 1, Decimal("0.000002")))
        self.assertFalse(verify.same_value(1.1, 1, Decimal("0.01")))
        self.assertTrue(verify.same_value("1.0000", Decimal(1), Decimal("0")))
        self.assertFalse(verify.same_value(True, 1, Decimal("0")))

    def test_dates_and_strings(self):
        self.assertTrue(verify.same_value("2026-01-01", datetime.date(2026, 1, 1), Decimal(0)))
        self.assertFalse(verify.same_value("abc", "ABC", Decimal(0)))

    def test_result_shape_and_duplicates(self):
        with self.assertRaises(ValueError):
            verify.parse_rows("1\n")
        with self.assertRaises(ValueError):
            verify.parse_rows('{"x":1,"x":2}\n')
        self.assertEqual(verify.parse_rows('{"x":1.5}\n'), [{"x": Decimal("1.5")}])


if __name__ == "__main__":
    unittest.main()
