"""Non-finite values must not quietly pass Spark differential comparisons."""
import unittest
from spark_compat import QueryResult, fingerprint


class FiniteCompatibilityResultsTests(unittest.TestCase):
    def test_rejects_nan_and_infinity(self):
        for value in (float("nan"), float("inf"), float("-inf")):
            with self.subTest(value=value), self.assertRaisesRegex(ValueError, "Out of range"):
                fingerprint(QueryResult(columns=["x"], rows=[[value]]))

    def test_finite_rows_still_match(self):
        result = QueryResult(columns=["x"], rows=[[0.0], [12.5]])
        self.assertEqual(fingerprint(result), (["x"], ["[0.0]", "[12.5]"]))


if __name__ == "__main__":
    unittest.main()
