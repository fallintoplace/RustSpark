import contextlib
import importlib.util
import io
import pathlib
import sys
import unittest

SOURCE = pathlib.Path(__file__).with_name("spark_typed_compat.py")
spec = importlib.util.spec_from_file_location("spark_typed_compat", SOURCE)
mod = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = mod
from pathlib import Path
sys.path.insert(0, str(SOURCE.parent))
try:
    spec.loader.exec_module(mod)
finally:
    sys.path.pop(0)


class TypedSparkCompatibilityTests(unittest.TestCase):
    def test_canonical_types(self):
        self.assertEqual(mod.canonical_type("Int64"), "bigint")
        self.assertEqual(mod.canonical_type("Decimal128(10, 2)"), "decimal(10,2)")
        self.assertEqual(mod.canonical_type("Timestamp(Microsecond, None)"), "timestamp")
        self.assertEqual(mod.canonical_type("Utf8"), "string")


    def test_rustspark_description_preserves_column_tabs(self):
        names, types, nullables = mod.parse_rustspark_description(
            "first\tpart\tInt64\tNOT NULL\n"
        )
        self.assertEqual((names, types, nullables), (["first\tpart"], ["bigint"], [False]))

    def test_parse_rustspark_description_rejects_incomplete_fields(self):
        self.assertEqual(
            mod.parse_rustspark_description("count\tInt64\tNULL\n"),
            (["count"], ["bigint"], [True]),
        )
        with self.assertRaisesRegex(ValueError, "Invalid RustSpark schema description"):
            mod.parse_rustspark_description("bad-field\n")
        with self.assertRaisesRegex(ValueError, "Invalid RustSpark schema description"):
            mod.parse_rustspark_description("count\tInt64\tUNKNOWN\n")

    def test_parse_rustspark_json_uses_described_column_order(self):
        result = mod.parse_rustspark_json('{"x":1,"y":null}\n{"x":2,"y":3}\n', ["y", "x"])
        self.assertEqual(result.columns, ["y", "x"])
        self.assertEqual(result.rows, [[None, 1], [3, 2]])

    def test_type_difference_is_not_silenced(self):
        result = mod.QueryResult(["v"], [[42]])
        left = mod.TypedResult(result, ["bigint"])
        right = mod.TypedResult(result, ["int"])
        self.assertIn("schema differs", mod.compare_typed(left, right, False))

    def test_duplicate_rows_and_nulls(self):
        left = mod.TypedResult(mod.QueryResult(["v"], [[None], [1], [1]]), ["bigint"])
        right = mod.TypedResult(mod.QueryResult(["v"], [[1], [None], [1]]), ["bigint"])
        self.assertIsNone(mod.compare_typed(left, right, False))
        self.assertIsNotNone(mod.compare_typed(left, right, True))

    def test_timestamp_and_decimal_encodings_are_normalized(self):
        ours = mod.TypedResult(
            mod.QueryResult(["ts", "money"], [["2024-01-02 12:00:00", "10.00"]]),
            ["timestamp", "decimal(10,2)"],
        )
        import datetime
        import decimal
        reference = mod.TypedResult(
            mod.QueryResult(["ts", "money"], [[datetime.datetime(2024, 1, 2, 12), decimal.Decimal("10.0")]]),
            ["timestamp", "decimal(10,2)"],
        )
        self.assertIsNone(mod.compare_typed(ours, reference, True))

    def test_nullability_is_opt_in_and_reports_mismatch(self):
        ours = mod.TypedResult(mod.QueryResult(["n"], [[1]]), ["bigint"], [True])
        reference = mod.TypedResult(mod.QueryResult(["n"], [[1]]), ["bigint"], [False])
        self.assertIsNone(mod.compare_typed(ours, reference, True))
        self.assertIn("nullability differs", mod.compare_typed(
            ours, reference, True, check_nullability=True))

    def test_result_shape_is_validated(self):
        bad = mod.TypedResult(mod.QueryResult(["n"], [[1, 2]]), ["bigint"])
        with self.assertRaisesRegex(ValueError, "Row length"):
            mod.compare_typed(bad, bad, True)

    def test_fixture_corpus_is_valid(self):
        cases = mod.load_cases(mod.CASES)
        self.assertGreaterEqual(len(cases), 30)
        for name in mod.TABLES:
            self.assertTrue((mod.FIXTURES / (name + ".csv")).is_file())

    def test_failures_are_reported(self):
        cases = [{"id": "good", "sql": "SELECT 1"}, {"id": "wrong", "sql": "SELECT 2"}]
        def ours(sql):
            return mod.TypedResult(mod.QueryResult(["v"], [[1]]), ["bigint"])
        def reference(sql):
            return mod.TypedResult(mod.QueryResult(["v"], [[1]]), [
                "int" if sql.endswith("2") else "bigint"
            ])
        with contextlib.redirect_stdout(io.StringIO()):
            count, results = mod.run_cases(cases, ours, reference)
        self.assertEqual(count, 1)
        self.assertEqual([item["status"] for item in results], ["PASS", "DIFF"])


if __name__ == "__main__":
    unittest.main()
