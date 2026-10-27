import contextlib
import importlib.util
import io
import pathlib
import sys
import unittest
from unittest import mock

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

    def test_unsigned_types_are_not_mangled_by_signed_aliases(self):
        for type_name in ("UInt8", "UInt16", "UInt32", "UInt64"):
            self.assertEqual(mod.canonical_type(type_name), type_name.lower())
        self.assertEqual(mod.canonical_type("List(Int64)"), "list(bigint)")

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

    def test_decimal38_comparison_does_not_round_significant_digits(self):
        value = "123456789012345678901234567890123456.78"
        self.assertEqual(
            mod.normalized_typed_value(value, "decimal(38,2)"),
            value,
        )
        self.assertEqual(
            mod.normalized_typed_value("0.000", "decimal(38,3)"), "0",
        )

    def test_temporal_helpers_keep_utc_and_date_values(self):
        import datetime
        self.assertEqual(
            mod.normalize_timestamp("2024-01-02T03:00:00+03:00"),
            "2024-01-02T00:00:00.000000",
        )
        self.assertEqual(
            mod.normalize_date(datetime.datetime(2024, 1, 2, 9)),
            "2024-01-02",
        )
        self.assertIsNone(mod.normalized_typed_value(None, "timestamp"))

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


    def test_csv_command_preserves_typed_schema(self):
        command = mod.rustspark_command("/bin/rustspark", "SELECT 1")
        self.assertEqual(command.count("--csv-table"), 2)
        self.assertIn(mod.RUST_SCHEMA, command)
        self.assertNotIn("--parquet-table", command)

    def test_parquet_command_reads_the_same_directories(self):
        root = pathlib.Path("/tmp/parquet-fixtures")
        command = mod.rustspark_command(
            "/bin/rustspark", "SELECT COUNT(*) FROM events_left", "parquet", root,
        )
        self.assertNotIn("--csv-schema", command)
        self.assertNotIn("--csv-table", command)
        self.assertEqual(command.count("--parquet-table"), 2)
        self.assertIn(f"events_left={root / 'events_left'}", command)
        self.assertIn(f"events_right={root / 'events_right'}", command)

    def test_bad_formats_and_missing_parquet_root(self):
        with self.assertRaisesRegex(ValueError, "requires"):
            mod.rustspark_command("rs", "SELECT 1", "parquet")
        with self.assertRaisesRegex(ValueError, "Unsupported"):
            mod.rustspark_command("rs", "SELECT 1", "json")
        with self.assertRaisesRegex(ValueError, "requires"):
            mod.register_spark_sources(mock.Mock(), "parquet")

    def test_spark_reads_the_parquet_it_wrote(self):
        session = mock.Mock()
        csv_frame = session.read.option.return_value.schema.return_value.csv.return_value
        parquet_frame = session.read.parquet.return_value
        root = pathlib.Path("/tmp/shared-parquet-data")
        mod.register_spark_sources(session, "parquet", root)
        self.assertEqual(csv_frame.write.parquet.call_count, len(mod.TABLES))
        self.assertEqual(session.read.parquet.call_count, len(mod.TABLES))
        self.assertEqual(parquet_frame.createOrReplaceTempView.call_count, len(mod.TABLES))
        for name in mod.TABLES:
            csv_frame.write.parquet.assert_any_call(str(root / name))
            session.read.parquet.assert_any_call(str(root / name))

    def test_csv_input_never_writes_parquet(self):
        session = mock.Mock()
        csv_frame = session.read.option.return_value.schema.return_value.csv.return_value
        mod.register_spark_sources(session, "csv")
        csv_frame.write.parquet.assert_not_called()
        self.assertEqual(csv_frame.createOrReplaceTempView.call_count, len(mod.TABLES))

if __name__ == "__main__":
    unittest.main()
