import contextlib
import io
import importlib.util
import json
import pathlib
import tempfile
import unittest

SOURCE = pathlib.Path(__file__).with_name("spark_compat.py")
spec = importlib.util.spec_from_file_location("spark_compat", SOURCE)
mod = importlib.util.module_from_spec(spec)
import sys
sys.modules[spec.name] = mod
spec.loader.exec_module(mod)


class SparkCompatibilityTests(unittest.TestCase):
    def test_described_columns_preserve_quoted_tabs(self):
        columns = mod.parse_rustspark_columns("first\tpart\tInt64\tNOT NULL\n")
        self.assertEqual(columns, ["first\tpart"])
        with self.assertRaisesRegex(ValueError, "Invalid RustSpark schema description"):
            mod.parse_rustspark_columns("missing_types\n")

    def test_unordered_results_are_multiset_compared(self):
        first = mod.QueryResult(["id"], [[2], [1], [1]])
        second = mod.QueryResult(["id"], [[1], [2], [1]])
        self.assertIsNone(mod.compare(first, second))
        self.assertIsNotNone(mod.compare(first, second, ordered=True))

    def test_nulls_and_types_are_not_lost(self):
        self.assertIsNone(
            mod.compare(mod.QueryResult(["v"], [[None]]), mod.QueryResult(["v"], [[None]]))
        )
        self.assertIsNotNone(
            mod.compare(mod.QueryResult(["v"], [[1]]), mod.QueryResult(["v"], [["1"]]))
        )
        self.assertIsNotNone(
            mod.compare(mod.QueryResult(["a"], [[]]), mod.QueryResult(["b"], [[]]))
        )

    def test_case_validator_rejects_malformed_cases(self):
        with self.assertRaisesRegex(ValueError, "string id"):
            mod.validate_case({"sql": "SELECT 1"}, set())
        with self.assertRaisesRegex(ValueError, "requires nonempty sql"):
            mod.validate_case({"id": "broken", "sql": ""}, set())

    def test_duplicate_case_ids_are_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "cases.json"
            path.write_text(json.dumps([{"id": "x", "sql": "SELECT 1"},
                                         {"id": "x", "sql": "SELECT 2"}]))
            with self.assertRaisesRegex(ValueError, "Duplicate"):
                mod.load_cases(path)

    def test_rejects_string_ordered_flags(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "cases.json"
            path.write_text(json.dumps([{"id": "unordered", "sql": "SELECT 1", "ordered": "false"}]))
            with self.assertRaisesRegex(ValueError, "ordered must be a boolean"):
                mod.load_cases(path)

    def test_rejects_blank_case_ids(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "cases.json"
            path.write_text(json.dumps([{"id": " ", "sql": "SELECT 1"}]))
            with self.assertRaisesRegex(ValueError, "id must not be empty"):
                mod.load_cases(path)

    def test_case_runner_reports_pass_diff_and_error(self):
        cases = [
            {"id": "ok", "sql": "SELECT 1"},
            {"id": "diff", "sql": "SELECT 2"},
            {"id": "error", "sql": "SELECT 3"},
        ]

        def ours(sql):
            if sql == "SELECT 3":
                raise ValueError("simulated failure")
            return mod.QueryResult(["n"], [[1]])

        def reference(sql):
            return mod.QueryResult(["n"], [[2 if sql == "SELECT 2" else 1]])

        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            failures = mod.run_cases(cases, ours, reference)
        self.assertEqual(failures, 2)
        self.assertIn("PASS ok", output.getvalue())
        self.assertIn("DIFF diff", output.getvalue())
        self.assertIn("ERROR error: simulated failure", output.getvalue())

    def test_corpus_is_valid(self):
        cases = mod.load_cases(mod.DEFAULT_CASES)
        self.assertGreaterEqual(len(cases), 25)
        self.assertTrue(all("sql" in case for case in cases))


if __name__ == "__main__":
    unittest.main()
