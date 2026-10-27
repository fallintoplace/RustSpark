import pathlib
import unittest
from unittest.mock import patch

import release_smoke


class ReleaseSmokeTests(unittest.TestCase):
    def test_catalog_lifecycle_steps_run_in_order(self):
        calls = []

        def execute(binary, *args):
            self.assertEqual(args[-2], "--sql")
            calls.append(("execute", args[-1]))

        def expect_rows(binary, expected, *args):
            self.assertEqual(args[-2], "--sql")
            calls.append(("expect", args[-1], expected))

        with patch.object(release_smoke, "execute", side_effect=execute), patch.object(
            release_smoke, "expect_rows", side_effect=expect_rows
        ):
            release_smoke.check_catalog_lifecycle(pathlib.Path("/unused/binary"))

        self.assertEqual(
            calls,
            [
                ("execute", "CREATE TABLE items (id BIGINT)"),
                ("execute", "INSERT INTO items VALUES (1), (2)"),
                ("expect", "SELECT SUM(id) AS total FROM items", [{"total": 3}]),
                ("execute", "ALTER TABLE items RENAME TO entries"),
                ("expect", "SELECT COUNT(*) AS n FROM entries", [{"n": 2}]),
                ("execute", "DROP TABLE entries"),
            ],
        )

    def test_drop_leaves_no_table_directory(self):
        import contextlib
        import tempfile

        with tempfile.TemporaryDirectory() as directory:
            (pathlib.Path(directory) / "entries").mkdir()
            with (
                patch.object(
                    release_smoke.tempfile, "TemporaryDirectory",
                    return_value=contextlib.nullcontext(directory),
                ),
                patch.object(release_smoke, "execute"),
                patch.object(release_smoke, "expect_rows"),
            ):
                with self.assertRaisesRegex(AssertionError, "left the entries directory"):
                    release_smoke.check_catalog_lifecycle(pathlib.Path("/unused/binary"))


    def test_file_workflow_checks_join_parameters_and_parquet(self):
        results = []
        exports = []

        def expect_rows(binary, expected, *args):
            results.append((expected, args))
            self.assertIn("--sql", args)

        with patch.object(release_smoke, "expect_rows", side_effect=expect_rows), patch.object(
            release_smoke, "execute", side_effect=lambda binary, *args: exports.append(args)
        ):
            release_smoke.check_file_workflows(pathlib.Path("/unused/binary"))

        self.assertEqual(len(results), 3)
        self.assertIn("--csv-table", results[0][1])
        self.assertEqual(results[0][0][0]["weighted"], 9)
        self.assertIn("int64:3", results[1][1])
        self.assertEqual(results[1][0], [{"total": 10}])
        self.assertEqual(len(exports), 1)
        self.assertIn("--output-parquet", exports[0])
        self.assertIn("--parquet", results[2][1])

    def test_invalid_sql_must_fail_without_success_output(self):
        from unittest.mock import Mock

        bad = Mock(returncode=1, stdout="", stderr="missing table")
        with patch.object(release_smoke.subprocess, "run", return_value=bad) as run:
            release_smoke.check_query_errors(pathlib.Path("/unused/binary"))
        self.assertIn("--stdout-format", run.call_args.args[0])

        for response, message in (
            (Mock(returncode=0, stdout="", stderr=""), "exit code"),
            (Mock(returncode=1, stdout="success", stderr=""), "success output"),
        ):
            with patch.object(release_smoke.subprocess, "run", return_value=response):
                with self.assertRaisesRegex(AssertionError, message):
                    release_smoke.check_query_errors(pathlib.Path("/unused/binary"))


if __name__ == "__main__":
    unittest.main()
