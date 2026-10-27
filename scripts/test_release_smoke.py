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
            with patch.object(release_smoke.tempfile, "TemporaryDirectory",
                              return_value=contextlib.nullcontext(directory)), \\
                    patch.object(release_smoke, "execute"), \\
                    patch.object(release_smoke, "expect_rows"):
                with self.assertRaisesRegex(AssertionError, "left the entries directory"):
                    release_smoke.check_catalog_lifecycle(pathlib.Path("/unused/binary"))


if __name__ == "__main__":
    unittest.main()
