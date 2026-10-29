import tempfile
from pathlib import Path
import unittest

from query_templates import list_templates, make_command, template_path


class TemplatesTests(unittest.TestCase):
    def test_names_are_allowlisted_and_cannot_escape_directory(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            (base / "safe.sql").write_text("SELECT 1", encoding="utf-8")
            (base / "bad.name.sql").write_text("SELECT 2", encoding="utf-8")
            self.assertEqual(list_templates(base), ["safe"])
            self.assertEqual(template_path(base, "safe").name, "safe.sql")
            for name in ["../secret", "bad.name", "", ".hidden", "some/name"]:
                with self.subTest(name=name), self.assertRaises(ValueError):
                    template_path(base, name)

    def test_params_are_bounded_and_bound_outside_sql(self):
        command = make_command("rustspark", Path("/db"), Path("orders.sql"),
                               {"region": "utf8:North America"})
        self.assertIn("region=utf8:North America", command)
        self.assertIn("--sql-file", command)
        with self.assertRaises(ValueError):
            make_command("rustspark", Path("/db"), Path("q.sql"),
                         {"bad-name": "int64:3"})

    def test_per_query_limits_cannot_exceed_service_caps(self):
        command = make_command("rustspark", Path("/db"), Path("q.sql"), {},
                               max_rows=5, query_timeout=2)
        self.assertEqual(command[command.index("--max-rows") + 1], "5")
        self.assertEqual(command[command.index("--query-timeout") + 1], "2")
        for rows, timeout in ((201, 1), (1, 21), (True, 1), (1, 0)):
            with self.subTest(rows=rows, timeout=timeout), self.assertRaises(ValueError):
                make_command("rustspark", Path("/db"), Path("q.sql"), {},
                             max_rows=rows, query_timeout=timeout)


if __name__ == "__main__":
    unittest.main()
