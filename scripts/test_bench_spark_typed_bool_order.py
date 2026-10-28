"""Typed Spark compatibility requires a real Boolean ordering flag."""
import json
from pathlib import Path
import tempfile
import unittest
import spark_typed_compat as typed


class TypedOrderedFlagTests(unittest.TestCase):
    def test_rejects_integer_flags(self):
        for ordered in (0, 1, "false"):
            with self.subTest(ordered=ordered), tempfile.TemporaryDirectory() as tmp:
                path = Path(tmp) / "cases.json"
                path.write_text(json.dumps([{"id": "probe", "sql": "SELECT 1", "ordered": ordered}]))
                with self.assertRaisesRegex(ValueError, "ordering"):
                    typed.load_cases(path)

    def test_accepts_boolean_flags(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "cases.json"
            path.write_text(json.dumps([{"id": "probe", "sql": "SELECT 1", "ordered": False}]))
            self.assertEqual(len(typed.load_cases(path)), 1)


if __name__ == "__main__":
    unittest.main()
