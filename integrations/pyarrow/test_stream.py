import unittest
from pathlib import Path
from stream import build_command


class ArrowStreamCommandTests(unittest.TestCase):
    def test_command_uses_arrow_ipc_and_literal_file_paths(self):
        args = build_command("SELECT * FROM data", csv=Path("/tmp/a b.csv"), max_rows=50)
        self.assertEqual(args[:4], ["rustspark", "--sql", "SELECT * FROM data", "--stdout-format"])
        self.assertIn("arrow", args)
        self.assertIn("/tmp/a b.csv", args)
        self.assertIn("50", args)

    def test_rejects_conflicting_sources(self):
        with self.assertRaises(ValueError):
            build_command("SELECT 1", csv="a.csv", parquet="b.parquet")
        with self.assertRaises(ValueError):
            build_command(" ", csv="a.csv")
        with self.assertRaises(ValueError):
            build_command("SELECT 1", query_timeout=0)

    def test_rejects_non_integer_limits_before_process_launch(self):
        for bad in (True, 1.5, float("nan"), -1):
            with self.subTest(max_rows=bad), self.assertRaisesRegex(ValueError, "max_rows"):
                build_command("SELECT 1", max_rows=bad)
        for bad in (False, 1.5, float("inf"), -1):
            with self.subTest(query_timeout=bad), self.assertRaisesRegex(ValueError, "query_timeout"):
                build_command("SELECT 1", query_timeout=bad)
        self.assertIn("0", build_command("SELECT 1", max_rows=0))


if __name__ == "__main__":
    unittest.main()
