import unittest
from pathlib import Path
from query_lazy import build_command


class PolarsCommandTests(unittest.TestCase):
    def test_generates_ipc_file_without_shell(self):
        command = build_command("SELECT 1", Path("/tmp/x y.arrow"), csv="/tmp/a b.csv")
        self.assertEqual(command[0], "rustspark")
        self.assertIn("/tmp/x y.arrow", command)
        self.assertIn("/tmp/a b.csv", command)
        self.assertIn("--output-arrow", command)

    def test_bounds_lazy_ipc_export_rows(self):
        command = build_command("SELECT 1", Path("rows.arrow"), max_rows=25)
        self.assertEqual(command[command.index("--max-rows") + 1], "25")
        for invalid in (0, -1, True, 1.5):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                build_command("SELECT 1", Path("rows.arrow"), max_rows=invalid)

    def test_rejects_conflicting_sources(self):
        with self.assertRaises(ValueError):
            build_command("SELECT 1", Path("r.arrow"), csv="a.csv", parquet="b.parquet")


if __name__ == "__main__":
    unittest.main()
