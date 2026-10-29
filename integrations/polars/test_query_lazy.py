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

    def test_rejects_conflicting_sources(self):
        with self.assertRaises(ValueError):
            build_command("SELECT 1", Path("r.arrow"), csv="a.csv", parquet="b.parquet")

    def test_persistent_database_directory_is_passed_as_argv(self):
        command = build_command("SELECT * FROM accounts", Path("result.arrow"),
                                database_dir="/tmp/rustspark db")
        self.assertEqual(command[command.index("--database-dir") + 1], "/tmp/rustspark db")
        self.assertNotIn("--csv", command)
        self.assertNotIn("--parquet", command)


if __name__ == "__main__":
    unittest.main()
