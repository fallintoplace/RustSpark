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

    def test_json_source_is_forwarded_as_separate_cli_argument(self):
        args = build_command("SELECT * FROM events", Path("/tmp/out.arrow"),
                             json_input="/tmp/events.json")
        self.assertEqual(args[args.index("--json") + 1], "/tmp/events.json")
        with self.assertRaises(ValueError):
            build_command("SELECT 1", Path("/tmp/out.arrow"),
                          csv="/tmp/x.csv", json_input="/tmp/x.json")


if __name__ == "__main__":
    unittest.main()
