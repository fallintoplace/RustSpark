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

    def test_named_tables_enable_multi_file_queries(self):
        command = build_command("SELECT * FROM users JOIN orders ON users.id = orders.uid",
                                csv_tables={"users": "/a b/users.csv"},
                                parquet_tables={"orders": "/data/orders.parquet"})
        self.assertIn("users=/a b/users.csv", command)
        self.assertIn("orders=/data/orders.parquet", command)
        self.assertIn("--csv-table", command)
        with self.assertRaisesRegex(ValueError, "Named and unnamed"):
            build_command("SELECT 1", csv="a.csv", csv_tables={"users": "b.csv"})
        for name in ("bad-name", "a b", "1bad", "é"):
            with self.subTest(name=name), self.assertRaisesRegex(ValueError, "table name"):
                build_command("SELECT 1", json_tables={name: "data.json"})


if __name__ == "__main__":
    unittest.main()
