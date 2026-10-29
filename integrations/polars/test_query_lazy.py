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

    def test_named_file_sources_enable_multi_table_sql(self):
        args = build_command("SELECT * FROM users JOIN orders ON users.id = orders.id",
                             Path("result.arrow"),
                             csv_tables={"users": "/tmp/users.csv"},
                             parquet_tables={"orders": "/tmp/orders.parquet"})
        self.assertIn("users=/tmp/users.csv", args)
        self.assertIn("orders=/tmp/orders.parquet", args)
        self.assertIn("--csv-table", args)
        with self.assertRaisesRegex(ValueError, "Named and unnamed"):
            build_command("SELECT 1", Path("result.arrow"),
                          csv="data.csv", csv_tables={"users": "users.csv"})
        with self.assertRaisesRegex(ValueError, "table name"):
            build_command("SELECT 1", Path("result.arrow"),
                          parquet_tables={"bad-name": "some.parquet"})


if __name__ == "__main__":
    unittest.main()
