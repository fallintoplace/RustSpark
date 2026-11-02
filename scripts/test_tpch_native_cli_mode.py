import importlib.util
from pathlib import Path
import unittest

SOURCE = Path(__file__).with_name("tpch_run.py")
spec = importlib.util.spec_from_file_location("tpch_run", SOURCE)
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)

class NativeTpchCommandTests(unittest.TestCase):
    def test_native_tpch_command_uses_only_supported_flags(self):
        args = runner.command_for(
            "./rustspark", ["--parquet-table", "lineitem=data.parquet"],
            "06.sql", 2, native_cli=True,
        )
        self.assertEqual(args, [
            "./rustspark", "--parquet-table", "lineitem=data.parquet",
            "--sql-file", "06.sql", "--stdout-format", "json",
        ])
        self.assertNotIn("--target-partitions", args)

    def test_legacy_tpch_still_forwards_partition_count(self):
        args = runner.command_for("rs", [], "06.sql", 4)
        self.assertEqual(args[args.index("--target-partitions") + 1], "4")

    def test_native_mode_rejects_unsupported_legacy_tuning(self):
        for tuning in (
            {"batch_size": 8192},
            {"memory_limit_mb": 512},
            {"pushdown_filters": True},
        ):
            with self.subTest(tuning=tuning), self.assertRaises(ValueError):
                runner.command_for("rs", [], "06.sql", 2, native_cli=True, **tuning)
            with self.assertRaises(ValueError):
                runner.benchmark(
                    "rs", [], Path("."), native_cli=True, **tuning
                )

if __name__ == "__main__":
    unittest.main()
