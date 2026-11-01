"""Check JSONL fixture generation and typed SQL options."""

import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import bench_jsonl as benchmark


class JSONLTests(unittest.TestCase):
    def test_benchmark_rejects_non_integer_budgets_before_file_creation(self):
        defaults = dict(rows=4, repeat=1, warmup=0, timeout=5)
        invalid = (True, False, 1.5, "2", None, float("nan"), float("inf"), -1)
        for field in ("rows", "repeat", "warmup", "timeout"):
            for value in invalid:
                with self.subTest(field=field, value=value):
                    settings = {**defaults, field: value}
                    with patch.object(benchmark, "write_jsonl") as writer:
                        with self.assertRaisesRegex(ValueError, "benchmark settings"):
                            benchmark.benchmark("rustspark", **settings)
                        writer.assert_not_called()

    def test_generated_rows_are_deterministic(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "data.jsonl"
            benchmark.write_jsonl(path, 4)
            rows = [json.loads(line) for line in path.read_text().splitlines()]
            self.assertEqual(len(rows), 4)
            self.assertEqual(rows[0], {"id": 0, "city": "city_0", "amount": 0})
            self.assertEqual(rows[1], {"id": 1, "city": "city_1", "amount": 37})

    def test_explicit_schema_in_cli(self):
        command = benchmark.command("rs", "events.ndjson", "SELECT 1")
        self.assertEqual(command[command.index("--json-schema") + 1],
                         "id:int64,city:utf8,amount:int64")
        self.assertIn("--stdout-format", command)

    def test_invalid_rows_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(ValueError):
                benchmark.write_jsonl(Path(tmp) / "data", 0)

    def test_invalid_row_types_do_not_create_fixture(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "data.jsonl"
            for invalid in (True, False, 1.5, float("nan"), "4", None):
                with self.subTest(rows=invalid), self.assertRaises(ValueError):
                    benchmark.write_jsonl(path, invalid)
                self.assertFalse(path.exists())

    def test_selects_one_workload_without_running_other_queries(self):
        with patch.object(benchmark, "measure", return_value={"median_ms": 1.0}) as measured:
            report = benchmark.benchmark("rustspark", rows=4, repeat=1, warmup=0,
                                         workloads=["scan_count"])
        self.assertEqual([item["name"] for item in report["workloads"]], ["scan_count"])
        self.assertEqual(measured.call_count, 1)
        for invalid in ([], ["unknown"], ["scan_count", "scan_count"]):
            with self.subTest(invalid=invalid), self.assertRaisesRegex(ValueError, "workloads"):
                benchmark.benchmark("rustspark", rows=4, workloads=invalid)


if __name__ == "__main__":
    unittest.main()
