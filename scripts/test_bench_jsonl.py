"""Check JSONL fixture generation and typed SQL options."""

import json
from pathlib import Path
import tempfile
import unittest

import bench_jsonl as benchmark


class JSONLTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
