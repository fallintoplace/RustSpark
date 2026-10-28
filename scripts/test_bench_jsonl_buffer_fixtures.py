"""JSONL fixture writer is correct around buffering boundaries."""
import json
from pathlib import Path
import tempfile
import unittest
from bench_jsonl import benchmark_row, write_jsonl

class JsonlBufferTests(unittest.TestCase):
    def test_output_matches_reference_at_chunk_boundaries(self):
        for size in (1, 4095, 4096, 4097, 8200):
            with self.subTest(size=size), tempfile.TemporaryDirectory() as tmp:
                path = Path(tmp) / "data.jsonl"
                write_jsonl(path, size)
                records = [json.loads(x) for x in path.read_text().splitlines()]
                self.assertEqual(len(records), size)
                for index in (0, size - 1):
                    city, amount = benchmark_row(index)
                    self.assertEqual(records[index], {"id": index, "city": city, "amount": amount})

if __name__ == "__main__":
    unittest.main()
