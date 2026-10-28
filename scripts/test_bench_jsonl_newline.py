"""JSONL benchmark fixtures must be byte-identical on Windows and Unix."""

from pathlib import Path
import tempfile
import unittest

import bench_jsonl as benchmark


class JsonlNewlineTests(unittest.TestCase):
    def test_records_use_unix_newlines(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "input.ndjson"
            benchmark.write_jsonl(path, 3)
            raw = path.read_bytes()
            self.assertEqual(raw.count(b"\n"), 3)
            self.assertNotIn(b"\r\n", raw)
            self.assertTrue(raw.endswith(b"\n"))


if __name__ == "__main__":
    unittest.main()
