"""TPC-H fixture checks must reject missing or changed inputs."""

import hashlib
import json
from pathlib import Path
import tempfile
import unittest

import bench_tpch_manifest as audit


class ManifestTests(unittest.TestCase):
    def fixture(self, root):
        (root / "queries").mkdir()
        (root / "data").mkdir()
        tables = {}
        rows = {}
        for table in audit.TABLES:
            tables[table] = f"data/{table}.parquet"
            (root / tables[table]).write_bytes(b"PAR1")
            rows[table] = 2
        queries = []
        for number in range(1, 23):
            path = f"queries/{number:02}.sql"
            contents = f"SELECT {number};\n".encode()
            (root / path).write_bytes(contents)
            queries.append({"id": number, "path": path, "sha256": hashlib.sha256(contents).hexdigest()})
        manifest = {"scale_factor": 0.01, "tables": tables, "rows": rows, "queries": queries}
        (root / "manifest.json").write_text(json.dumps(manifest))
        return manifest

    def test_valid_manifest(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.fixture(root)
            self.assertEqual(audit.check(root)["queries"], 22)

    def test_rejects_corrupted_query(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.fixture(root)
            (root / "queries/03.sql").write_text("SELECT 42")
            with self.assertRaisesRegex(ValueError, "mismatch"):
                audit.check(root)

    def test_rejects_missing_table(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.fixture(root)
            (root / "data/customer.parquet").unlink()
            with self.assertRaisesRegex(ValueError, "Missing fixture"):
                audit.check(root)

    def test_rejects_duplicates(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest = self.fixture(root)
            manifest["queries"][1]["id"] = 1
            (root / "manifest.json").write_text(json.dumps(manifest))
            with self.assertRaisesRegex(ValueError, "duplicate"):
                audit.check(root)


if __name__ == "__main__":
    unittest.main()
