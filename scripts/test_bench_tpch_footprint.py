"""Exact fixture comparison uses streaming hashes, not scale factor alone."""

import hashlib
import json
from pathlib import Path
import tempfile
import unittest

import bench_tpch_footprint as footprint
from bench_tpch_manifest import TABLES


def make_fixture(root, content=b"PAR1"):
    root.mkdir()
    (root / "data").mkdir()
    (root / "queries").mkdir()
    paths = {}
    rows = {}
    hashes = {}
    for table in TABLES:
        path = f"data/{table}.parquet"
        (root / path).write_bytes(content)
        paths[table] = path
        rows[table] = 2
        hashes[table] = hashlib.sha256(content).hexdigest()
    queries = []
    for n in range(1, 23):
        sql = f"SELECT {n};\n".encode()
        path = f"queries/{n:02}.sql"
        (root / path).write_bytes(sql)
        queries.append({"id": n, "path": path, "sha256": hashlib.sha256(sql).hexdigest()})
    (root / "manifest.json").write_text(json.dumps({
        "scale_factor": 0.01, "tables": paths, "rows": rows,
        "table_sha256": hashes, "queries": queries,
    }))


class FootprintTests(unittest.TestCase):
    def test_same_scale_does_not_imply_same_parquet(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            make_fixture(root / "first")
            make_fixture(root / "other", content=b"PAR1modified")
            first = footprint.profile(root / "first")
            second = footprint.profile(root / "other")
            self.assertEqual(first["total_physical_bytes"], 4 * 8)
            self.assertEqual(first["tables"]["customer"]["physical_bytes_per_row"], 2.0)
            self.assertTrue(footprint.compare(first, first)["identical_parquet_inputs"])
            changed = footprint.compare(first, second)
            self.assertEqual(len(changed["changed_tables"]), 8)
            self.assertTrue(changed["identical_sql"])

    def test_streamed_sha_matches_known_digest(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "big.parquet"
            data = b"PAR1" * (1024 * 1024)
            path.write_bytes(data)
            self.assertEqual(footprint.streamed_sha256(path), hashlib.sha256(data).hexdigest())

    def test_different_scale_factors_are_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            make_fixture(root / "first")
            report = footprint.profile(root / "first")
            other = dict(report)
            other["scale_factor"] = 1.0
            with self.assertRaisesRegex(ValueError, "scale factors"):
                footprint.compare(report, other)

    def test_changed_row_metadata_is_not_an_identical_fixture(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            make_fixture(root / "first")
            before = footprint.profile(root / "first")
            after = json.loads(json.dumps(before))
            after["tables"]["customer"]["rows"] += 1
            difference = footprint.compare(before, after)
            self.assertFalse(difference["identical_parquet_inputs"])
            self.assertEqual(difference["changed_tables"], ["customer"])


if __name__ == "__main__":
    unittest.main()
