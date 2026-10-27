import importlib.util
from pathlib import Path
import tempfile
import unittest

SOURCE = Path(__file__).with_name("tpch_prepare.py")
spec = importlib.util.spec_from_file_location("tpch_prepare", SOURCE)
prepare = importlib.util.module_from_spec(spec)
spec.loader.exec_module(prepare)


class FakeConnection:
    def __init__(self, full=True):
        self.commands = []
        self.full = full

    def execute(self, command):
        self.commands.append(command)
        return self

    def fetchone(self):
        return (5,)

    def fetchall(self):
        return [(n, f"SELECT {n}") for n in range(1, 23 if self.full else 3)]


class FixtureTests(unittest.TestCase):
    def test_requires_positive_finite_scale(self):
        for scale in (0, -1, float("nan"), float("inf")):
            with self.subTest(scale=scale), tempfile.TemporaryDirectory() as folder:
                with self.assertRaises(ValueError):
                    prepare.prepare(FakeConnection(), Path(folder), scale)

    def test_writes_manifest_and_queries(self):
        with tempfile.TemporaryDirectory() as folder:
            destination = Path(folder) / "fixture"
            connection = FakeConnection()
            report = prepare.prepare(connection, destination, 0.1)
            self.assertEqual(report["rows"]["orders"], 5)
            self.assertEqual(len(report["queries"]), 22)
            self.assertEqual(len(report["tables"]), 8)
            self.assertEqual((destination / "queries/18.sql").read_text(),
                             "SELECT 18\n")
            self.assertTrue((destination / "manifest.json").exists())
            self.assertIn("CALL dbgen(sf=0.1)", connection.commands)
            with self.assertRaisesRegex(ValueError, "empty"):
                prepare.prepare(connection, destination, 0.1)

    def test_rejects_symlinked_and_regular_file_destinations(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            target = root / "elsewhere"
            target.mkdir()
            linked = root / "fixtures-link"
            try:
                linked.symlink_to(target, target_is_directory=True)
            except (OSError, NotImplementedError):
                self.skipTest("directory symlinks are unavailable")
            with self.assertRaisesRegex(ValueError, "symlink"):
                prepare.prepare(FakeConnection(), linked, 0.1)
            self.assertEqual(list(target.iterdir()), [])

            regular_file = root / "fixtures-file"
            regular_file.write_text("keep", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "directory"):
                prepare.prepare(FakeConnection(), regular_file, 0.1)
            self.assertEqual(regular_file.read_text(encoding="utf-8"), "keep")

    def test_rejects_missing_queries(self):
        with tempfile.TemporaryDirectory() as folder:
            with self.assertRaisesRegex(ValueError, "22"):
                prepare.prepare(FakeConnection(full=False), Path(folder), 0.1)


if __name__ == "__main__":
    unittest.main()
