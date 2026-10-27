import io
import pathlib
import sys
import tarfile
import tempfile
import unittest
import zipfile

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import verify_release_archive as archive


class ArchiveVerifierTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = pathlib.Path(self.tmp.name)
        (self.root / "README.md").write_bytes(b"readme\n")
        (self.root / "LICENSE").write_bytes(b"Apache 2\n")

    def build(self, kind, *, extra=None, missing=None, binary_payload=b"program\n",
              executable=True):
        name = "rustspark.exe" if kind == "zip" else "rustspark"
        binary = self.root / name
        binary.write_bytes(b"program\n")
        binary.chmod(0o755)
        files = {"README.md": b"readme\n", "LICENSE": b"Apache 2\n",
                 name: binary_payload}
        if missing:
            del files[missing]
        if extra:
            files[extra] = b"extra\n"
        path = self.root / ("release.zip" if kind == "zip" else "release.tar.gz")
        if kind == "zip":
            with zipfile.ZipFile(path, "w") as output:
                for member, contents in files.items():
                    output.writestr(member, contents)
        else:
            with tarfile.open(path, "w:gz") as output:
                for member, contents in files.items():
                    entry = tarfile.TarInfo(member)
                    entry.size = len(contents)
                    entry.mode = (0o755 if executable else 0o644) if member == name else 0o644
                    output.addfile(entry, io.BytesIO(contents))
        return path, binary

    def test_tarball_binary_and_docs_round_trip(self):
        path, binary = self.build("tar")
        extracted = archive.verify_archive(path, binary, self.root, self.root)
        self.assertEqual(extracted.read_bytes(), binary.read_bytes())
        self.assertTrue(extracted.stat().st_mode & 0o111)

    def test_windows_zip_binary_and_docs_round_trip(self):
        path, binary = self.build("zip")
        extracted = archive.verify_archive(path, binary, self.root, self.root)
        self.assertEqual(extracted.read_bytes(), binary.read_bytes())

    def test_rejects_missing_license(self):
        for kind in ("tar", "zip"):
            path, binary = self.build(kind, missing="LICENSE")
            with self.assertRaisesRegex(ValueError, "Archive members differ"):
                archive.verify_archive(path, binary, self.root, self.root)

    def test_rejects_changed_executable(self):
        for kind in ("tar", "zip"):
            path, binary = self.build(kind, binary_payload=b"changed\n")
            with self.assertRaisesRegex(ValueError, "differs"):
                archive.verify_archive(path, binary, self.root, self.root)

    def test_rejects_extra_member_and_path_traversal(self):
        for bad_name in ("notes.txt", "../outside"):
            path, binary = self.build("tar", extra=bad_name)
            with self.assertRaisesRegex(ValueError, "Archive members differ"):
                archive.verify_archive(path, binary, self.root, self.root)

    def test_rejects_non_executable_unix_binary(self):
        path, binary = self.build("tar", executable=False)
        with self.assertRaisesRegex(ValueError, "executable permissions"):
            archive.verify_archive(path, binary, self.root, self.root)

    def test_rejects_unsupported_archive_type(self):
        with self.assertRaisesRegex(ValueError, "Unsupported release archive"):
            archive.archive_contents(self.root / "archive.bin")


if __name__ == "__main__":
    unittest.main()
