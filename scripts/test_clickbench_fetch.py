import hashlib
import io
from pathlib import Path
import importlib.util
import tempfile
import unittest
from unittest import mock


MODULE = Path(__file__).with_name("clickbench_fetch.py")
spec = importlib.util.spec_from_file_location("clickbench_fetch", MODULE)
fetch = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fetch)


class ClickBenchFetchTests(unittest.TestCase):
    def setUp(self):
        self.corpus = ("SELECT COUNT(*) FROM hits;\n" * fetch.QUERY_COUNT).encode("utf-8")

    def test_git_blob_hash_matches_git_object_format(self):
        prefix = f"blob {len(self.corpus)}".encode("ascii") + bytes([0])
        expected = hashlib.sha1(prefix + self.corpus).hexdigest()
        self.assertEqual(fetch.git_blob_sha1(self.corpus), expected)

    def test_download_requires_pinned_hash(self):
        with mock.patch.object(fetch, "urlopen", return_value=io.BytesIO(self.corpus)) as opener:
            with self.assertRaisesRegex(ValueError, "hash does not match"):
                fetch.fetch_queries()
        opener.assert_called_once_with(fetch.UPSTREAM_URL, timeout=30)

    def test_pinned_download_accepts_43_valid_statements(self):
        with (
            mock.patch.object(fetch, "UPSTREAM_BLOB_SHA1", fetch.git_blob_sha1(self.corpus)),
            mock.patch.object(fetch, "urlopen", return_value=io.BytesIO(self.corpus)),
        ):
            self.assertEqual(fetch.fetch_queries(), self.corpus)

    def test_rejects_tampered_or_oversized_download(self):
        with self.assertRaisesRegex(ValueError, "hash does not match"):
            fetch.verify_queries(self.corpus + b"X")
        with self.assertRaisesRegex(ValueError, "size limit"):
            fetch.verify_queries(b"x" * (fetch.MAX_BYTES + 1))

    def test_rejects_invalid_number_of_queries(self):
        short = self.corpus.splitlines(keepends=True)[0]
        with mock.patch.object(fetch, "UPSTREAM_BLOB_SHA1", fetch.git_blob_sha1(short)):
            with self.assertRaisesRegex(ValueError, "Expected 43"):
                fetch.verify_queries(short)

    def test_dangling_symlink_needs_explicit_overwrite(self):
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "queries.sql"
            missing = Path(directory) / "missing.sql"
            try:
                target.symlink_to(missing)
            except (OSError, NotImplementedError):
                self.skipTest("symlinks are unavailable")
            with self.assertRaises(FileExistsError):
                fetch.write_queries(target, self.corpus)
            self.assertTrue(target.is_symlink())
            fetch.write_queries(target, self.corpus, overwrite=True)
            self.assertFalse(target.is_symlink())
            self.assertFalse(missing.exists())
            self.assertEqual(target.read_bytes(), self.corpus)

    def test_concurrent_output_creation_is_not_overwritten(self):
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "queries.sql"
            real_link = fetch.os.link

            def concurrent_creator(source, destination):
                target.write_bytes(b"created by someone else")
                return real_link(source, destination)

            with mock.patch.object(fetch.os, "link", side_effect=concurrent_creator):
                with self.assertRaises(FileExistsError):
                    fetch.write_queries(target, self.corpus)
            self.assertEqual(target.read_bytes(), b"created by someone else")
            self.assertEqual(sorted(path.name for path in Path(directory).iterdir()), ["queries.sql"])

    def test_write_does_not_overwrite_unless_requested(self):
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "queries" / "queries.sql"
            fetch.write_queries(target, self.corpus)
            self.assertEqual(target.read_bytes(), self.corpus)
            with self.assertRaises(FileExistsError):
                fetch.write_queries(target, b"new")
            self.assertEqual(target.read_bytes(), self.corpus)
            fetch.write_queries(target, b"new", overwrite=True)
            self.assertEqual(target.read_bytes(), b"new")


if __name__ == "__main__":
    unittest.main()
