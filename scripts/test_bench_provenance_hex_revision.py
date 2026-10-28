"""Only hexadecimal Git revisions should become benchmark provenance."""
import tempfile
import unittest
from unittest import mock
import bench_provenance as provenance

class GitRevisionTests(unittest.TestCase):
    def test_rejects_arbitrary_40_character_output(self):
        with mock.patch.object(provenance.subprocess, "run", return_value=mock.Mock(
            returncode=0, stdout="z" * 40)):
            self.assertIsNone(provenance.git_revision("."))

    def test_normalizes_valid_sha(self):
        revision = "AB" * 20
        with mock.patch.object(provenance.subprocess, "run", return_value=mock.Mock(
            returncode=0, stdout=revision + "\n")):
            self.assertEqual(provenance.git_revision("."), revision.lower())

if __name__ == "__main__":
    unittest.main()
