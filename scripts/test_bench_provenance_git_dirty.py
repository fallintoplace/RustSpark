"""Provenance should reveal uncommitted benchmark code changes."""

import unittest
from unittest import mock

import bench_provenance as provenance


class GitDirtyProvenanceTests(unittest.TestCase):
    def test_dirty_and_clean_worktrees(self):
        with mock.patch.object(provenance.subprocess, "run",
                               return_value=mock.Mock(returncode=0, stdout=" M scripts/a.py\n")):
            self.assertTrue(provenance.git_dirty("."))
        with mock.patch.object(provenance.subprocess, "run",
                               return_value=mock.Mock(returncode=0, stdout="")):
            self.assertFalse(provenance.git_dirty("."))

    def test_unavailable_git_state_is_explicit(self):
        with mock.patch.object(provenance.subprocess, "run",
                               return_value=mock.Mock(returncode=128, stdout="")):
            self.assertIsNone(provenance.git_dirty("."))


if __name__ == "__main__":
    unittest.main()
