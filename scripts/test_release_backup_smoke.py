import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import release_backup_smoke


class ReleaseBackupSmokeTests(unittest.TestCase):
    def test_manifest_checks_table_names_and_parts(self):
        with tempfile.TemporaryDirectory() as root:
            snapshot = Path(root)
            (snapshot / ".rustspark-backup-manifest").write_text(
                "rustspark-backup-v1\nfirst\nsecond\n", encoding="utf-8"
            )
            for table in ("first", "second"):
                folder = snapshot / table
                folder.mkdir()
                (folder / "part-0.parquet").write_bytes(b"parquet")
            release_backup_smoke.check_manifest(snapshot)

    def test_manifest_rejects_missing_table(self):
        with tempfile.TemporaryDirectory() as root:
            snapshot = Path(root)
            (snapshot / ".rustspark-backup-manifest").write_text(
                "rustspark-backup-v1\nfirst\n", encoding="utf-8"
            )
            with self.assertRaisesRegex(AssertionError, "manifest"):
                release_backup_smoke.check_manifest(snapshot)

    def test_manifest_rejects_missing_parquet(self):
        with tempfile.TemporaryDirectory() as root:
            snapshot = Path(root)
            (snapshot / ".rustspark-backup-manifest").write_text(
                "rustspark-backup-v1\nfirst\nsecond\n", encoding="utf-8"
            )
            for table in ("first", "second"):
                (snapshot / table).mkdir()
            with self.assertRaisesRegex(AssertionError, "Parquet"):
                release_backup_smoke.check_manifest(snapshot)

    @patch.object(release_backup_smoke.subprocess, "run")
    def test_rejected_backup_must_fail(self, command):
        command.return_value = subprocess.CompletedProcess([], 0, "", "")
        with self.assertRaisesRegex(AssertionError, "destination accepted"):
            release_backup_smoke.expect_backup_rejected(
                Path("/bin/rustspark"), Path("/db"), Path("/db/nested")
            )

    @patch.object(release_backup_smoke.subprocess, "run")
    def test_expected_backup_error_is_allowed(self, command):
        command.return_value = subprocess.CompletedProcess([], 1, "", "refused")
        release_backup_smoke.expect_backup_rejected(
            Path("/bin/rustspark"), Path("/db"), Path("/db/nested")
        )
        self.assertEqual(command.call_args.args[0][-2:], [
            "--backup-database", str(Path("/db/nested")),
        ])


if __name__ == "__main__":
    unittest.main()
