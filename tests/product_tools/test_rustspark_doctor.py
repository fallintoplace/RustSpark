import json
import subprocess
import unittest
from unittest.mock import patch

from scripts.rustspark_doctor import diagnose


class DoctorTests(unittest.TestCase):
    @patch("scripts.rustspark_doctor.invoke")
    def test_local_read_only_checks_pass(self, run):
        run.side_effect = [
            subprocess.CompletedProcess([], 0, "rustspark 0.1.0\n", ""),
            subprocess.CompletedProcess([], 0, '[{"probe":42}]', ""),
        ]
        report = diagnose("/some/private/path/rustspark")
        self.assertTrue(report["ok"])
        self.assertEqual(len(report["checks"]), 2)
        self.assertNotIn("/some/private/path", json.dumps(report))

    @patch("scripts.rustspark_doctor.invoke", side_effect=FileNotFoundError())
    def test_missing_binary_is_a_failure_without_path_disclosure(self, run):
        report = diagnose("/secret/rustspark")
        self.assertFalse(report["ok"])
        self.assertTrue(all(not item["ok"] for item in report["checks"]))
        self.assertNotIn("/secret", json.dumps(report))


if __name__ == "__main__":
    unittest.main()
