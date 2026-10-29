import json
import subprocess
import tempfile
from pathlib import Path
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

    @patch("scripts.rustspark_doctor.invoke")
    def test_optional_parquet_and_json_probes_do_not_expose_paths(self, invoke):
        invoke.side_effect = [
            subprocess.CompletedProcess([], 0, "rustspark 0.1.0", ""),
            *[subprocess.CompletedProcess([], 0, '[{"probe":42}]', "") for _ in range(3)],
        ]
        with tempfile.TemporaryDirectory() as tmp:
            parquet = Path(tmp) / "secret.parquet"
            json_file = Path(tmp) / "secret.json"
            parquet.touch()
            json_file.touch()
            report = diagnose("rustspark", parquet=str(parquet), json_input=str(json_file))
        self.assertTrue(report["ok"])
        self.assertEqual([item["name"] for item in report["checks"]],
                         ["version", "standalone_sql", "parquet_input", "json_input"])
        self.assertNotIn("secret.parquet", json.dumps(report))
        args = [call.args[0] for call in invoke.call_args_list]
        self.assertTrue(any("--parquet" in arg for arg in args))
        self.assertTrue(any("--json" in arg for arg in args))


if __name__ == "__main__":
    unittest.main()
