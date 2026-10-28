"""Reject duplicate query rows before writing partition comparison results."""

import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

import bench_tpch_matrix as matrix


class MatrixIdentityTests(unittest.TestCase):
    def test_duplicate_query_ids_fail_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            folder = Path(directory)

            def execute(command, **kwargs):
                output = Path(command[command.index("--json-out") + 1])
                rows = [
                    {"id": 1, "status": "ok", "result_sha256": "a" * 64}
                    for _ in range(22)
                ]
                output.write_text(json.dumps({"results": rows}), encoding="utf-8")
                return mock.Mock(returncode=0)

            with mock.patch.object(matrix.subprocess, "run", side_effect=execute):
                with self.assertRaisesRegex(ValueError, "duplicate"):
                    matrix.run_configuration("rs", folder, folder, folder, 1, 1, 0, {})

    def test_malformed_results_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            folder = Path(directory)

            def execute(command, **kwargs):
                output = Path(command[command.index("--json-out") + 1])
                output.write_text('{"results":[null]}', encoding="utf-8")
                return mock.Mock(returncode=0)

            with mock.patch.object(matrix.subprocess, "run", side_effect=execute):
                with self.assertRaisesRegex(ValueError, "JSON objects"):
                    matrix.run_configuration("rs", folder, folder, folder, 2, 1, 0, {})


if __name__ == "__main__":
    unittest.main()
