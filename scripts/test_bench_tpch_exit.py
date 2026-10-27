"""TPC-H CLI must report failures to automation, not only to JSON."""

from contextlib import redirect_stdout
from io import StringIO
import tempfile
import unittest
from unittest import mock

import tpch_run


class SmokeExitTests(unittest.TestCase):
    def run_status(self, ids, statuses=None, require_all=False):
        rows = [{"id": n, "status": (statuses or {}).get(n, "ok")} for n in ids]
        with tempfile.TemporaryDirectory() as folder:
            with mock.patch.object(tpch_run, "query_paths", return_value=[]):
                with mock.patch.object(tpch_run, "benchmark", return_value={"results": rows}):
                    arguments = ["--queries-dir", folder, "--parquet-dir", folder]
                    if require_all:
                        arguments.append("--require-all")
                    with redirect_stdout(StringIO()):
                        return tpch_run.main(arguments)

    def test_query_failure_exits_nonzero(self):
        self.assertEqual(self.run_status([1, 2], {2: "error"}), 1)

    def test_subset_succeeds_without_full_gate(self):
        self.assertEqual(self.run_status([1]), 0)

    def test_full_gate_rejects_subset(self):
        self.assertEqual(self.run_status([1, 2], require_all=True), 1)

    def test_full_gate_accepts_exact_set(self):
        self.assertEqual(self.run_status(range(1, 23), require_all=True), 0)

    def test_full_gate_does_not_accept_duplicated_query(self):
        self.assertEqual(self.run_status([1] * 22, require_all=True), 1)


if __name__ == "__main__":
    unittest.main()
