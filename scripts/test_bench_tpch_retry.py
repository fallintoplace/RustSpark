"""Failed query selection and runner invocation."""

import unittest

import bench_tpch_retry as retry


class RetryTests(unittest.TestCase):
    def test_selects_only_failures(self):
        report = {"results": [{"id": 3, "status": "error"},
                              {"id": 1, "status": "ok"},
                              {"id": 2, "status": "error"}]}
        self.assertEqual(retry.failed_ids(report), [2, 3])

    def test_rejects_duplicate_ids(self):
        with self.assertRaisesRegex(ValueError, "repeated"):
            retry.failed_ids({"results": [{"id": 2}, {"id": 2}]})

    def test_builds_safe_argument_list(self):
        args = retry.retry_command("rs", "queries", "data", [2, 4], "out.json")
        start = args.index("--query-ids")
        self.assertEqual(args[start + 1:start + 3], ["2", "4"])
        self.assertIn("--json-out", args)

    def test_no_empty_rerun_command(self):
        with self.assertRaises(ValueError):
            retry.retry_command("rs", "queries", "data", [], "out.json")


if __name__ == "__main__":
    unittest.main()
