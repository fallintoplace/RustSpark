"""Failed query selection and runner invocation."""

import unittest

import bench_tpch_retry as retry


class RetryTests(unittest.TestCase):
    def test_rejects_duplicate_json_keys_in_previous_and_retry_reports(self):
        from pathlib import Path
        from tempfile import TemporaryDirectory
        from unittest import mock

        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "retry.json"
            for body in (
                '{"results":[{"id":1,"status":"error"}],"results":[]}',
                '{"results":[{"id":1,"id":2,"status":"ok"}]}',
                '{"results":[{"id":1,"status":"ok","median_ms":NaN}]}',
            ):
                with self.subTest(body=body):
                    path.write_text(body, encoding="utf-8")
                    with self.assertRaises(ValueError):
                        retry.load_retry_report(path)

            previous = Path(tmp) / "previous.json"
            previous.write_text('{"results":[{"id":1,"status":"error"}]}',
                                encoding="utf-8")
            path.write_text('{"results":[{"id":1,"status":"ok"}],"results":[]}',
                            encoding="utf-8")
            args = type("Args", (), {
                "previous": previous, "json_out": path, "binary": "rustspark",
                "queries_dir": Path(tmp), "parquet_dir": Path(tmp),
                "repeat": 1, "timeout": 10,
            })()
            with mock.patch.object(retry.subprocess, "run", return_value=mock.Mock(returncode=0)):
                with self.assertRaises(ValueError):
                    retry.execute_retry(args)

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

    def test_retry_admits_only_positive_integer_budgets(self):
        for option in ("repeat", "timeout"):
            for invalid in (True, False, 0, -1, 1.5, float("nan"), float("inf"), "2"):
                kwargs = {option: invalid}
                with self.subTest(option=option, value=invalid), self.assertRaisesRegex(ValueError, "retry parameters"):
                    retry.retry_command("rs", "queries", "data", [2], "out.json", **kwargs)

    def test_no_empty_rerun_command(self):
        with self.assertRaises(ValueError):
            retry.retry_command("rs", "queries", "data", [], "out.json")


    def test_retry_rejects_boolean_and_malformed_ids(self):
        self.assertFalse(retry.retry_completed(
            {"results": [{"id": True, "status": "ok"}]}, [1]))
        self.assertFalse(retry.retry_completed(
            {"results": [None]}, [1]))
        self.assertFalse(retry.retry_completed(
            {"results": "not a result list"}, [1]))

    def test_retries_require_same_sql_fingerprint_as_failed_report(self):
        previous = {"results": [
            {"id": 2, "status": "error", "sql_sha256": "a" * 64},
        ]}
        replay = {"results": [
            {"id": 2, "status": "ok", "sql_sha256": "b" * 64},
        ]}
        self.assertFalse(retry.retry_completed(replay, [2], previous))
        replay["results"][0]["sql_sha256"] = "a" * 64
        self.assertTrue(retry.retry_completed(replay, [2], previous))
        replay["results"][0].pop("sql_sha256")
        self.assertFalse(retry.retry_completed(replay, [2], previous))
        previous["results"][0].pop("sql_sha256")
        self.assertTrue(retry.retry_completed(replay, [2], previous))

    def test_malformed_previous_results_fail_with_validation_error(self):
        with self.assertRaisesRegex(ValueError, "JSON object"):
            retry.failed_ids(None)
        with self.assertRaisesRegex(ValueError, "query result entry"):
            retry.failed_ids({"results": [None]})

    def test_retry_does_not_reuse_old_output(self):
        import json
        from pathlib import Path
        from tempfile import TemporaryDirectory
        from unittest import mock

        with TemporaryDirectory() as tmp:
            prior = Path(tmp) / "prior.json"
            output = Path(tmp) / "retry.json"
            prior.write_text(json.dumps(
                {"results": [{"id": 1, "status": "error"}]}),
                encoding="utf-8")
            output.write_text(json.dumps(
                {"results": [{"id": 1, "status": "ok"}]}),
                encoding="utf-8")
            with mock.patch.object(
                retry.subprocess, "run",
                return_value=mock.Mock(returncode=0),
            ):
                with self.assertRaises(SystemExit) as caught:
                    retry.main([
                        str(prior), "--queries-dir", tmp,
                        "--parquet-dir", tmp, "--json-out", str(output),
                    ])
            self.assertEqual(caught.exception.code, 1)
            self.assertFalse(output.exists())


if __name__ == "__main__":
    unittest.main()
