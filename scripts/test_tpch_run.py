import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

SOURCE = Path(__file__).with_name("tpch_run.py")
spec = importlib.util.spec_from_file_location("tpch_run", SOURCE)
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)


class QueryRunnerTests(unittest.TestCase):
    def test_signature_ignores_row_order(self):
        a = '{"a":1,"b":2}\n{"a":2,"b":3}\n'
        b = '{"b":3,"a":2}\n{"b":2,"a":1}\n'
        self.assertEqual(runner.result_signature(a), runner.result_signature(b))
        for bad in ('{"a":NaN}', '{"a":1,"a":2}'):
            with self.assertRaises(ValueError):
                runner.result_signature(bad)

    def test_ordered_signature_detects_row_reordering(self):
        first = '{"n":1}\n{"n":2}\n'
        reversed_rows = '{"n":2}\n{"n":1}\n'
        self.assertEqual(runner.result_signature(first), runner.result_signature(reversed_rows))
        self.assertNotEqual(
            runner.result_signature(first, ordered=True),
            runner.result_signature(reversed_rows, ordered=True),
        )

    def test_signature_rejects_scalar_and_overflowed_json(self):
        for payload in ('1', '[1]', '{"x":1e400}', '{"nested":{"x":-1e400}}'):
            with self.subTest(payload=payload), self.assertRaises(ValueError):
                runner.result_signature(payload)

    def test_ordered_sql_uses_order_sensitive_repetition_check(self):
        first = mock.Mock(returncode=0, stdout='{"n":1}\n{"n":2}\n', stderr="")
        second = mock.Mock(returncode=0, stdout='{"n":2}\n{"n":1}\n', stderr="")
        with mock.patch.object(runner.subprocess, "run", side_effect=[first, second]):
            with self.assertRaisesRegex(ValueError, "Nondeterministic"):
                runner.measure(["rustspark"], 2, 0, 30, ordered=True)

    def test_warmup_is_not_measured(self):
        fake = mock.Mock(returncode=0, stdout='{"n":3}\n', stderr="")
        with mock.patch.object(runner.subprocess, "run", return_value=fake) as call:
            result = runner.measure(["rustspark"], 3, 1, 30)
        self.assertEqual(call.call_count, 4)
        self.assertEqual(len(result["samples_ms"]), 3)
        self.assertEqual(result["result_rows"], 1)
        self.assertGreaterEqual(result["p95_ms"], result["median_ms"])

    def test_benchmark_detects_top_level_order_by(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            data = root / "data"
            queries = root / "queries"
            data.mkdir()
            queries.mkdir()
            for name in runner.TABLES:
                (data / f"{name}.parquet").touch()
            (queries / "01.sql").write_text(
                "SELECT n FROM (SELECT 1 AS n) ORDER BY n;", encoding="utf-8"
            )
            (queries / "02.sql").write_text(
                "SELECT n FROM (SELECT 1 AS n ORDER BY n);", encoding="utf-8"
            )
            with mock.patch.object(runner, "measure", return_value={
                "status": "ok", "median_ms": 1,
            }) as measure:
                runner.benchmark("rustspark", runner.query_paths(queries), data, 1, 0, 10, 2)
            self.assertEqual(
                [call.kwargs["ordered"] for call in measure.call_args_list],
                [True, False],
            )

    def test_query_errors_do_not_skip_later_queries(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            data = root / "data"
            queries = root / "queries"
            data.mkdir()
            queries.mkdir()
            for name in runner.TABLES:
                (data / f"{name}.parquet").touch()
            for number in (1, 2):
                (queries / f"{number:02}.sql").write_text(f"SELECT {number};")
            with mock.patch.object(runner, "measure", side_effect=[
                RuntimeError("bad query"), {"status": "ok", "median_ms": 1},
            ]):
                report = runner.benchmark(
                    "rustspark", runner.query_paths(queries), data, 1, 0, 10, 2
                )
            self.assertEqual([r["status"] for r in report["results"]],
                             ["error", "ok"])
            with self.assertRaisesRegex(ValueError, "missing"):
                runner.query_paths(queries, [3])


if __name__ == "__main__":
    unittest.main()
