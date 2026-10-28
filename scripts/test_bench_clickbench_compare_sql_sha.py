"""ClickBench report comparison requires the same pinned SQL corpus."""
import unittest
import bench_clickbench_compare as compare

def report(digest):
    return {"query_file_sha256": digest, "results": [
        {"id": 1, "status": "executed", "median_ms": 1.0, "result_sha256": "a" * 64}
    ]}

class QueryCorpusMatchTests(unittest.TestCase):
    def test_rejects_different_corpora(self):
        with self.assertRaisesRegex(ValueError, "corpus"):
            compare.compare(report("b" * 64), report("c" * 64))

    def test_rejects_only_one_report_with_hash(self):
        left, right = report("b" * 64), report("b" * 64)
        right.pop("query_file_sha256")
        with self.assertRaisesRegex(ValueError, "corpus"):
            compare.compare(left, right)

    def test_matches_valid_hash(self):
        self.assertEqual(compare.compare(report("b" * 64), report("b" * 64))["queries"], 1)

if __name__ == "__main__":
    unittest.main()
