"""A pinned ClickBench corpus must contain one SELECT per input line."""
import unittest
from unittest import mock
import clickbench_fetch as fetch


class CorpusSingleStatementTests(unittest.TestCase):
    def test_rejects_multiple_statements_even_with_pinned_hash(self):
        corpus = ("SELECT 1; DROP TABLE hits;\n" * fetch.QUERY_COUNT).encode()
        with mock.patch.object(fetch, "UPSTREAM_BLOB_SHA1", fetch.git_blob_sha1(corpus)):
            with self.assertRaisesRegex(ValueError, "one SELECT"):
                fetch.verify_queries(corpus)

    def test_accepts_single_statement_lines_with_matching_hash(self):
        corpus = ("SELECT COUNT(*) FROM hits;\n" * fetch.QUERY_COUNT).encode()
        with mock.patch.object(fetch, "UPSTREAM_BLOB_SHA1", fetch.git_blob_sha1(corpus)):
            fetch.verify_queries(corpus)


if __name__ == "__main__":
    unittest.main()
