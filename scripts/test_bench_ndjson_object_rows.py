"""Reject malformed NDJSON rows before benchmarking SQL correctness."""

import unittest

from bench_rustspark import output_signature


class JsonObjectRowsTests(unittest.TestCase):
    def test_rejects_scalar_and_array_rows(self):
        for text in ('42\n', '[]\n', 'null\n', '{"n": 1}\n[1, 2]\n'):
            with self.subTest(text=text), self.assertRaisesRegex(ValueError, "JSON object"):
                output_signature(text)

    def test_valid_result_objects_keep_stable_digest(self):
        left = output_signature('{"city": "A", "n": 1}\n{"city": "B", "n": 2}\n')
        right = output_signature('{"n": 2, "city": "B"}\n{"n": 1, "city": "A"}\n')
        self.assertEqual(left[0], right[0])


if __name__ == "__main__":
    unittest.main()
