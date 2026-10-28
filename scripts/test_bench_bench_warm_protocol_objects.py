"""Validate warm benchmark subprocess protocol rows."""
import unittest
import bench_warm_compare as warm

class WarmProtocolTests(unittest.TestCase):
    def test_rejects_nonobjects(self):
        for text in ('[]', '5', 'null', '"message"'):
            with self.subTest(text=text), self.assertRaisesRegex(ValueError, "JSON objects"):
                warm.parse_result_line(text)

    def test_valid_object(self):
        self.assertEqual(warm.parse_result_line('{"name":"scan_count","elapsed_ms":1}'),
                         {"name": "scan_count", "elapsed_ms": 1})


if __name__ == "__main__":
    unittest.main()
