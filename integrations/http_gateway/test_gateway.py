import json
import unittest
from pathlib import Path
from server import command_for_template, validate_params


class GatewayTests(unittest.TestCase):
    def test_typed_parameters_not_substituted_into_sql(self):
        args = command_for_template(
            "rustspark", Path("/db"), Path("/queries/orders.sql"),
            {"customer": "utf8:Alice Smith", "limit": "int64:5"}, 100, 10,
        )
        self.assertIn("customer=utf8:Alice Smith", args)
        self.assertIn("--sql-file", args)
        self.assertNotIn("Alice Smith", args)
        self.assertEqual(args[args.index("--sql-file") + 1], "/queries/orders.sql")

    def test_input_validation(self):
        self.assertEqual(validate_params(b'{"params":{"id":"int64:123"}}'), {"id": "int64:123"})
        invalid = [
            b'{"sql":"DROP TABLE users"}', b'{"params":[]}',
            b'{"params":{"1x":"int64:4"}}', b'{"params":{"x":"4"}}',
            b'{"params":{"x":4}}', b'invalid',
        ]
        for body in invalid:
            with self.subTest(body=body), self.assertRaises((ValueError, json.JSONDecodeError)):
                validate_params(body)


if __name__ == "__main__":
    unittest.main()
