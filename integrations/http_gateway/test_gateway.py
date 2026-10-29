import json
import unittest
import tempfile
from pathlib import Path
from server import command_for_template, list_query_templates, validate_params, parse_result_rows


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


    def test_duplicate_json_request_keys_are_not_accepted(self):
        for body in (
            b'{"params":{"id":"int64:1","id":"int64:2"}}',
            b'{"params":{},"params":{"id":"int64:2"}}',
        ):
            with self.subTest(body=body), self.assertRaises(ValueError):
                validate_params(body)

    def test_backend_rows_must_be_json_objects(self):
        self.assertEqual(parse_result_rows('[{"city":"Berlin"},{"amount":3}]'),
                         [{"city": "Berlin"}, {"amount": 3}])
        self.assertEqual(parse_result_rows("[]"), [])
        for invalid in ('{}', '[1]', '[null]', '[{},"text"]', 'invalid'):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                parse_result_rows(invalid)

    def test_only_safe_regular_templates_are_listed(self):
        with tempfile.TemporaryDirectory() as directory:
            queries = Path(directory)
            (queries / "first.sql").write_text("SELECT 1")
            (queries / "second.sql").write_text("SELECT 2")
            (queries / "hidden.txt").write_text("SELECT 3")
            (queries / "bad.name.sql").write_text("SELECT 4")
            (queries / "link.sql").symlink_to(queries / "first.sql")
            self.assertEqual(list_query_templates(queries), ["first", "second"])


if __name__ == "__main__":
    unittest.main()
