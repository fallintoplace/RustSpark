# Querying JSON stored in VARCHAR columns

RustSpark registers three JSON scalar SQL functions for UTF-8 text columns:

- `GET_JSON_OBJECT(json_text, path)` extracts a JSON value as text.
- `JSON_VALID(json_text)` checks whether a string is valid JSON.
- `JSON_ARRAY_LENGTH(json_text)` returns the top-level array's length.

```sql
SELECT GET_JSON_OBJECT('{"user":{"name":"Ada"}}', '$.user.name');
SELECT JSON_VALID('{"valid":true}');
SELECT JSON_ARRAY_LENGTH('[1,2,3]');
```

`GET_JSON_OBJECT` supports the root path `$`, dot-separated fields,
zero-based array indices such as `$[0]`, and quoted object keys such as
`$['hyphen-key']`. JSON string values are returned without surrounding
quotes. JSON objects and arrays are returned as compact serialized text.

For missing keys, JSON nulls, malformed JSON and unsupported paths,
`GET_JSON_OBJECT` returns SQL NULL. Wildcards, filters, escaped quoted keys,
and full Spark JSONPath syntax are not yet supported.

`JSON_VALID` returns FALSE for malformed non-NULL text and NULL for SQL NULL.
`JSON_ARRAY_LENGTH` returns NULL for non-array or malformed JSON.

These functions query JSON text. Use `--json` and `--json-schema` to
read JSON files into native Arrow columns instead.
