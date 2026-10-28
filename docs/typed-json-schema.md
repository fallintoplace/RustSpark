# Typed nested JSON schemas

Use `--json-schema` with native Arrow nested types when reading JSON files.
Primitive types continue to use the existing syntax. Supported additions:

- `array<TYPE>` or `list<TYPE>` for nullable list elements
- `struct<NAME:TYPE,...>` for named nested object fields
- `map<KEY,VALUE>` for Arrow maps with non-null keys and nullable values

Types can be nested, for example:

```sh
cargo run -- --json events.jsonl \
  --json-schema 'id:int64,tags:array<utf8>,meta:struct<city:utf8,level:int64>,attrs:map<utf8,int64>' \
  --sql 'SELECT tags, meta, attrs FROM data' --stdout-format json
```

For `map<utf8,TYPE>`, JSON input can be an object with string keys.
Fields are nullable by default. Nested commas are supported inside
angle brackets. JSON object keys not present in the explicit struct schema
are not automatically added as fields.

This option describes columns read from JSON. It does not add Spark's
`FROM_JSON` SQL function, parse JSON embedded in UTF-8 columns, or
change `--json-array` (which controls file-level JSON record arrays).
