package rustsparklocal

import (
    "bytes"
    "context"
    "errors"
    "encoding/json"
    "fmt"
    "reflect"
    "strings"
    "testing"
)

func TestCommandEscapesNothingThroughShell(t *testing.T) {
    c := Client{Binary: "/usr/bin/rustspark", DatabaseDir: "/tmp/data"}
    args, err := c.arguments("SELECT * FROM orders WHERE id = $id", Options{
        CSVTables: map[string]string{"orders": "/tmp/my sales.csv"},
        NamedParams: map[string]string{"id": "int64:42"},
        MaxRows: 10,
    })
    if err != nil { t.Fatal(err) }
    if !contains(args, "orders=/tmp/my sales.csv") || !contains(args, "id=int64:42") {
        t.Fatalf("expected verbatim argv values: %v", args)
    }
    if !reflect.DeepEqual(args[:4], []string{"--sql", "SELECT * FROM orders WHERE id = $id", "--stdout-format", "json-array"}) {
        t.Fatalf("unexpected command start: %v", args[:4])
    }
}

func TestIdentifiersRejectNewlinesAndNonASCII(t *testing.T) {
    c := Client{}
    for _, name := range []string{"bad\n", "bad-name", "é", "1start"} {
        if _, err := c.arguments("SELECT 1", Options{CSVTables: map[string]string{name: "file.csv"}}); err == nil {
            t.Fatalf("expected invalid table identifier %q to be rejected", name)
        }
        if _, err := c.arguments("SELECT 1", Options{NamedParams: map[string]string{name: "int64:1"}}); err == nil {
            t.Fatalf("expected invalid parameter identifier %q to be rejected", name)
        }
    }
}

func TestRejectsConflictingAndInvalidInputs(t *testing.T) {
    c := Client{}
    cases := []Options{
        {CSV: "a.csv", Parquet: "b.parquet"},
        {CSV: "a.csv", CSVTables: map[string]string{"t":"b.csv"}},
        {NamedParams: map[string]string{"bad-name":"int64:3"}},
        {NamedParams: map[string]string{"value":"no_type"}},
        {MaxRows: -1},
    }
    for _, options := range cases {
        if _, err := c.arguments("SELECT 1", options); err == nil {
            t.Fatalf("expected validation error for %+v", options)
        }
    }
    if _, err := c.arguments(strings.Repeat(" ", 4), Options{}); err == nil {
        t.Fatal("blank SQL must be rejected")
    }
}

func contains(values []string, needle string) bool {
    for _, value := range values { if value == needle { return true } }
    return false
}

func TestNDJSONStreamsProcessOutput(t *testing.T) {
    var output bytes.Buffer
    client := Client{Binary: "echo"}
    if err := client.QueryNDJSON(context.Background(), "SELECT 1", Options{}, &output); err != nil {
        t.Fatal(err)
    }
    if !strings.Contains(output.String(), "--stdout-format json") {
        t.Fatalf("expected NDJSON output flag, got %q", output.String())
    }
    if err := client.QueryNDJSON(context.Background(), "SELECT 1", Options{}, nil); err == nil {
        t.Fatal("nil writer must be rejected")
    }
}

func TestQuerySQLFileInsteadOfInlineSQL(t *testing.T) {
    c := Client{}
    args, err := c.arguments("", Options{SQLFile: "/tmp/query files/report.sql"})
    if err != nil { t.Fatal(err) }
    if !reflect.DeepEqual(args[:4], []string{"--sql-file", "/tmp/query files/report.sql", "--stdout-format", "json-array"}) {
        t.Fatalf("unexpected SQL file arguments: %v", args)
    }
    if _, err := c.arguments("SELECT 1", Options{SQLFile: "query.sql"}); err == nil {
        t.Fatal("inline SQL and SQLFile cannot be combined")
    }
}

func TestBufferedJSONStdoutIsLimited(t *testing.T) {
    output := boundedOutput{limit: 3}
    if _, err := output.Write([]byte("ab")); err != nil { t.Fatal(err) }
    if _, err := output.Write([]byte("cd")); !errors.Is(err, errOutputLimit) {
        t.Fatalf("expected output limit error, got %v", err)
    }
    if output.String() != "ab" { t.Fatalf("unexpected buffered output: %q", output.String()) }
    if _, err := (Client{}).arguments("SELECT 1", Options{MaxOutputBytes: -1}); err == nil {
        t.Fatal("negative byte budget must be rejected")
    }
}

func TestQueryOneBoundsCardinality(t *testing.T) {
    row, err := oneRow(nil)
    if err != nil || row != nil { t.Fatalf("expected nil result, got %v: %v", row, err) }
    row, err = oneRow([]map[string]any{{"id": 1}})
    if err != nil || row["id"] != 1 { t.Fatalf("unexpected row %v: %v", row, err) }
    if _, err := oneRow([]map[string]any{{"id": 1}, {"id": 2}}); err == nil {
        t.Fatal("expected error for multiple rows")
    }
}

func TestDecodeRowsRejectsNullAndNonObjectRows(t *testing.T) {
    for _, raw := range []string{
        "null", "[null]", "[{},null]", "{}", "[1]", "[true]",
        "[{}] []", "[{}] garbage",
    } {
        if rows, err := decodeRows(strings.NewReader(raw)); err == nil {
            t.Fatalf("accepted non-object JSON result %q: %#v", raw, rows)
        }
    }
    for _, raw := range []string{"[]", "[{}]", "[{\"n\":1234567890123456789}]"} {
        rows, err := decodeRows(strings.NewReader(raw))
        if err != nil {
            t.Fatalf("rejected valid rows %q: %v", raw, err)
        }
        if len(rows) == 0 && raw != "[]" {
            t.Fatalf("unexpected empty result for %q", raw)
        }
    }
}

func TestStderrBudgetStopsUnboundedDiagnostics(t *testing.T) {
    output := boundedStderr()
    if _, err := output.Write(bytes.Repeat([]byte("x"), maxStderrBytes)); err != nil {
        t.Fatal(err)
    }
    if _, err := output.Write([]byte("z")); !errors.Is(err, errOutputLimit) {
        t.Fatalf("expected stderr limit error, got %v", err)
    }
    if output.Len() != maxStderrBytes {
        t.Fatalf("expected %d buffered diagnostic bytes, got %d", maxStderrBytes, output.Len())
    }
}

func TestNamedParameterLimitsMatchNativeCLI(t *testing.T) {
    client := Client{}
    params := make(map[string]string)
    for index := 0; index < 65; index++ {
        params[fmt.Sprintf("p%d", index)] = "int64:1"
    }
    if _, err := client.arguments("SELECT 1", Options{NamedParams: params}); err == nil {
        t.Fatal("more than 64 parameters must be rejected")
    }
    if _, err := client.arguments("SELECT 1", Options{NamedParams: map[string]string{
        "long": "utf8:" + strings.Repeat("x", 8192),
    }}); err == nil {
        t.Fatal("parameter longer than 8192 bytes must be rejected")
    }
    if _, err := client.arguments("SELECT 1", Options{NamedParams: map[string]string{
        "max": "utf8:" + strings.Repeat("x", 8192-len("utf8:")),
    }}); err != nil {
        t.Fatalf("valid maximum-length parameter rejected: %v", err)
    }
}

func TestDecodeRowsRejectsDuplicateJSONKeysAtEveryDepth(t *testing.T) {
    invalid := []string{
        `[{"id":1,"id":2}]`,
        `[{"nested":{"id":1,"id":2}}]`,
        `[{"nested":[{"name":"a","name":"b"}]}]`,
        `[{"a":1,"\u0061":2}]`,
    }
    for _, raw := range invalid {
        if _, err := decodeRows(strings.NewReader(raw)); err == nil ||
            !strings.Contains(err.Error(), "duplicate") {
            t.Errorf("expected duplicate-key error for %q, got %v", raw, err)
        }
    }
    rows, err := decodeRows(strings.NewReader(`[{"nested":[{"id":1},{"id":2}],"n":1234567890123456789}]`))
    if err != nil { t.Fatal(err) }
    if _, ok := rows[0]["n"].(json.Number); !ok {
        t.Fatalf("numeric precision was not retained: %#v", rows[0]["n"])
    }
}

func TestDecodeRowsBoundsJSONNesting(t *testing.T) {
    valid := `[{"nested":` + strings.Repeat("[", maxJSONNestingDepth-2) +
        `0` + strings.Repeat("]", maxJSONNestingDepth-2) + `}]`
    if _, err := decodeRows(strings.NewReader(valid)); err != nil {
        t.Fatalf("rejected JSON within nesting bound: %v", err)
    }
    invalid := `[{"nested":` + strings.Repeat("[", maxJSONNestingDepth) +
        `0` + strings.Repeat("]", maxJSONNestingDepth) + `}]`
    if _, err := decodeRows(strings.NewReader(invalid)); err == nil || !strings.Contains(err.Error(), "nesting limit") {
        t.Fatalf("expected nesting limit error, got %v", err)
    }
}

func TestDecodeRowsRejectsNonfiniteJSONExponents(t *testing.T) {
    for _, raw := range []string{
        `[{"value":1e999}]`,
        `[{"value":-1e999}]`,
        `[{"value":{"nested":1e999}}]`,
    } {
        if _, err := decodeRows(strings.NewReader(raw)); err == nil ||
            !strings.Contains(err.Error(), "nonfinite") {
            t.Fatalf("expected exponent error for %q, got %v", raw, err)
        }
    }
    rows, err := decodeRows(strings.NewReader(`[{"value":1e308,"id":9007199254740993}]`))
    if err != nil { t.Fatal(err) }
    if rows[0]["id"].(json.Number).String() != "9007199254740993" {
        t.Fatalf("lost large integer precision: %#v", rows[0]["id"])
    }
}


func TestDecodeRowsStreamsIntoTypedRowSlice(t *testing.T) {
    var wire strings.Builder
    wire.WriteByte('[')
    for n := 0; n < 10_000; n++ {
        if n > 0 { wire.WriteByte(',') }
        fmt.Fprintf(&wire, `{"n":%d,"metadata":{"nullable":null}}`, n)
    }
    wire.WriteByte(']')
    rows, err := decodeRows(strings.NewReader(wire.String()))
    if err != nil { t.Fatal(err) }
    if len(rows) != 10_000 { t.Fatalf("lost rows: %d", len(rows)) }
    if rows[9_999]["n"].(json.Number).String() != "9999" {
        t.Fatalf("lost integer precision: %#v", rows[9_999])
    }
}

func TestDecodeRowsFailsClosedOnTruncatedOrTrailingArrays(t *testing.T) {
    for _, wire := range []string{
        `[{"n":1}`, `[{"n":1}][{"n":2}]`, `[{"n":1}]x`,
        `{"n":1}`, `[{},null]`, `[{"n":1},"bad"]`,
    } {
        if rows, err := decodeRows(strings.NewReader(wire)); err == nil {
            t.Fatalf("accepted malformed result %q: %#v", wire, rows)
        }
    }
}
