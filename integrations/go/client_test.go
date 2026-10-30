package rustsparklocal

import (
    "bytes"
    "context"
    "errors"
    "io"
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

type failingStreamWriter struct{}

func (failingStreamWriter) Write([]byte) (int, error) {
    return 0, io.ErrClosedPipe
}

func TestNDJSONStopsOnOutputFailure(t *testing.T) {
    client := Client{Binary: "echo"}
    err := client.QueryNDJSON(context.Background(), "SELECT 1", Options{}, failingStreamWriter{})
    if !errors.Is(err, io.ErrClosedPipe) {
        t.Fatalf("expected the output writer error, got %v", err)
    }
}
