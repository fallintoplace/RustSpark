package rustsparklocal

import (
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

func TestOneRowBoundsCardinality(t *testing.T) {
    row, err := oneRow(nil)
    if err != nil || row != nil { t.Fatalf("expected empty result, got %v: %v", row, err) }
    row, err = oneRow([]map[string]any{{"id": 1}})
    if err != nil || row["id"] != 1 { t.Fatalf("unexpected row %v: %v", row, err) }
    _, err = oneRow([]map[string]any{{"id": 1}, {"id": 2}})
    if err == nil { t.Fatal("expected multi-row error") }
}
