package rustsparklocal

import (
    "bytes"
    "context"
    "errors"
    "strings"
    "testing"
)

func TestNDJSONOptInByteCapStopsWrites(t *testing.T) {
    var captured bytes.Buffer
    writer := &ndjsonByteLimit{output: &captured, remaining: 6}
    if count, err := writer.Write([]byte("ab")); err != nil || count != 2 {
        t.Fatalf("first write = %d, %v", count, err)
    }
    if count, err := writer.Write([]byte("cd")); err != nil || count != 2 {
        t.Fatalf("second write = %d, %v", count, err)
    }
    if count, err := writer.Write([]byte("123")); count != 0 || !errors.Is(err, errOutputLimit) {
        t.Fatalf("overflow write = %d, %v", count, err)
    }
    if count, err := writer.Write([]byte("!")); err != nil || count != 1 {
        t.Fatalf("valid write after rejected chunk = %d, %v", count, err)
    }
    if captured.String() != "abcd!" {
        t.Fatalf("unexpected output %q", captured.String())
    }
}

func TestQueryNDJSONLimitsProcessOutputWhenRequested(t *testing.T) {
    client := Client{Binary: "echo"}
    sql := "SELECT " + strings.Repeat("x", 1000)
    var limited bytes.Buffer
    err := client.QueryNDJSON(context.Background(), sql,
        Options{MaxOutputBytes: 4}, &limited)
    if !errors.Is(err, errOutputLimit) {
        t.Fatalf("expected stdout budget failure, got %v", err)
    }
    if limited.Len() > 4 {
        t.Fatalf("wrote %d bytes beyond NDJSON budget", limited.Len())
    }

    var normal bytes.Buffer
    if err := client.QueryNDJSON(context.Background(), sql, Options{}, &normal); err != nil {
        t.Fatal(err)
    }
    if !strings.Contains(normal.String(), sql) {
        t.Fatal("unlimited stream was truncated")
    }
}
