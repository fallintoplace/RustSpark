package rustsparklocal

import (
    "bytes"
    "context"
    "errors"
    "io"
    "strings"
    "testing"
)

type shortNDJSONWriter struct {
    bytes.Buffer
}

func (w *shortNDJSONWriter) Write(data []byte) (int, error) {
    if len(data) > 1 {
        data = data[:1]
    }
    return w.Buffer.Write(data)
}

func TestNDJSONByteCapRejectsSilentShortWrites(t *testing.T) {
    underlying := &shortNDJSONWriter{}
    writer := &ndjsonByteLimit{output: underlying, remaining: 5}
    count, err := writer.Write([]byte("abcd"))
    if count != 1 || !errors.Is(err, io.ErrShortWrite) {
        t.Fatalf("short write = %d, %v", count, err)
    }
    if writer.remaining != 4 || underlying.String() != "a" {
        t.Fatalf("short write must debit only emitted bytes: remaining %d", writer.remaining)
    }
}

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
