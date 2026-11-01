package rustsparklocal

import (
    "context"
    "encoding/json"
    "errors"
    "fmt"
    "os"
    "path/filepath"
    "strings"
    "testing"
)

func TestScanNDJSONRowsRetainsNumbersAndRejectsMalformedInput(t *testing.T) {
    var seen []map[string]any
    err := scanNDJSONRows(strings.NewReader(
        "{\"id\":9223372036854775807,\"nested\":[{\"n\":4}]}\n{\"id\":2}\r\n",
    ), func(row map[string]any) error {
        seen = append(seen, row)
        return nil
    })
    if err != nil { t.Fatal(err) }
    if len(seen) != 2 || seen[0]["id"].(json.Number).String() != "9223372036854775807" {
        t.Fatalf("lost row data: %#v", seen)
    }
    for _, input := range []string{
        "null\n", "[]\n", "{}\n\n", "{\"x\":1,\"x\":2}\n",
        "{\"nested\":{\"x\":1,\"x\":2}}\n", "{\"n\":1e999}\n",
        "{\"id\":1} {\"id\":2}\n",
        "{\"x\":1}\n" + strings.Repeat(" ", maxNDJSONRowBytes) + "\n",
    } {
        if err := scanNDJSONRows(strings.NewReader(input), func(map[string]any) error {
            return nil
        }); err == nil {
            t.Errorf("accepted malformed NDJSON %q", input[:min(len(input), 40)])
        }
    }
}

func TestScanNDJSONRowsPropagatesVisitorErrorWithoutReadingMore(t *testing.T) {
    sentinel := errors.New("stop")
    count := 0
    err := scanNDJSONRows(strings.NewReader("{}\n{}\n"), func(map[string]any) error {
        count++
        return sentinel
    })
    if !errors.Is(err, sentinel) || count != 1 {
        t.Fatalf("unexpected visit result: %d %v", count, err)
    }
}

func TestQueryEachStopsChildWhenVisitorFails(t *testing.T) {
    if os.PathSeparator != '/' { t.Skip("test helper uses POSIX executable script") }
    path := filepath.Join(t.TempDir(), "fake-rustspark")
    script := "#!/bin/sh\nprintf '%s\\n' '{\"id\":1}'\nwhile :; do printf '%s\\n' '{\"id\":2}'; done\n"
    if err := os.WriteFile(path, []byte(script), 0700); err != nil { t.Fatal(err) }
    client := Client{Binary: path}
    sentinel := fmt.Errorf("done")
    count := 0
    err := client.QueryEach(context.Background(), "SELECT 1", Options{}, func(row map[string]any) error {
        count++
        return sentinel
    })
    if !errors.Is(err, sentinel) || count != 1 {
        t.Fatalf("expected early cancellation, got %d rows: %v", count, err)
    }
    if err := client.QueryEach(context.Background(), "SELECT 1", Options{}, nil); err == nil {
        t.Fatal("nil visitor accepted")
    }
}

func TestScanNDJSONRowsAcceptsExactByteLimit(t *testing.T) {
    prefix := `{"data":"`
    suffix := `"}`
    size := maxNDJSONRowBytes - len(prefix) - len(suffix)
    exact := prefix + strings.Repeat("a", size) + suffix
    seen := 0
    err := scanNDJSONRows(strings.NewReader(exact+"\r\n"), func(row map[string]any) error {
        seen++
        if len(row["data"].(string)) != size {
            t.Fatal("lost the boundary row payload")
        }
        return nil
    })
    if err != nil || seen != 1 {
        t.Fatalf("exactly 16 MiB should fit: seen=%d, err=%v", seen, err)
    }
    if err := scanNDJSONRows(strings.NewReader(exact+" "+"\n"), func(map[string]any) error {
        t.Fatal("oversized row reached visitor")
        return nil
    }); err == nil {
        t.Fatal("expected oversized row to be rejected")
    }
}


func TestScanNDJSONRowsRejectsNonJSONWhitespace(t *testing.T) {
    for _, raw := range []string{
        "\u00a0{\"n\":1}\n", "{\"n\":1}\u00a0\n",
        "\u2028{\"n\":1}\n", "{\"n\":1}\u2003\n",
    } {
        if err := scanNDJSONRows(strings.NewReader(raw),
            func(map[string]any) error { return nil }); err == nil {
            t.Fatalf("accepted Unicode whitespace outside JSON: %q", raw)
        }
    }
    visits := 0
    if err := scanNDJSONRows(strings.NewReader(" \t{\"n\":1} \r\n"),
        func(map[string]any) error { visits++; return nil }); err != nil || visits != 1 {
        t.Fatalf("rejected valid JSON whitespace: visits=%d err=%v", visits, err)
    }
}
