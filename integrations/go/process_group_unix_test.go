//go:build unix

package rustsparklocal

import (
    "bytes"
    "context"
    "errors"
    "os"
    "os/exec"
    "path/filepath"
    "testing"
    "time"
)

func TestInheritedCLIPipesCannotHangQuery(t *testing.T) {
    executable := filepath.Join(t.TempDir(), "fake-rustspark")
    script := "#!/bin/sh\nsleep 10 &\nprintf '[{\"id\":1}]'\n"
    if err := os.WriteFile(executable, []byte(script), 0700); err != nil {
        t.Fatal(err)
    }
    client := Client{Binary: executable}
    start := time.Now()
    _, err := client.Query(context.Background(), "SELECT 1", Options{})
    if !errors.Is(err, exec.ErrWaitDelay) {
        t.Fatalf("expected inherited-pipe timeout, got %v", err)
    }
    if elapsed := time.Since(start); elapsed > 6*time.Second {
        t.Fatalf("inherited stdout blocked buffered query for %v", elapsed)
    }

    var output bytes.Buffer
    start = time.Now()
    err = client.QueryNDJSON(context.Background(), "SELECT 1", Options{}, &output)
    if !errors.Is(err, exec.ErrWaitDelay) {
        t.Fatalf("expected inherited-pipe timeout in stream, got %v", err)
    }
    if elapsed := time.Since(start); elapsed > 6*time.Second {
        t.Fatalf("inherited stdout blocked stream for %v", elapsed)
    }
}
