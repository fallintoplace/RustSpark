package rustsparklocal

import (
    "bufio"
    "bytes"
    "context"
    "encoding/json"
    "errors"
    "fmt"
    "io"
)

const maxNDJSONRowBytes = 16 * 1024 * 1024

// scanNDJSONRows checks complete JSON object rows before calling the visitor.
// The line bound prevents a single untrusted result from exhausting memory.
func scanNDJSONRows(input io.Reader, visit func(map[string]any) error) error {
    scanner := bufio.NewScanner(input)
    // Scanner needs room for the line ending in addition to row bytes.
    scanner.Buffer(make([]byte, 64*1024), maxNDJSONRowBytes+2)
    index := 0
    for scanner.Scan() {
        index++
        if len(scanner.Bytes()) > maxNDJSONRowBytes {
            return fmt.Errorf("RustSpark NDJSON row %d exceeds %d bytes", index, maxNDJSONRowBytes)
        }
        line := bytes.TrimSpace(scanner.Bytes())
        if len(line) == 0 {
            return fmt.Errorf("RustSpark NDJSON row %d is blank", index)
        }
        decoder := json.NewDecoder(bytes.NewReader(line))
        decoder.UseNumber()
        item, err := decodeUniqueValue(decoder, 0)
        if err != nil {
            return fmt.Errorf("invalid RustSpark NDJSON row %d: %w", index, err)
        }
        row, ok := item.(map[string]any)
        if !ok {
            return fmt.Errorf("RustSpark NDJSON row %d must be an object", index)
        }
        if _, err := decoder.Token(); err == nil {
            return fmt.Errorf("RustSpark NDJSON row %d contains trailing JSON", index)
        } else if !errors.Is(err, io.EOF) {
            return fmt.Errorf("invalid RustSpark NDJSON row %d: %w", index, err)
        }
        if err := visit(row); err != nil {
            return err
        }
    }
    if err := scanner.Err(); err != nil {
        return fmt.Errorf("reading RustSpark NDJSON: %w", err)
    }
    return nil
}

// QueryEach streams checked JSON object rows to visit without collecting all
// results in memory. If visit fails or the JSON is malformed, it cancels the
// subprocess before returning the error. Rows larger than 16 MiB are rejected.
func (c Client) QueryEach(
    ctx context.Context, sql string, opts Options,
    visit func(map[string]any) error,
) error {
    if visit == nil {
        return errors.New("RustSpark NDJSON visitor must not be nil")
    }
    if _, err := c.arguments(sql, opts); err != nil {
        return err
    }
    childCtx, cancel := context.WithCancel(ctx)
    defer cancel()

    reader, writer := io.Pipe()
    defer reader.Close()
    done := make(chan error, 1)
    go func() {
        err := c.QueryNDJSON(childCtx, sql, opts, writer)
        _ = writer.CloseWithError(err)
        done <- err
    }()

    err := scanNDJSONRows(reader, visit)
    if err != nil {
        cancel()
        _ = reader.CloseWithError(err)
        <-done
        return err
    }
    return <-done
}
