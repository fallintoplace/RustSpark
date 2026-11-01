package rustsparklocal

import "io"

// ndjsonByteLimit is an opt-in streaming budget, not an in-memory buffer.
// Refuse a write before it can exceed the configured total byte count.
type ndjsonByteLimit struct {
    output io.Writer
    remaining int
    onLimit func()
    exceeded bool
}

func (w *ndjsonByteLimit) Write(data []byte) (int, error) {
    if len(data) > w.remaining {
        w.exceeded = true
        if w.onLimit != nil { w.onLimit() }
        return 0, errOutputLimit
    }
    written, err := w.output.Write(data)
    w.remaining -= written
    if written < len(data) && err == nil {
        return written, io.ErrShortWrite
    }
    return written, err
}
