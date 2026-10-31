// Package rustsparklocal invokes the local RustSpark CLI with typed bindings.
// It is not a network driver and does not claim Spark API compatibility.
package rustsparklocal

import (
    "bytes"
    "context"
    "encoding/json"
    "errors"
    "fmt"
    "io"
    "os/exec"
    "regexp"
    "sort"
    "strconv"
    "strings"
)

var identifier = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*\z`)

var errOutputLimit = errors.New("RustSpark stdout exceeded MaxOutputBytes")

// boundedOutput stops JSON accumulation when a large row exceeds the byte budget.
type boundedOutput struct {
    bytes.Buffer
    limit int
}

func (output *boundedOutput) Write(chunk []byte) (int, error) {
    if len(chunk) > output.limit-output.Len() {
        return 0, errOutputLimit
    }
    return output.Buffer.Write(chunk)
}

// Keep child diagnostics bounded even if a backend crashes in a tight loop.
const maxStderrBytes = 64 * 1024

func boundedStderr() boundedOutput {
    return boundedOutput{limit: maxStderrBytes}
}

// Client calls an installed RustSpark binary without a shell.
type Client struct {
    Binary      string
    DatabaseDir string
}

type Options struct {
    Input, CSV, Parquet, JSON string
    SQLFile string
    CSVTables, ParquetTables, JSONTables map[string]string
    NamedParams map[string]string
    MaxRows int
    QueryTimeoutSeconds int
    MaxOutputBytes int
}

func addNamed(args []string, flag string, tables map[string]string) ([]string, error) {
    keys := make([]string, 0, len(tables))
    for name := range tables {
        keys = append(keys, name)
    }
    sort.Strings(keys)
    for _, name := range keys {
        if !identifier.MatchString(name) || tables[name] == "" {
            return nil, fmt.Errorf("invalid table definition: %q", name)
        }
        args = append(args, flag, name+"="+tables[name])
    }
    return args, nil
}

func (c Client) arguments(sql string, opts Options) ([]string, error) {
    if (strings.TrimSpace(sql) == "") == (opts.SQLFile == "") {
        return nil, errors.New("provide exactly one SQL text or SQLFile")
    }
    count := 0
    args := make([]string, 0, 8)
    if opts.SQLFile != "" {
        args = append(args, "--sql-file", opts.SQLFile)
    } else {
        args = append(args, "--sql", sql)
    }
    args = append(args, "--stdout-format", "json-array")
    if c.DatabaseDir != "" {
        args = append(args, "--database-dir", c.DatabaseDir)
    }
    for _, source := range []struct{ flag, value string }{
        {"--input", opts.Input}, {"--csv", opts.CSV},
        {"--parquet", opts.Parquet}, {"--json", opts.JSON},
    } {
        if source.value != "" {
            count++
            args = append(args, source.flag, source.value)
        }
    }
    if count > 1 {
        return nil, errors.New("only one unnamed file input is supported")
    }
    if count > 0 && (len(opts.CSVTables)+len(opts.ParquetTables)+len(opts.JSONTables) > 0) {
        return nil, errors.New("named and unnamed inputs cannot be mixed")
    }
    var err error
    if args, err = addNamed(args, "--csv-table", opts.CSVTables); err != nil { return nil, err }
    if args, err = addNamed(args, "--parquet-table", opts.ParquetTables); err != nil { return nil, err }
    if args, err = addNamed(args, "--json-table", opts.JSONTables); err != nil { return nil, err }
    if len(opts.NamedParams) > 64 {
        return nil, errors.New("too many typed SQL parameters (maximum 64)")
    }
    names := make([]string, 0, len(opts.NamedParams))
    for name := range opts.NamedParams { names = append(names, name) }
    sort.Strings(names)
    for _, name := range names {
        typedValue := opts.NamedParams[name]
        if !identifier.MatchString(name) || !strings.Contains(typedValue, ":") || len(typedValue) > 8192 {
            return nil, fmt.Errorf("invalid typed parameter: %q", name)
        }
        args = append(args, "--named-param", name+"="+typedValue)
    }
    if opts.MaxRows < 0 || opts.QueryTimeoutSeconds < 0 || opts.MaxOutputBytes < 0 {
        return nil, errors.New("max rows, query timeout and output byte limit must not be negative")
    }
    if opts.MaxRows > 0 {
        args = append(args, "--max-rows", strconv.Itoa(opts.MaxRows))
    }
    if opts.QueryTimeoutSeconds > 0 {
        args = append(args, "--query-timeout", strconv.Itoa(opts.QueryTimeoutSeconds))
    }
    return args, nil
}

// Query returns JSON object rows with json.Number values preserved.
// ctx can cancel planning, file registration or execution.
func (c Client) Query(ctx context.Context, sql string, opts Options) ([]map[string]any, error) {
    args, err := c.arguments(sql, opts)
    if err != nil { return nil, err }
    binary := c.Binary
    if binary == "" { binary = "rustspark" }
    command := exec.CommandContext(ctx, binary, args...)
    output := boundedOutput{limit: opts.MaxOutputBytes}
    if output.limit == 0 { output.limit = 64 * 1024 * 1024 }
    stderr := boundedStderr()
    command.Stdout, command.Stderr = &output, &stderr
    if err := command.Run(); err != nil {
        if ctx.Err() != nil { return nil, ctx.Err() }
        return nil, fmt.Errorf("RustSpark query failed: %w: %s", err, strings.TrimSpace(stderr.String()))
    }
    return decodeRows(&output)
}

// decodeUniqueValue reads a JSON value while rejecting duplicate object keys,
// including duplicate names expressed using different JSON escape sequences.
func decodeUniqueValue(decoder *json.Decoder) (any, error) {
    token, err := decoder.Token()
    if err != nil { return nil, err }
    if number, ok := token.(json.Number); ok && strings.ContainsAny(string(number), ".eE") {
        if _, err := strconv.ParseFloat(string(number), 64); err != nil {
            return nil, fmt.Errorf("nonfinite RustSpark JSON number %q: %w", number, err)
        }
    }
    delim, isDelim := token.(json.Delim)
    if !isDelim { return token, nil }
    switch delim {
    case '{':
        object := make(map[string]any)
        for decoder.More() {
            nameToken, err := decoder.Token()
            if err != nil { return nil, err }
            name, ok := nameToken.(string)
            if !ok { return nil, errors.New("invalid RustSpark JSON object key") }
            if _, exists := object[name]; exists {
                return nil, fmt.Errorf("duplicate RustSpark JSON key %q", name)
            }
            value, err := decodeUniqueValue(decoder)
            if err != nil { return nil, err }
            object[name] = value
        }
        if _, err := decoder.Token(); err != nil { return nil, err }
        return object, nil
    case '[':
        array := make([]any, 0)
        for decoder.More() {
            value, err := decodeUniqueValue(decoder)
            if err != nil { return nil, err }
            array = append(array, value)
        }
        if _, err := decoder.Token(); err != nil { return nil, err }
        return array, nil
    default:
        return nil, errors.New("unexpected RustSpark JSON closing delimiter")
    }
}

// decodeRows validates every row object and retains json.Number values without
// silently accepting repeated JSON keys or trailing output.
func decodeRows(output io.Reader) ([]map[string]any, error) {
    decoder := json.NewDecoder(output)
    decoder.UseNumber()
    value, err := decodeUniqueValue(decoder)
    if err != nil { return nil, fmt.Errorf("invalid RustSpark JSON output: %w", err) }
    values, ok := value.([]any)
    if !ok { return nil, errors.New("RustSpark JSON result must be an array") }
    rows := make([]map[string]any, 0, len(values))
    for index, item := range values {
        row, ok := item.(map[string]any)
        if !ok { return nil, fmt.Errorf("RustSpark JSON result row %d must be an object", index) }
        rows = append(rows, row)
    }
    if _, err := decoder.Token(); err != nil {
        if errors.Is(err, io.EOF) { return rows, nil }
        return nil, fmt.Errorf("unexpected trailing output: %w", err)
    }
    return nil, errors.New("unexpected additional JSON output")
}

// QueryOne returns nil for no rows and an error when more than one row exists.
// A two-row CLI limit bounds result collection for ordinary uniqueness lookups.
func (c Client) QueryOne(ctx context.Context, sql string, opts Options) (map[string]any, error) {
    if opts.MaxRows != 0 {
        return nil, errors.New("QueryOne controls MaxRows automatically")
    }
    opts.MaxRows = 2
    rows, err := c.Query(ctx, sql, opts)
    if err != nil { return nil, err }
    return oneRow(rows)
}

func oneRow(rows []map[string]any) (map[string]any, error) {
    if len(rows) > 1 { return nil, errors.New("QueryOne expected at most one row") }
    if len(rows) == 0 { return nil, nil }
    return rows[0], nil
}

// QueryNDJSON streams newline-delimited JSON rows without buffering the result.
// MaxOutputBytes optionally bounds bytes sent to the output writer; zero means
// unlimited streaming. An output write failure stops the query subprocess.
func (c Client) QueryNDJSON(ctx context.Context, sql string, opts Options, output io.Writer) error {
    if output == nil { return errors.New("NDJSON output writer must not be nil") }
    args, err := c.arguments(sql, opts)
    if err != nil { return err }
    for index := 0; index+1 < len(args); index++ {
        if args[index] == "--stdout-format" {
            args[index+1] = "json"
            break
        }
    }
    binary := c.Binary
    if binary == "" { binary = "rustspark" }
    command := exec.CommandContext(ctx, binary, args...)
    if opts.MaxOutputBytes > 0 {
        command.Stdout = &ndjsonByteLimit{output: output, remaining: opts.MaxOutputBytes}
    } else {
        command.Stdout = output
    }
    stderr := boundedStderr()
    command.Stderr = &stderr
    if err := command.Run(); err != nil {
        if ctx.Err() != nil { return ctx.Err() }
        return fmt.Errorf("RustSpark NDJSON query failed: %w: %s", err, strings.TrimSpace(stderr.String()))
    }
    return nil
}
