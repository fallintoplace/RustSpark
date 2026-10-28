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

var identifier = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// Client calls an installed RustSpark binary without a shell.
type Client struct {
    Binary      string
    DatabaseDir string
}

type Options struct {
    Input, CSV, Parquet, JSON string
    CSVTables, ParquetTables, JSONTables map[string]string
    NamedParams map[string]string
    MaxRows int
    QueryTimeoutSeconds int
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
    if strings.TrimSpace(sql) == "" {
        return nil, errors.New("SQL cannot be empty")
    }
    count := 0
    args := []string{"--sql", sql, "--stdout-format", "json-array"}
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
    names := make([]string, 0, len(opts.NamedParams))
    for name := range opts.NamedParams { names = append(names, name) }
    sort.Strings(names)
    for _, name := range names {
        typedValue := opts.NamedParams[name]
        if !identifier.MatchString(name) || !strings.Contains(typedValue, ":") {
            return nil, fmt.Errorf("invalid typed parameter: %q", name)
        }
        args = append(args, "--named-param", name+"="+typedValue)
    }
    if opts.MaxRows < 0 || opts.QueryTimeoutSeconds < 0 {
        return nil, errors.New("max rows and query timeout must not be negative")
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
    var output, stderr bytes.Buffer
    command.Stdout, command.Stderr = &output, &stderr
    if err := command.Run(); err != nil {
        if ctx.Err() != nil { return nil, ctx.Err() }
        return nil, fmt.Errorf("RustSpark query failed: %w: %s", err, strings.TrimSpace(stderr.String()))
    }
    decoder := json.NewDecoder(&output)
    decoder.UseNumber()
    var rows []map[string]any
    if err := decoder.Decode(&rows); err != nil {
        return nil, fmt.Errorf("invalid RustSpark JSON output: %w", err)
    }
    var extra any
    if err := decoder.Decode(&extra); err != nil {
        if errors.Is(err, io.EOF) { return rows, nil }
        return nil, fmt.Errorf("unexpected trailing output: %w", err)
    }
    return nil, errors.New("unexpected additional JSON output")
}
