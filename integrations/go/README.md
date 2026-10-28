# RustSpark Go client

An experimental, dependency-free Go subprocess client for the local CLI.
Go 1.23+ and a RustSpark executable are required.

```go
package main

import (
  "context"
  "fmt"
  rustspark "github.com/fallintoplace/RustSpark/integrations/go"
)

func main() {
  client := rustspark.Client{Binary: "rustspark"}
  rows, err := client.Query(context.Background(),
    "SELECT city, COUNT(*) AS n FROM data GROUP BY city",
    rustspark.Options{CSV: "tests/fixtures/sales.csv", MaxRows: 100})
  if err != nil { panic(err) }
  fmt.Println(rows)
}
```

Results use `json.Number` so 64-bit integral values do not silently
lose precision through Go's default float64 JSON handling.
Use `NamedParams` for typed `$name` SQL bindings. Each argument is
passed directly through `exec.CommandContext`, never through a shell.

Query rows are decoded into memory. Use RustSpark's Arrow or NDJSON CLI
streaming for large datasets. Persistent databases remain single-writer.

```sh
cd integrations/go && go test ./...
```
