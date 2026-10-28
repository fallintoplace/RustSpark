import { spawnSync } from "node:child_process";

export class RustSparkError extends Error {
  constructor(message) {
    super(message);
    this.name = "RustSparkError";
  }
}

function addNamedTables(args, flag, tables = {}) {
  for (const [table, path] of Object.entries(tables)) {
    if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(table) || typeof path !== "string") {
      throw new TypeError("Table names must be SQL identifiers and paths must be strings");
    }
    args.push(flag, `${table}=${path}`);
  }
}

export class RustSpark {
  constructor({ binary = "rustspark", databaseDir = null, runner = spawnSync } = {}) {
    this.binary = binary;
    this.databaseDir = databaseDir;
    this.runner = runner;
  }

  query(sql, {
    input, csv, parquet, json,
    csvTables, parquetTables, jsonTables,
    namedParams, maxRows, queryTimeout, processTimeoutMs,
    maxOutputBytes = 64 * 1024 * 1024,
  } = {}) {
    if (typeof sql !== "string" || !sql.trim()) {
      throw new TypeError("SQL must be a nonempty string");
    }
    const unnamed = { input, csv, parquet, json };
    const selected = Object.values(unnamed).filter((value) => value != null);
    const hasNamed = [csvTables, parquetTables, jsonTables]
      .some((tables) => tables && Object.keys(tables).length > 0);
    if (selected.length > 1 || (selected.length && hasNamed)) {
      throw new TypeError("Use one unnamed input or named tables");
    }
    if (maxRows !== undefined && (!Number.isSafeInteger(maxRows) || maxRows < 0)) {
      throw new RangeError("maxRows must be a nonnegative safe integer");
    }
    if (queryTimeout !== undefined && (!Number.isSafeInteger(queryTimeout) || queryTimeout <= 0)) {
      throw new RangeError("queryTimeout must be a positive integer of seconds");
    }
    if (processTimeoutMs !== undefined && (!Number.isSafeInteger(processTimeoutMs) || processTimeoutMs <= 0)) {
      throw new RangeError("processTimeoutMs must be a positive integer of milliseconds");
    }
    if (!Number.isSafeInteger(maxOutputBytes) || maxOutputBytes <= 0) {
      throw new RangeError("maxOutputBytes must be positive");
    }

    const args = ["--sql", sql, "--stdout-format", "json-array"];
    if (this.databaseDir !== null) args.push("--database-dir", String(this.databaseDir));
    for (const [key, value] of Object.entries(unnamed)) {
      if (value !== undefined && value !== null) args.push(`--${key}`, String(value));
    }
    addNamedTables(args, "--csv-table", csvTables);
    addNamedTables(args, "--parquet-table", parquetTables);
    addNamedTables(args, "--json-table", jsonTables);
    for (const [name, value] of Object.entries(namedParams ?? {})) {
      if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(name) || typeof value !== "string" || !value.includes(":")) {
        throw new TypeError(`Invalid typed SQL parameter: ${name}`);
      }
      args.push("--named-param", `${name}=${value}`);
    }
    if (maxRows !== undefined) args.push("--max-rows", String(maxRows));
    if (queryTimeout !== undefined) args.push("--query-timeout", String(queryTimeout));

    const result = this.runner(this.binary, args, {
      encoding: "utf8", shell: false, timeout: processTimeoutMs, maxBuffer: maxOutputBytes,
    });
    if (result.error) throw new RustSparkError(`Failed to run RustSpark: ${result.error.message}`);
    if (result.status !== 0) {
      throw new RustSparkError((result.stderr ?? "").trim() || `RustSpark exited with ${result.status}`);
    }
    let rows;
    try { rows = JSON.parse(result.stdout); }
    catch { throw new RustSparkError("RustSpark returned invalid JSON"); }
    if (!Array.isArray(rows) || rows.some((row) => row === null || typeof row !== "object" || Array.isArray(row))) {
      throw new RustSparkError("RustSpark returned an unexpected JSON result");
    }
    return rows;
  }
}
