import { spawnSync } from "node:child_process";

export class RustSparkError extends Error {
  constructor(message) {
    super(message);
    this.name = "RustSparkError";
  }
}

function addNamedTables(args, flag, tables = {}) {
  for (const [table, path] of Object.entries(tables).sort(([a], [b]) => a.localeCompare(b, "en"))) {
    if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(table) || typeof path !== "string" || path.length === 0) {
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
    namedParams, sqlFile, maxRows, queryTimeout, processTimeoutMs,
    maxOutputBytes = 64 * 1024 * 1024,
  } = {}) {
    if (typeof sql !== "string" || (sql.trim() === "") === (sqlFile === undefined)) {
      throw new TypeError("Provide exactly one nonempty SQL string or sqlFile");
    }
    if (sqlFile !== undefined && (typeof sqlFile !== "string" || sqlFile.trim() === "")) {
      throw new TypeError("sqlFile must be a nonempty file path");
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

    const args = sqlFile !== undefined ? ["--sql-file", sqlFile, "--stdout-format", "json-array"] : ["--sql", sql, "--stdout-format", "json-array"];
    if (this.databaseDir !== null) args.push("--database-dir", String(this.databaseDir));
    for (const [key, value] of Object.entries(unnamed)) {
      if (value !== undefined && value !== null) args.push(`--${key}`, String(value));
    }
    addNamedTables(args, "--csv-table", csvTables);
    addNamedTables(args, "--parquet-table", parquetTables);
    addNamedTables(args, "--json-table", jsonTables);
    if (Object.keys(namedParams ?? {}).length > 64) {
      throw new RangeError("Too many typed SQL parameters");
    }
    for (const [name, value] of Object.entries(namedParams ?? {}).sort(([a], [b]) => a.localeCompare(b, "en"))) {
      if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(name) || typeof value !== "string" || !value.includes(":") || Buffer.byteLength(value, "utf8") > 8192) {
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
    try {
      rows = JSON.parse(result.stdout, (_key, value) => {
        if (typeof value === "number" && !Number.isFinite(value)) {
          throw new RangeError("non-finite SQL JSON number");
        }
        if (typeof value === "number" && Number.isInteger(value) && !Number.isSafeInteger(value)) {
          throw new RangeError("SQL integer cannot be represented safely in JavaScript");
        }
        return value;
      });
    }
    catch { throw new RustSparkError("RustSpark returned invalid JSON"); }
    if (!Array.isArray(rows) || rows.some((row) => row === null || typeof row !== "object" || Array.isArray(row))) {
      throw new RustSparkError("RustSpark returned an unexpected JSON result");
    }
    return rows;
  }
  queryOne(sql, options = {}) {
    if (options.maxRows !== undefined) {
      throw new TypeError("queryOne sets maxRows automatically");
    }
    const rows = this.query(sql, { ...options, maxRows: 2 });
    if (rows.length > 1) throw new RustSparkError("queryOne expected at most one row");
    return rows[0] ?? null;
  }
}
