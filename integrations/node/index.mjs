import { spawn, spawnSync } from "node:child_process";
import { TextDecoder } from "node:util";
import { parseStrictJSON } from "./strict-json.mjs";

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

  _command(sql, {
    input, csv, parquet, json,
    csvTables, parquetTables, jsonTables,
    namedParams, sqlFile, maxRows, queryTimeout, processTimeoutMs,
    maxOutputBytes = 64 * 1024 * 1024,
    unsafeIntegers = "error",
  } = {}) {
    const hasInlineSQL = typeof sql === "string" && sql.trim().length > 0;
    const hasSQLFile = sqlFile !== undefined;
    if (hasInlineSQL === hasSQLFile || (sql != null && typeof sql !== "string")) {
      throw new TypeError("Provide exactly one nonempty SQL string or sqlFile");
    }
    if (sqlFile !== undefined && (typeof sqlFile !== "string" || sqlFile.trim() === "")) {
      throw new TypeError("sqlFile must be a nonempty file path");
    }
    const unnamed = { input, csv, parquet, json };
    const selected = Object.values(unnamed).filter((value) => value != null);
    if (selected.some((value) => typeof value !== "string" || value.length === 0)) {
      throw new TypeError("Unnamed file paths must be nonempty strings");
    }
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
    if (unsafeIntegers !== "error" && unsafeIntegers !== "bigint") {
      throw new TypeError("unsafeIntegers must be error or bigint");
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

    return { args, maxOutputBytes, processTimeoutMs, unsafeIntegers };
  }

  query(sql, options = {}) {
    const { args, maxOutputBytes, processTimeoutMs, unsafeIntegers } = this._command(sql, options);
    const result = this.runner(this.binary, args, {
      // Keep stdout as bytes so malformed UTF-8 is never silently replaced.
      encoding: null, shell: false, timeout: processTimeoutMs, maxBuffer: maxOutputBytes,
    });
    if (result.error) throw new RustSparkError(`Failed to run RustSpark: ${result.error.message}`);
    if (result.status !== 0) {
      const stderr = Buffer.isBuffer(result.stderr) ? result.stderr.toString("utf8") : (result.stderr ?? "");
      throw new RustSparkError(stderr.trim() || `RustSpark exited with ${result.status}`);
    }
    let rows;
    try {
      const stdout = Buffer.isBuffer(result.stdout)
        ? new TextDecoder("utf-8", { fatal: true }).decode(result.stdout)
        : result.stdout;
      rows = parseStrictJSON(stdout, { unsafeIntegers });
    }
    catch { throw new RustSparkError("RustSpark returned invalid JSON"); }
    if (!Array.isArray(rows) || rows.some((row) => row === null || typeof row !== "object" || Array.isArray(row))) {
      throw new RustSparkError("RustSpark returned an unexpected JSON result");
    }
    return rows;
  }

  // Stream validated row objects with backpressure and bounded line storage.
  // This API launches a real child process; the synchronous custom runner only
  // affects query() and queryOne().
  async *queryRows(sql, options = {}) {
    const { args, maxOutputBytes, processTimeoutMs, unsafeIntegers } = this._command(sql, options);
    const flag = args.indexOf("--stdout-format");
    args[flag + 1] = "json";

    const child = spawn(this.binary, args, {
      shell: false, detached: process.platform !== "win32",
      stdio: ["ignore", "pipe", "pipe"],
    });
    let launchError = null;
    let timedOut = false;
    let stderr = Buffer.alloc(0);
    child.stderr.on("data", (chunk) => {
      if (stderr.length < 65536) {
        stderr = Buffer.concat([stderr, chunk.subarray(0, 65536 - stderr.length)]);
      }
    });
    const finished = new Promise((resolve) => {
      child.on("error", (error) => {
        launchError = error;
        resolve({ code: null, signal: null });
      });
      child.on("close", (code, signal) => resolve({ code, signal }));
    });
    const stopChild = () => {
      if (child.pid && process.platform !== "win32") {
        try {
          process.kill(-child.pid, "SIGKILL");
          return;
        } catch (error) {
          if (error?.code !== "ESRCH") child.kill("SIGKILL");
        }
      } else {
        child.kill("SIGKILL");
      }
    };
    const deadline = processTimeoutMs === undefined ? null : setTimeout(() => {
      timedOut = true;
      stopChild();
    }, processTimeoutMs);

    const maxLineBytes = 16 * 1024 * 1024;
    let outputBytes = 0;
    let segments = [];
    let segmentBytes = 0;
    const utf8 = new TextDecoder("utf-8", { fatal: true });
    const parseRow = (directLine = null) => {
      // A complete row inside one stdout chunk needs no Buffer.concat.
      // TextDecoder still rejects malformed UTF-8 for either path.
      const source = utf8.decode(directLine ?? Buffer.concat(segments, segmentBytes));
      const row = parseStrictJSON(source, { unsafeIntegers });
      if (row === null || typeof row !== "object" || Array.isArray(row)) {
        throw new RustSparkError("RustSpark NDJSON row must be an object");
      }
      segments = [];
      segmentBytes = 0;
      return row;
    };
    try {
      for await (const chunk of child.stdout) {
        outputBytes += chunk.length;
        if (outputBytes > maxOutputBytes) {
          throw new RustSparkError("RustSpark NDJSON stdout exceeded maxOutputBytes");
        }
        let from = 0;
        while (from < chunk.length) {
          const newline = chunk.indexOf(10, from);
          const until = newline < 0 ? chunk.length : newline;
          const part = chunk.subarray(from, until);
          if (segmentBytes + part.length > maxLineBytes) {
            throw new RustSparkError("RustSpark NDJSON row exceeded 16 MiB");
          }
          if (newline >= 0 && segmentBytes === 0) {
            // Most CLI rows fit in one pipe chunk. Parse the borrowed slice
            // directly instead of allocating a new buffer for every row.
            yield parseRow(part);
          } else {
            segments.push(part);
            segmentBytes += part.length;
            if (newline >= 0) yield parseRow();
          }
          from = newline >= 0 ? newline + 1 : chunk.length;
        }
      }
      if (segmentBytes > 0) yield parseRow();
      const { code } = await finished;
      if (timedOut) throw new RustSparkError("RustSpark NDJSON process timeout");
      if (launchError) throw new RustSparkError(`Unable to launch RustSpark: ${launchError.message}`);
      if (code !== 0) {
        throw new RustSparkError(stderr.toString("utf8").trim() || `RustSpark exited with ${code}`);
      }
    } catch (error) {
      if (timedOut) throw new RustSparkError("RustSpark NDJSON process timeout");
      if (launchError) throw new RustSparkError(`Unable to launch RustSpark: ${launchError.message}`);
      if (error instanceof RustSparkError) throw error;
      throw new RustSparkError(`Invalid RustSpark NDJSON: ${error.message}`);
    } finally {
      if (deadline !== null) clearTimeout(deadline);
      if (child.exitCode === null && child.signalCode === null) stopChild();
      await finished;
    }
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
