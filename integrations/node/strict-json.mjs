// Reject conflicting object keys before JSON.parse discards the earlier value.
// Keep the scanner small: JSON.parse still owns full JSON syntax validation.
export function parseStrictJSON(source, { unsafeIntegers = "error" } = {}) {
  if (unsafeIntegers !== "error" && unsafeIntegers !== "bigint") {
    throw new TypeError("unsafeIntegers must be 'error' or 'bigint'");
  }
  if (typeof source !== "string") throw new TypeError("SQL JSON output must be text");
  let position = 0;
  const maxDepth = 256;

  const whitespace = () => {
    while (position < source.length && /[\x20\t\n\r]/.test(source[position])) position++;
  };
  const stringToken = (decode = true) => {
    const start = position++;
    while (position < source.length) {
      const char = source[position++];
      if (char === '"') {
        return decode ? JSON.parse(source.slice(start, position)) : undefined;
      }
      if (char === "\\") position++;
    }
    throw new SyntaxError("unterminated SQL JSON string");
  };
  const value = (depth) => {
    if (depth > maxDepth) throw new RangeError("SQL JSON nesting limit exceeded");
    whitespace();
    const first = source[position];
    if (first === '"') {
      // JSON.parse validates the full document later. Avoid materializing
      // large value strings during the duplicate-key pre-scan.
      stringToken(false);
    } else if (first === "{") {
      position++;
      const names = new Set();
      whitespace();
      if (source[position] === "}") { position++; return; }
      while (position < source.length) {
        whitespace();
        if (source[position] !== '"') throw new SyntaxError("SQL JSON object key must be text");
        const name = stringToken();
        if (names.has(name)) throw new SyntaxError("Duplicate SQL JSON result key: " + name);
        names.add(name);
        whitespace();
        if (source[position++] !== ":") throw new SyntaxError("Missing SQL JSON key separator");
        value(depth + 1);
        whitespace();
        const separator = source[position++];
        if (separator === "}") return;
        if (separator !== ",") throw new SyntaxError("Invalid SQL JSON object separator");
      }
      throw new SyntaxError("Unterminated SQL JSON object");
    } else if (first === "[") {
      position++;
      whitespace();
      if (source[position] === "]") { position++; return; }
      while (position < source.length) {
        value(depth + 1);
        whitespace();
        const separator = source[position++];
        if (separator === "]") return;
        if (separator !== ",") throw new SyntaxError("Invalid SQL JSON array separator");
      }
      throw new SyntaxError("Unterminated SQL JSON array");
    } else {
      const start = position;
      while (position < source.length && !/[\x20\t\n\r,\]}]/.test(source[position])) position++;
      if (start === position) throw new SyntaxError("Invalid SQL JSON scalar");
    }
  };

  value(0);
  whitespace();
  if (position !== source.length) throw new SyntaxError("Unexpected trailing SQL JSON");
  return JSON.parse(source, (_key, item, context) => {
    if (typeof item === "number" && !Number.isFinite(item)) {
      throw new RangeError("Non-finite SQL JSON result number");
    }
    if (typeof item === "number" && Number.isInteger(item) && !Number.isSafeInteger(item)) {
      if (unsafeIntegers === "bigint") {
        // JSON.parse has already rounded the number. Use its original token.
        // Older Node versions have no source context: fail instead of corrupting it.
        if (typeof context?.source !== "string") {
          throw new RangeError("Lossless integers require Node.js with JSON.parse source context");
        }
        if (/^-?(?:0|[1-9][0-9]*)$/.test(context.source)) {
          return BigInt(context.source);
        }
      }
      throw new RangeError("Unsafe SQL JSON integer");
    }
    return item;
  });
}
