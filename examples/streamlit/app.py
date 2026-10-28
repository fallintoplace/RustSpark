"""Small local RustSpark CSV analytics UI (trusted desktop use only)."""
from __future__ import annotations

import csv
import io
import json
from pathlib import Path
import subprocess
import tempfile

MAX_UPLOAD_BYTES = 32 * 1024 * 1024


def query_csv(binary: str, csv_bytes: bytes, sql: str, max_rows: int = 1000):
    if not csv_bytes or len(csv_bytes) > MAX_UPLOAD_BYTES:
        raise ValueError("Upload must be between 1 byte and 32 MiB")
    if not sql.strip():
        raise ValueError("SQL is required")
    if not 1 <= max_rows <= 1000:
        raise ValueError("Result row limit must be between 1 and 1000")
    with tempfile.TemporaryDirectory(prefix="rustspark-ui-") as directory:
        input_file = Path(directory) / "input.csv"
        input_file.write_bytes(csv_bytes)
        completed = subprocess.run(
            [
                binary, "--csv", str(input_file), "--sql", sql,
                "--stdout-format", "json-array", "--max-rows", str(max_rows),
                "--query-timeout", "20",
            ],
            capture_output=True, text=True, timeout=30, check=False,
        )
        if completed.returncode:
            raise RuntimeError(completed.stderr.strip() or "RustSpark query failed")
        rows = json.loads(completed.stdout)
        if not isinstance(rows, list):
            raise RuntimeError("Unexpected RustSpark output")
        return rows


def as_csv(rows):
    if not rows:
        return ""
    text = io.StringIO(newline="")
    columns = list(rows[0])
    writer = csv.DictWriter(text, fieldnames=columns, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(rows)
    return text.getvalue()


def main():
    import streamlit as st

    st.set_page_config(page_title="RustSpark CSV Explorer", layout="wide")
    st.title("RustSpark CSV Explorer")
    st.caption("Local proof of concept. Not a public multi-user SQL service.")
    binary = st.text_input("RustSpark binary", "target/release/rustspark")
    uploaded = st.file_uploader("CSV input (max 32 MiB)", type=["csv"])
    sql = st.text_area(
        "SQL over table `data`",
        "SELECT * FROM data LIMIT 10",
        height=120,
    )
    max_rows = st.slider("Maximum result rows", 1, 1000, 100)
    if st.button("Run SQL", type="primary"):
        if uploaded is None:
            st.warning("Select a CSV file first")
            return
        try:
            rows = query_csv(binary, uploaded.getvalue(), sql, max_rows)
        except (ValueError, RuntimeError, subprocess.TimeoutExpired, OSError, json.JSONDecodeError) as error:
            st.error(str(error))
            return
        st.success(f"{len(rows)} result rows")
        st.dataframe(rows, use_container_width=True)
        st.download_button("Download CSV result", data=as_csv(rows),
                           file_name="rustspark-result.csv", mime="text/csv")


if __name__ == "__main__":
    main()
