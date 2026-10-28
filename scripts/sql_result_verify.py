#!/usr/bin/env python3
"""Verify per-query SQL results against a trusted reference report."""

import argparse
import json
from pathlib import Path
import re


def reject_duplicate_keys(pairs):
    values = {}
    for key, value in pairs:
        if key in values:
            raise ValueError(f"Duplicate JSON key: {key}")
        values[key] = value
    return values


def reject_constant(value):
    raise ValueError(f"Non-finite JSON value: {value}")


def load_report(path: Path) -> dict:
    report = json.loads(
        path.read_text(encoding="utf-8"),
        object_pairs_hook=reject_duplicate_keys,
        parse_constant=reject_constant,
    )
    if not isinstance(report, dict):
        raise ValueError(f"{path}: expected a JSON report object")
    return report


def index_queries(report: dict, label: str) -> dict[int, dict]:
    results = report.get("results")
    if not isinstance(results, list) or not results:
        raise ValueError(f"{label}: expected a non-empty results array")
    attempted = report.get("queries_attempted")
    if type(attempted) is not int or attempted != len(results):
        raise ValueError(f"{label}: queries_attempted does not match results")
    total = report.get("queries_total")
    if total is not None and (type(total) is not int or total < attempted):
        raise ValueError(f"{label}: invalid queries_total")
    indexed = {}
    for row in results:
        if not isinstance(row, dict) or type(row.get("id")) is not int or row["id"] < 1:
            raise ValueError(f"{label}: invalid query result or ID")
        if row["id"] in indexed:
            raise ValueError(f"{label}: duplicate query ID {row['id']}")
        if total is not None and row["id"] > total:
            raise ValueError(f"{label}: query ID exceeds queries_total")
        indexed[row["id"]] = row
    return indexed


def valid_digest(value) -> bool:
    return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) is not None


def _append_row_differences(number, observed, prior, issues):
    for label, result in [("Actual", observed), ("Reference", prior)]:
        count = result.get("result_rows")
        if type(count) is not int or count < 0:
            issues.append(f"Query {number}: invalid {label.lower()} row count")
            return
    if observed["result_rows"] != prior["result_rows"]:
        issues.append(f"Query {number}: row count differs")
    if observed["result_sha256"] != prior["result_sha256"]:
        issues.append(f"Query {number}: result hash differs")


def verify_reports(actual: dict, reference: dict) -> list[str]:
    digest = actual.get("query_file_sha256")
    if not valid_digest(digest) or digest != reference.get("query_file_sha256"):
        raise ValueError("SQL workload hash differs between reports")
    rows = index_queries(actual, "Actual")
    expected = index_queries(reference, "Reference")
    if (actual.get("queries_total") is not None
            and reference.get("queries_total") is not None
            and actual["queries_total"] != reference["queries_total"]):
        raise ValueError("SQL report query totals differ")
    issues = []
    for number in sorted(expected.keys() - rows.keys()):
        issues.append(f"Query {number}: missing actual result")
    for number, observed in sorted(rows.items()):
        prior = expected.get(number)
        if prior is None:
            issues.append(f"Query {number}: missing reference result")
            continue
        if not isinstance(observed.get("sql"), str) or observed["sql"] != prior.get("sql"):
            issues.append(f"Query {number}: SQL differs from reference")
            continue
        if prior.get("status") != "executed" or not valid_digest(prior.get("result_sha256")):
            issues.append(f"Query {number}: invalid reference result")
            continue
        if observed.get("status") != "executed" or not valid_digest(observed.get("result_sha256")):
            issues.append(f"Query {number}: query did not execute successfully")
            continue
        _append_row_differences(number, observed, prior, issues)
    return issues


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--actual", type=Path, required=True)
    parser.add_argument("--reference", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        actual = load_report(args.actual)
        reference = load_report(args.reference)
        issues = verify_reports(actual, reference)
    except (OSError, UnicodeError, ValueError) as error:
        parser.error(str(error))
    if issues:
        for issue in issues:
            print(issue)
        return 1
    print(f"Verified {len(actual['results'])} query results")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
