"""Deterministic output validation for reproducible record collections.
Does not execute jobs or mutate tables. For real jobs use isolated snapshots.
"""
import hashlib
import json
from collections import Counter

def fingerprint(rows: list[dict], ignore_columns=()) -> dict:
    normalized = [json.dumps({k:v for k,v in r.items() if k not in ignore_columns}, sort_keys=True, default=str, separators=(',',':')) for r in rows]
    hashes = Counter(hashlib.sha256(row.encode()).hexdigest() for row in normalized)
    schema = sorted({k for row in rows for k in row if k not in ignore_columns})
    return {"row_count": len(rows), "columns": schema, "record_hashes": hashes}

def compare_rows(classic: list[dict], serverless: list[dict], ignore_columns=()) -> dict:
    before = fingerprint(classic, ignore_columns)
    after = fingerprint(serverless, ignore_columns)
    missing = sum((before["record_hashes"]-after["record_hashes"]).values())
    extra = sum((after["record_hashes"]-before["record_hashes"]).values())
    passed = (before["columns"] == after["columns"] and not missing and not extra)
    return {"passed": passed, "classic_rows":before["row_count"], "serverless_rows":after["row_count"], "columns_equal":before["columns"] == after["columns"], "missing_records":missing, "extra_records":extra}
