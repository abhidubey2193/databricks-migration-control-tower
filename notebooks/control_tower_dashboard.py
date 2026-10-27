# Databricks notebook source
# MAGIC %md
# MAGIC # Serverless Migration Control Tower Dashboard
# MAGIC Reads the persisted fleet scan and validation reports. No workload mutations.
# MAGIC Set catalog to auto, then Run all. Use object_type and minimum_score to filter inventory.

# COMMAND ----------
import sys
from pathlib import Path

sys.path.insert(0, str(Path.cwd().parent / "src"))
import json
from smct.destination import resolve_destination
from smct.ab import safe_table

# Git updates do not invalidate Python's session module cache.
import importlib
import smct.notebook_dashboard as dashboard_module

importlib.invalidate_caches()
dashboard_module = importlib.reload(dashboard_module)
render_dashboard = dashboard_module.render_dashboard

for key, default in [("catalog", "auto"), ("schema", "serverless_migration")]:
    dbutils.widgets.text(key, default)
dbutils.widgets.dropdown("object_type", "All", ["All", "JOB", "NOTEBOOK", "CLUSTER"])
dbutils.widgets.dropdown("minimum_score", "0", ["0", "25", "50", "75", "100"])
catalog, schema = resolve_destination(
    spark, dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")
)
inventory_table = safe_table(f"{catalog}.{schema}.fleet_snapshots")
validation_table = safe_table(f"{catalog}.{schema}.validation_demo_evidence")
rows = spark.sql(
    f"SELECT scanned_at,payload_json FROM {inventory_table} ORDER BY scanned_at DESC LIMIT 1"
).collect()
if not rows:
    raise ValueError("No saved inventory. Run fleet_scan first.")
payload = json.loads(rows[0]["payload_json"])
# Do not suppress permissions, corrupt evidence or query errors as empty results.
validation_rows = spark.sql(
    f"SELECT report_json FROM {validation_table} ORDER BY validated_at DESC, scenario LIMIT 20"
).collect()
reports = [json.loads(row["report_json"]) for row in validation_rows]
safety_table = safe_table(f"{catalog}.{schema}.clone_safety_demo_evidence")
safety_reports = []
if spark.catalog.tableExists(safety_table):
    safety_rows = spark.sql(
        f"SELECT report_json FROM {safety_table} ORDER BY checked_at DESC LIMIT 1"
    ).collect()
    safety_reports = [json.loads(row["report_json"]) for row in safety_rows]
program_states = []
program_events = []
program_table = safe_table(f"{catalog}.{schema}.program_events")
if spark.catalog.tableExists(program_table):
    raw = spark.sql(
        f"SELECT * FROM {program_table} ORDER BY event_at DESC LIMIT 5001"
    ).collect()
    if len(raw) > 5000:
        raise ValueError(
            "Dashboard programme history exceeds cap; filter/archive before displaying"
        )
    latest = {}
    seen = set()
    for row in sorted(raw, key=lambda r: (r["event_at"], r["sequence"])):
        event = {
            "program_id": row["program_id"],
            "workload_id": row["workload_id"],
            "sequence": row["sequence"],
            "event_type": row["event_type"],
            "actor": row["actor"],
            "event_at": row["event_at"],
            "state": json.loads(row["state_json"]),
        }
        key = (event["program_id"], event["workload_id"])
        revision = (*key, event["sequence"])
        if revision in seen:
            raise ValueError(
                "Conflicting programme audit revisions; reconcile before use"
            )
        seen.add(revision)
        program_events.append(event)
        if key not in latest or event["sequence"] > latest[key]["sequence"]:
            latest[key] = event
    program_states = [
        dict(e["state"], program_id=e["program_id"]) for e in latest.values()
    ]
    for state in program_states:
        for field in ("validation", "negative_control"):
            if state.get(field):
                reports.append(dict(state[field], workload=state["name"]))
html = render_dashboard(
    payload,
    reports,
    program_states=program_states,
    program_events=program_events,
    safety_reports=safety_reports,
    scanned_at=rows[0]["scanned_at"],
    object_type=dbutils.widgets.get("object_type"),
    minimum_score=int(dbutils.widgets.get("minimum_score")),
)
displayHTML(html)
print(
    json.dumps(
        {
            "dashboard": "rendered",
            "inventory_objects": len(payload.get("workloads", [])),
            "inventory_complete": payload.get("metadata", {}).get("complete", False),
            "program_workloads": len(program_states),
            "program_validated": sum(
                s["status"] == "VALIDATED" for s in program_states
            ),
            "program_audit_events": len(program_events),
            "safety_reports": len(safety_reports),
            "validation_reports": len(reports),
            "pass_reports": sum(r.get("passed") is True for r in reports),
            "fail_reports": sum(r.get("passed") is False for r in reports),
        }
    )
)
