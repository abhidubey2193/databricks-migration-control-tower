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
from smct.evidence import load_evidence
saved = load_evidence(lambda sql: [r.asDict() for r in spark.sql(sql).collect()], catalog, schema)
payload = saved['payload']
reports = saved['reports']
safety_reports = saved['safety_reports']
program_states = saved['program_states']
program_events = saved['program_events']
html = render_dashboard(
    payload,
    reports,
    program_states=program_states,
    program_events=program_events,
    safety_reports=safety_reports,
    scanned_at=saved["scanned_at"],
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
