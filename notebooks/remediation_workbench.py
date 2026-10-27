# Databricks notebook source
# MAGIC %md
# MAGIC # Remediation workbench
# MAGIC Produces reviewable proposals, not an in-place rewrite. Scripts are read, never executed.
# MAGIC Python/SQL source is supplied explicitly; use a sandbox and omit credentials.
# COMMAND ----------
# MAGIC %pip install PyYAML==6.0.3 sqlglot==30.25.0
# COMMAND ----------
for key, default in [("script_uri", ""), ("source", ""), ("options_json", "{}")]:
    dbutils.widgets.text(key, default)
dbutils.widgets.dropdown(
    "operation",
    "init_script",
    ["init_script", "python_proposal", "sql_proposal", "jar_proposal"],
)
# COMMAND ----------
import sys, json
from pathlib import Path

sys.path.insert(0, str(Path.cwd().parent / "src"))
from databricks.sdk import WorkspaceClient
from smct.remediation import (
    read_init_script,
    dependency_plan,
    rewrite_python,
    rewrite_sql,
    jar_candidate,
)

options = json.loads(dbutils.widgets.get("options_json"))
operation = dbutils.widgets.get("operation")
if operation == "init_script":
    plan = dependency_plan(
        read_init_script(WorkspaceClient(), dbutils.widgets.get("script_uri"))
    )
    print(json.dumps(plan, indent=2))
elif operation == "python_proposal":
    print(
        json.dumps(rewrite_python(dbutils.widgets.get("source"), **options), indent=2)
    )
elif operation == "sql_proposal":
    print(json.dumps(rewrite_sql(dbutils.widgets.get("source"), **options), indent=2))
else:
    print(
        json.dumps(
            jar_candidate(json.loads(dbutils.widgets.get("source")), **options),
            indent=2,
        )
    )
