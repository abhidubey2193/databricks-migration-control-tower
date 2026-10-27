# Databricks notebook source
# MAGIC %md
# MAGIC # Classic-capable workspace acceptance setup
# MAGIC Requires an authorized classic cluster. Free Edition and Express serverless
# MAGIC workspaces cannot supply one. This creates two PAUSED source fixtures and
# MAGIC generated inputs; it never runs a job. Use the resulting manifest in fleet_controller.
# COMMAND ----------
for key, default in [
    ("catalog", "auto"),
    ("schema", "serverless_migration"),
    ("classic_cluster_id", ""),
    ("fixture_id", "classic_acceptance_v1"),
]:
    dbutils.widgets.text(key, default)
dbutils.widgets.dropdown("apply", "false", ["false", "true"])
# COMMAND ----------
import sys, json, hashlib, copy
from pathlib import Path

sys.path.insert(0, str(Path.cwd().parent / "src"))
from databricks.sdk import WorkspaceClient
from smct.destination import resolve_destination
from smct.ab import safe_table, IDENT
from smct.fleet import authenticated_identity, create_or_recover_job, check_classic
from smct.discovery import serial
from smct.assessment import check_cluster

catalog, schema = resolve_destination(
    spark, dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")
)
w = WorkspaceClient()
actor, admin = authenticated_identity(w, spark)
if not admin:
    raise PermissionError("Workspace administrator required for this acceptance setup")
cluster_id = dbutils.widgets.get("classic_cluster_id")
if not cluster_id:
    raise ValueError(
        "Supply an authorized classic cluster ID; this setup is unavailable in Free Edition/serverless-only workspaces"
    )
cluster = serial(w.clusters.get(cluster_id))
if any(f.severity == "blocker" for f in check_cluster(cluster)):
    raise ValueError(
        "Choose compatible DBR 13+ classic compute without ML Runtime, images or instance-profile blockers"
    )
fixture = dbutils.widgets.get("fixture_id")
if not IDENT.fullmatch(fixture):
    raise ValueError("Plain fixture_id required")
namespace = hashlib.sha256((actor + ":" + fixture).encode()).hexdigest()[:12]
path = None
for candidate in (str(Path.cwd() / "ab_target"), str(Path.cwd() / "ab_target.py")):
    try:
        w.workspace.get_status(candidate)
        path = candidate
        break
    except Exception as exc:
        if type(exc).__name__ not in ("NotFound", "ResourceDoesNotExist"):
            raise
if path is None:
    raise ValueError("ab_target notebook is missing; pull latest branch")
input_table = f"{catalog}.{schema}.smct_classic_{namespace}_input"
plans = []
for wid, count in (("ready", 1), ("large", 11)):
    tasks = []
    for i in range(count):
        task = dict(
            task_key=f"t{i}",
            existing_cluster_id=cluster_id,
            notebook_task=dict(
                notebook_path=path,
                source="WORKSPACE",
                base_parameters=dict(
                    input_table=input_table,
                    input_version="0",
                    output_table=f"{catalog}.{schema}.smct_source_{namespace}_{wid}_{i}",
                ),
            ),
        )
        if i:
            task["depends_on"] = [dict(task_key=f"t{i - 1}")]
        tasks.append(task)
    source = dict(
        name=f"[SMCT CLASSIC FIXTURE] {wid} {namespace}",
        tasks=tasks,
        max_concurrent_runs=1,
        schedule=dict(
            quartz_cron_expression="0 0 1 * * ?",
            timezone_id="UTC",
            pause_status="PAUSED",
        ),
        tags=dict(
            smct_program=fixture,
            smct_workload=wid,
            smct_role="classic_source_fixture",
            smct_fixture="generated_orders",
            team="sandbox",
        ),
    )
    plans.append((wid, count, source))
if dbutils.widgets.get("apply") != "true":
    print(
        json.dumps(
            dict(dry_run=True, source_plans=[s for _, _, s in plans], jobs_started=0),
            indent=2,
        )
    )
else:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {safe_table(input_table)} USING DELTA AS SELECT CAST(id AS BIGINT) AS id,CAST(amount AS BIGINT) AS amount,region FROM VALUES (1,10,'SE'),(2,20,'SE'),(2,20,'SE'),(3,NULL,'SE') AS v(id,amount,region)"
    )
    if (
        int(
            spark.sql(f"DESCRIBE HISTORY {safe_table(input_table)} LIMIT 1").first()[
                "version"
            ]
        )
        != 0
    ):
        raise ValueError("Fixture input changed; choose a fresh fixture_id")
    manifest = []

    # Source fixtures are intentionally paused; clone readback uses a trigger-stripped checker.
    def check_source(actual, expected):
        if actual.get("schedule") != expected["schedule"]:
            raise ValueError("Existing source fixture schedule differs or is active")
        a, b = copy.deepcopy(actual), copy.deepcopy(expected)
        a.pop("schedule", None)
        b.pop("schedule", None)
        check_classic(a, b)

    for wid, count, source in plans:
        job_id, _ = create_or_recover_job(w, source, check_source)
        manifest.append(
            dict(
                id=wid,
                source_job_id=job_id,
                wave=1 if count == 1 else 2,
                sandbox_execution_reviewed=True,
                source_data_synthetic=True,
                inputs=dict(table=input_table, version=0),
                output_task=f"t{count - 1}",
                key_columns=["id"],
                task_inputs={f"t{i}": f"t{i - 1}" for i in range(1, count)},
            )
        )
    print(json.dumps(dict(workloads=manifest), indent=2))
    print(
        "No source jobs were executed. Review this manifest in fleet_controller: plan, approve, execute. Genuine classic execution is established only after its validation clones succeed."
    )
