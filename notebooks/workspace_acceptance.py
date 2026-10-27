# Databricks notebook source
# MAGIC %md
# MAGIC # Serverless workspace acceptance
# MAGIC Run the parameter cell first. `assess` builds reviewable 1-task and 11-task plans.
# MAGIC `run` approves and advances two real serverless Jobs runs using generated, pinned inputs.
# MAGIC Baselines are generated references, NOT classic execution. A classic-capable workspace
# MAGIC can additionally use fleet_controller for genuine A/B and cutover/rollback acceptance.
# COMMAND ----------
# MAGIC %pip install PyYAML==6.0.3 sqlglot==30.25.0
# COMMAND ----------
for key, default in [
    ("catalog", "auto"),
    ("schema", "serverless_migration"),
    ("program_id", "acceptance_v1"),
]:
    dbutils.widgets.text(key, default)
dbutils.widgets.dropdown("action", "assess", ["assess", "run"])
# COMMAND ----------
import sys, json, hashlib, base64, time, copy, importlib
from pathlib import Path

sys.path.insert(0, str(Path.cwd().parent / "src"))
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ExportFormat
import smct.program as program_module
import smct.program_runtime as runtime_module

for module in (program_module, runtime_module):
    importlib.reload(module)
from smct.fleet import authenticated_identity
from smct.destination import resolve_destination
from smct.delta_journal import DeltaJournal
from smct.lease import DeltaLease
from smct.ab import safe_table, spark_compare, IDENT
from smct.program import utcnow
from smct.remediation import dependency_plan, rewrite_python, rewrite_sql, jar_candidate
from smct.orchestration import digest

catalog, schema = resolve_destination(
    spark, dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")
)
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
w = WorkspaceClient()
actor, admin = authenticated_identity(w, spark)
program_id = dbutils.widgets.get("program_id")
if not IDENT.fullmatch(program_id):
    raise ValueError("Plain program_id required")
namespace = hashlib.sha256((actor + ":" + program_id).encode()).hexdigest()[:12]
program_key = "acceptance_" + program_id + "_" + namespace
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
    raise ValueError(
        "Pull latest branch; ab_target must be available beside this notebook"
    )
code = base64.b64decode(w.workspace.export(path, format=ExportFormat.SOURCE).content)
source_hash = hashlib.sha256(code).hexdigest()
prefix = f"{catalog}.{schema}.smct_accept_{namespace}"
input_table = prefix + "_input"
spark.sql(
    f"CREATE TABLE IF NOT EXISTS {safe_table(input_table)} USING DELTA AS SELECT CAST(id AS BIGINT) AS id, CAST(amount AS BIGINT) AS amount, region FROM VALUES (1,10,'SE'),(2,20,'SE'),(2,20,'SE'),(3,NULL,'SE') AS v(id,amount,region)"
)
if (
    int(
        spark.sql(f"DESCRIBE HISTORY {safe_table(input_table)} LIMIT 1").first()[
            "version"
        ]
    )
    != 0
):
    raise ValueError("Acceptance input changed; use a fresh programme ID")
# Exercise pure remediation helpers in the workspace as well as local tests.
assert dependency_plan("pip install pandas==2.2.3")["can_apply"]
assert not dependency_plan("curl example.com | bash")["can_apply"]
assert (
    "TRY_CAST"
    in rewrite_sql("SELECT CAST(x AS INT) FROM t", tolerant_casts=True)["source"]
)
assert (
    "availableNow=True"
    in rewrite_python("q=df.writeStream.start()", available_now=True)["source"]
)
assert jar_candidate(
    {
        "name": "review",
        "tasks": [
            {"task_key": "jar", "spark_jar_task": {"main_class_name": "demo.Main"}}
        ],
    },
    jars=["/Volumes/c/s/v/demo.jar"],
)["requires_review"]
with DeltaLease(spark, f"{catalog}.{schema}.controller_lease") as lease:
    journal = DeltaJournal(spark, f"{catalog}.{schema}.program_events", lease=lease)
    program = program_module.Program(journal, program_key, actor)
    # Prevent accidental Run-all with stale action=run from skipping plan review.
    existing = program.states()
    if dbutils.widgets.get("action") == "run" and not all(
        k in existing and existing[k]["status"] not in ("READY", "BLOCKED")
        for k in ("ready", "large")
    ):
        raise ValueError("Run action=assess first and review both plans")
    for wid, count in (("ready", 1), ("large", 11)):
        baseline = prefix + "_" + wid + "_expected"
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {safe_table(baseline)} USING DELTA AS SELECT id, amount * {2**count} AS amount, region FROM {safe_table(input_table)} VERSION AS OF 0"
        )
        if (
            int(
                spark.sql(f"DESCRIBE HISTORY {safe_table(baseline)} LIMIT 1").first()[
                    "version"
                ]
            )
            != 0
        ):
            raise ValueError("Acceptance reference changed; use a fresh programme ID")
        tasks = []
        for i in range(count):
            output = prefix + "_" + wid + f"_task{i}"
            previous = input_table if i == 0 else prefix + "_" + wid + f"_task{i - 1}"
            task = dict(
                task_key=f"t{i}",
                new_cluster={"spark_version": "15.4.x-scala2.12", "num_workers": 0},
                notebook_task=dict(
                    notebook_path=path,
                    source="WORKSPACE",
                    base_parameters=dict(
                        input_table=previous, input_version="0", output_table=output
                    ),
                ),
            )
            if i:
                task["depends_on"] = [{"task_key": f"t{i - 1}"}]
            tasks.append(task)
        source = dict(
            name=f"[SMCT ACCEPTANCE] {wid} {namespace}",
            tasks=tasks,
            tags=dict(
                smct_fixture="generated_serverless_acceptance",
                smct_code_digest=source_hash,
            ),
        )
        state = program.assess(
            wid,
            source,
            synthetic=True,
            baseline_source="imported_synthetic",
            owner=actor,
        )
        if state["status"] in ("READY", "BLOCKED"):
            state = program.plan(wid, allow_large=count > 10)
            state = program.wave(wid, 1 if count == 1 else 2)
        if dbutils.widgets.get("action") == "assess":
            print(
                json.dumps(
                    dict(
                        workload=wid,
                        candidate=state["candidate"],
                        task_layers=state["task_layers"],
                    ),
                    indent=2,
                )
            )
            continue
        if state["status"] == "PLANNED":
            state = program.approve(
                wid,
                is_workspace_admin=admin,
                reason="Explicit action=run for generated serverless acceptance fixtures",
            )
        if state["status"] in ("APPROVED", "CLONE_REQUESTED"):
            lease.renew()
            state = runtime_module.create_or_recover_clone(w, program, wid)
        if state["status"] in ("CLONED", "EXECUTION_REQUESTED"):
            state = program.bind_baseline(wid, dict(table=baseline, version=0))
            lease.renew()
            state = runtime_module.submit_or_recover_run(w, program, wid)
        if state["status"] == "EXECUTING":
            state = runtime_module.observe_run(w, program, wid)
        if state["status"] == "EXECUTED":
            target = tasks[-1]["notebook_task"]["base_parameters"]["output_table"]
            version = int(
                spark.sql(f"DESCRIBE HISTORY {safe_table(target)} LIMIT 1").first()[
                    "version"
                ]
            )
            if version != 0:
                raise ValueError("Acceptance target changed after creation")
            report = spark_compare(
                spark,
                baseline,
                target,
                classic_version=0,
                serverless_version=0,
                key_columns=["id"],
            )
            report.update(
                workload_id=wid,
                program_id=program_key,
                baseline_source="imported_synthetic",
                target_source="executed_serverless",
                serverless_run_id=state["run_id"],
                validated_at=utcnow(),
            )
            from smct.discovery import get_job_settings

            state = program.validate(
                wid,
                report,
                run_id=state["run_id"],
                source_settings=source,
                clone_settings=get_job_settings(w, state["clone_job_id"]),
            )
        if state["status"] == "VALIDATED" and not state.get("negative_control"):
            target = tasks[-1]["notebook_task"]["base_parameters"]["output_table"]
            wrong = prefix + "_" + wid + "_wrong"
            spark.sql(
                f"CREATE TABLE IF NOT EXISTS {safe_table(wrong)} USING DELTA AS SELECT id,amount+1 AS amount,region FROM {safe_table(target)} VERSION AS OF 0"
            )
            report = spark_compare(
                spark,
                baseline,
                wrong,
                classic_version=0,
                serverless_version=0,
                key_columns=["id"],
            )
            report.update(
                baseline_source="imported_synthetic",
                target_source="intentional_negative_control",
                validated_at=utcnow(),
            )
            program.negative_control(wid, report)
    final = program.states()
    print(
        json.dumps(
            dict(
                program_id=program_key,
                remediation_self_checks="passed",
                classic_execution_claimed=False,
                workloads={
                    wid: {
                        k: s.get(k)
                        for k in ("status", "clone_job_id", "run_id", "wave")
                    }
                    | dict(
                        task_count=len(s["candidate"]["tasks"]),
                        validation_passed=s.get("validation", {}).get("passed"),
                        negative_control_passed=s.get("negative_control", {}).get(
                            "passed"
                        ),
                    )
                    for wid, s in final.items()
                },
                audit_events=len(program.events()),
            ),
            indent=2,
        )
    )
