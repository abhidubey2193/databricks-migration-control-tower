# Databricks notebook source
# MAGIC %md
# MAGIC # Connected migration programme demonstration
# MAGIC One Delta-backed lifecycle: assess → wave → plan → approve → backup → clone → execute → validate.
# MAGIC Inputs and classic definitions are SYNTHETIC. The serverless target executes as a real Jobs run.
# MAGIC action=assess persists three assessment examples and the large-job direct API plan.
# MAGIC action=run additionally approves one synthetic ready workload under the authenticated principal,
# MAGIC creates its unscheduled job and executes four generated records into an isolated table.
# MAGIC Single writer per program_id. Rerun to resume a pending run; never run two copies concurrently.

# COMMAND ----------
import sys, json, hashlib, importlib, time, base64
from pathlib import Path

sys.path.insert(0, str(Path.cwd().parent / "src"))
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ExportFormat
from smct.destination import resolve_destination
from smct.ab import safe_table, IDENT
from smct.validation import compare_rows, fingerprint
from smct.orchestration import digest
import smct.program as program_module
import smct.program_runtime as runtime_module
import smct.delta_journal as journal_module

for module in (program_module, runtime_module, journal_module):
    importlib.reload(module)
from smct.demo import fixtures

for key, default in [
    ("catalog", "auto"),
    ("schema", "serverless_migration"),
    ("program_id", "challenge_demo_v1"),
]:
    dbutils.widgets.text(key, default)
dbutils.widgets.dropdown("action", "assess", ["assess", "run"])
catalog, schema = resolve_destination(
    spark, dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")
)
program_id = dbutils.widgets.get("program_id")
if not IDENT.fullmatch(program_id):
    raise ValueError("Use a plain program_id identifier")
w = WorkspaceClient()
actor = spark.sql("SELECT current_user()").first()[0]
principal = w.current_user.me()
if not principal.user_name or principal.user_name.lower() != actor.lower():
    raise PermissionError("SQL and SDK identities must agree")
is_admin = any(getattr(g, "display", "") == "admins" for g in principal.groups or [])
# Per-principal programme identity avoids accidental cross-user reuse of fixture jobs.
namespace = hashlib.sha256((actor + ":" + program_id).encode()).hexdigest()[:12]
program_key = program_id + "_" + namespace
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
journal = journal_module.DeltaJournal(spark, f"{catalog}.{schema}.program_events")
program = program_module.Program(journal, program_key, actor)
path = None
for candidate in (
    str(Path.cwd() / "program_target.py"),
    str(Path.cwd() / "program_target"),
):
    try:
        obj = w.workspace.get_status(candidate)
        if getattr(obj.object_type, "value", str(obj.object_type)) == "NOTEBOOK":
            path = candidate
            break
    except Exception as exc:
        if type(exc).__name__ not in ("NotFound", "ResourceDoesNotExist"):
            raise
if path is None:
    raise ValueError("Pull latest branch and open this notebook from notebooks/")
exported = w.workspace.export(path, format=ExportFormat.SOURCE)
if not exported.content:
    raise ValueError(
        "Target notebook source export is required for the plan fingerprint"
    )
code_hash = hashlib.sha256(base64.b64decode(exported.content)).hexdigest()
base_name = "smct_program_" + namespace
baseline_name = base_name + "_baseline"
target_name = base_name + "_target"
examples = fixtures()
for key, identifier, label in [
    (101, "ready", "Ready generated orders"),
    (102, "blocked", "Blocked container/init-script"),
    (103, "large", "11-task dependency chain"),
]:
    settings = examples[key]
    settings["name"] = "[SMCT PROGRAM] " + label + " " + namespace
    settings["tags"] = {
        "smct_definition": "SYNTHETIC_CLASSIC",
        "smct_code_digest": code_hash,
    }
    for task in settings["tasks"]:
        task["notebook_task"] = {
            "notebook_path": path,
            "source": "WORKSPACE",
            "base_parameters": {
                "catalog": catalog,
                "schema": schema,
                "output_table": (base_name + "_large_" + task["task_key"] + "_target")
                if identifier == "large"
                else target_name,
            },
        }
    state = program.assess(
        identifier,
        settings,
        synthetic=True,
        baseline_source="imported_synthetic",
        owner=actor,
    )
    if identifier == "ready" and state["status"] == "READY":
        program.wave(identifier, 1)
        program.plan(identifier)
    if identifier == "large" and state["status"] == "BLOCKED":
        program.plan(identifier, allow_large=True)
        program.wave(identifier, 2)
# The 11-task candidate is persisted for review, never created or run by this demo.
if dbutils.widgets.get("action") == "run":
    state = program.state("ready")
    if state["status"] == "PLANNED":
        state = program.approve(
            "ready",
            is_workspace_admin=is_admin,
            reason="Explicit action=run for isolated generated sandbox orders",
        )
    if state["status"] in ("APPROVED", "CLONE_REQUESTED"):
        state = runtime_module.create_or_recover_clone(w, program, "ready")
    expected = [
        {"id": 1, "amount": 20, "region": "SE"},
        {"id": 2, "amount": 40, "region": "SE"},
        {"id": 2, "amount": 40, "region": "SE"},
        {"id": 3, "amount": None, "region": "SE"},
    ]
    baseline_table = safe_table(f"{catalog}.{schema}.{baseline_name}")
    target_table = safe_table(f"{catalog}.{schema}.{target_name}")
    if state["status"] in ("CLONED", "EXECUTION_REQUESTED"):
        if not spark.catalog.tableExists(baseline_table):
            spark.createDataFrame(
                expected, "id LONG, amount LONG, region STRING"
            ).write.format("delta").mode("errorifexists").saveAsTable(baseline_table)
        version = state.get("baseline", {}).get("version")
        if version is None:
            version = int(
                spark.sql(f"DESCRIBE HISTORY {baseline_table} LIMIT 1").first()[
                    "version"
                ]
            )
        baseline = spark.sql(f"SELECT * FROM {baseline_table} VERSION AS OF {version}")
        if not compare_rows(
            expected, [r.asDict() for r in baseline.limit(101).collect()]
        )["passed"]:
            raise ValueError("Imported baseline drift; stop")
        state = program.bind_baseline(
            "ready",
            {
                "table": f"{catalog}.{schema}.{baseline_name}",
                "version": version,
                "fingerprint": digest(fingerprint(expected)),
            },
        )
        state = runtime_module.submit_or_recover_run(w, program, "ready")
    deadline = time.monotonic() + 55
    while state["status"] == "EXECUTING":
        state = runtime_module.observe_run(w, program, "ready")
        if state["status"] != "EXECUTING" or time.monotonic() >= deadline:
            break
        time.sleep(5)
    if state["status"] == "EXECUTED":
        baseline_version = state["baseline"]["version"]
        target_version = int(
            spark.sql(f"DESCRIBE HISTORY {target_table} LIMIT 1").first()["version"]
        )
        if target_version != 0:
            raise ValueError("Target table changed after its immutable job write")
        baseline = spark.sql(
            f"SELECT * FROM {baseline_table} VERSION AS OF {baseline_version}"
        )
        target = spark.sql(
            f"SELECT * FROM {target_table} VERSION AS OF {target_version}"
        )
        left = [r.asDict() for r in baseline.limit(101).collect()]
        right = [r.asDict() for r in target.limit(101).collect()]
        if len(left) > 100 or len(right) > 100:
            raise ValueError("Demonstration output cap exceeded")
        if digest(fingerprint(left)) != state["baseline"]["fingerprint"]:
            raise ValueError(
                "Pinned baseline contents differ from imported expectations"
            )
        report = compare_rows(
            left,
            right,
            classic_schema=baseline.schema.jsonValue(),
            serverless_schema=target.schema.jsonValue(),
            key_columns=["id"],
        )
        report.update(
            workload_id="ready",
            program_id=program_key,
            baseline_source="imported_synthetic",
            target_source="executed_serverless_job",
            serverless_run_id=state["run_id"],
            classic_run_id=None,
            baseline_table=f"{catalog}.{schema}.{baseline_name}",
            target_table=f"{catalog}.{schema}.{target_name}",
            baseline_table_version=baseline_version,
            target_table_version=target_version,
            validated_at=program_module.utcnow(),
        )
        state = program.validate(
            "ready",
            report,
            run_id=state["run_id"],
            source_settings=examples[101],
            clone_settings=w.jobs.get(state["clone_job_id"]).settings.as_dict(),
        )
    if state["status"] == "VALIDATED" and not state.get("negative_control"):
        baseline = spark.sql(
            f"SELECT * FROM {baseline_table} VERSION AS OF {state['baseline']['version']}"
        )
        target = spark.sql(
            f"SELECT * FROM {target_table} VERSION AS OF {state['validation']['target_table_version']}"
        )
        wrong = target.selectExpr("id", "amount + 1 AS amount", "region")
        negative = compare_rows(
            [r.asDict() for r in baseline.collect()],
            [r.asDict() for r in wrong.collect()],
            classic_schema=baseline.schema.jsonValue(),
            serverless_schema=wrong.schema.jsonValue(),
            key_columns=["id"],
        )
        negative.update(
            workload_id="ready",
            program_id=program_key,
            scenario="NEGATIVE_CONTROL",
            baseline_source="imported_synthetic",
            target_source="intentionally_altered_job_output",
            serverless_run_id=state["run_id"],
            validated_at=program_module.utcnow(),
        )
        state = program.negative_control("ready", negative)
    if state["status"] == "VALIDATED" and not state.get("billing"):
        try:
            billing = (
                spark.sql(
                    f"SELECT sku_name, usage_unit, SUM(usage_quantity) AS quantity FROM system.billing.usage WHERE usage_metadata.job_id = '{int(state['clone_job_id'])}' AND usage_start_time >= current_timestamp() - INTERVAL 1 DAY GROUP BY sku_name, usage_unit"
                )
                .limit(50)
                .collect()
            )
            observation = {
                "source": "system.billing.usage",
                "status": "AVAILABLE" if billing else "NO_USAGE_YET",
                "rows": [{k: str(v) for k, v in r.asDict().items()} for r in billing],
                "comparison_available": False,
                "note": "Single target job only; billing may lag. No savings claimed.",
            }
        except Exception as exc:
            observation = {
                "source": "system.billing.usage",
                "status": "UNAVAILABLE",
                "reason": type(exc).__name__,
                "comparison_available": False,
            }
        state = program.billing_observed("ready", observation)
    if state["status"] in ("EXECUTION_FAILED", "VALIDATION_FAILED"):
        print(
            "Execution/validation failed. Inspect the recorded job run; no promotion was attempted."
        )
    if state["status"] == "EXECUTING":
        print(
            "Job is still running. Rerun this notebook with the same program_id and action=run to resume; run-now uses an idempotency token."
        )
states = program.states()
events = program.events()
summary = {
    "program_id": program_key,
    "storage": f"{catalog}.{schema}.program_events",
    "actor": actor,
    "approval_identity": "workspace_admin"
    if is_admin
    else "authenticated_synthetic_fixture_owner",
    "workloads": {
        k: {
            "status": s["status"],
            "readiness": s["readiness"],
            "wave": s["wave"],
            "synthetic": s["synthetic"],
            "clone_job_id": s.get("clone_job_id"),
            "run_id": s.get("run_id"),
            "validation_passed": s.get("validation", {}).get("passed"),
            "negative_control_passed": s.get("negative_control", {}).get("passed"),
            "cutover_allowed": s["cutover_allowed"],
        }
        for k, s in states.items()
    },
    "audit_events": len(events),
    "large_plan_tasks": len(states["large"].get("candidate", {}).get("tasks", [])),
    "classic_execution_claimed": False,
}
print(json.dumps(summary, indent=2))
