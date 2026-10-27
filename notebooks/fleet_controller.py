# Databricks notebook source
# MAGIC %md
# MAGIC # Fleet controller
# MAGIC Use Serverless notebook compute. Run the parameter cell first, then set an action.
# MAGIC `scan` is read-only except for evidence persistence. Review `plan` output before `approve`.
# MAGIC `execute` advances a wave without waiting; rerun to observe/continue pending Jobs runs.
# MAGIC A/B runs use independent unscheduled classic/serverless clones, never the source job.
# COMMAND ----------
# MAGIC %pip install PyYAML==6.0.3 sqlglot==30.25.0
# COMMAND ----------
for key, default in [
    ("catalog", "auto"),
    ("schema", "serverless_migration"),
    ("program_id", "fleet_review_v1"),
    ("manifest_json", '{"workloads": []}'),
    ("wave", "1"),
    ("reason", ""),
    ("workload_id", ""),
    ("repo_root", ""),
    ("start_date", ""),
    ("end_date", ""),
]:
    dbutils.widgets.text(key, default)
dbutils.widgets.dropdown(
    "action",
    "scan",
    [
        "scan",
        "assess",
        "plan",
        "approve",
        "execute",
        "cost",
        "reconcile",
        "cutover",
        "rollback",
        "activate",
        "keep_classic",
        "defer",
        "status",
    ],
)
# COMMAND ----------
import sys, json, importlib
from pathlib import Path
from datetime import datetime, timezone, timedelta

sys.path.insert(0, str(Path.cwd().parent / "src"))
from databricks.sdk import WorkspaceClient
import smct.fleet as fleet_module
import smct.program as program_module
import smct.program_runtime as runtime_module

for module in (fleet_module, program_module, runtime_module):
    importlib.reload(module)
from smct.destination import resolve_destination
from smct.delta_journal import DeltaJournal
from smct.lease import DeltaLease
from smct.promotion import PromotionController
from smct.discovery import inventory
from smct.static_scan import scan_notebook
from smct.ab import safe_table, IDENT
from smct.cost import billing_sql, compare_costs

catalog, schema = resolve_destination(
    spark, dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")
)
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
w = WorkspaceClient()
actor, is_admin = fleet_module.authenticated_identity(w, spark)
program_id = dbutils.widgets.get("program_id")
if not IDENT.fullmatch(program_id):
    raise ValueError("Plain program_id required")
manifest = json.loads(dbutils.widgets.get("manifest_json"))
items = manifest.get("workloads", [])
if len({x["id"] for x in items}) != len(items):
    raise ValueError("Duplicate manifest workload IDs")
action = dbutils.widgets.get("action")
with DeltaLease(spark, f"{catalog}.{schema}.controller_lease") as lease:
    # Guard every mutating REST request, even after long reads/comparisons.
    class GuardedAPI:
        def do(self, method, *args, **kwargs):
            if method.upper() not in ("GET", "HEAD"):
                lease.renew()
            return raw.api_client.do(method, *args, **kwargs)

    class GuardedWorkspace:
        api_client = GuardedAPI()

        def __getattr__(self, key):
            return getattr(raw, key)

    raw = w
    w = GuardedWorkspace()
    journal = DeltaJournal(spark, f"{catalog}.{schema}.program_events", lease=lease)
    program = program_module.Program(journal, program_id, actor)
    runner = fleet_module.FleetRunner(w, spark, program, catalog, schema)
    if action == "scan":
        workloads, errors = inventory(w, root="/", max_notebooks=10000)
        for item in workloads:
            if item.kind == "NOTEBOOK":
                scan = scan_notebook(w, item.to_dict())
                item.attributes["code_scanned"] = scan["code_scanned"]
                item.attributes["code_findings"] = scan["findings"]
                item.readiness, item.eligible = scan["readiness"], scan["eligible"]
                from smct.models import Finding

                item.findings = [Finding(**f) for f in scan["findings"]]
                if not scan["code_scanned"]:
                    errors.append(f"Code scan unavailable: {item.workload_id}")
        # Propagate code blockers from notebooks into their referencing jobs.
        by_path = {x.source_path: x for x in workloads if x.kind == "NOTEBOOK"}
        from smct.assessment import score

        for item in workloads:
            if item.kind == "JOB":
                for path in item.attributes.get("notebook_paths", []):
                    if path in by_path:
                        item.findings.extend(by_path[path].findings)
                    else:
                        from smct.models import Finding

                        item.findings.append(
                            Finding(
                                "L1",
                                "blocker",
                                "Referenced notebook not scanned",
                                "Restore export permissions or include the path in scan scope",
                            )
                        )
                item.readiness, item.eligible = score(item.findings)
        payload = dict(
            metadata=dict(
                complete=not errors, errors=errors, read_only=True, synthetic=False
            ),
            workloads=[x.to_dict() for x in workloads],
        )
        lease.renew()
        spark.createDataFrame(
            [(datetime.now(timezone.utc).isoformat(), json.dumps(payload))],
            "scanned_at STRING,payload_json STRING",
        ).write.format("delta").mode("append").saveAsTable(
            safe_table(f"{catalog}.{schema}.fleet_snapshots")
        )
        print(
            json.dumps(
                dict(objects=len(workloads), complete=not errors, warnings=errors)
            )
        )
    elif action in ("assess", "plan"):
        if not items:
            raise ValueError(
                "Supply manifest_json workloads; see examples/fleet-manifest.json"
            )
        for item in items:
            state = runner.assess(item)
            if action == "plan":
                state = runner.plan(item["id"])
            print(json.dumps(state, indent=2, default=str))
    elif action in ("approve", "execute"):
        result = runner.wave(
            int(dbutils.widgets.get("wave")),
            action,
            reason=dbutils.widgets.get("reason"),
        )
        print(json.dumps(result, indent=2, default=str))
    elif action == "cost":
        now = datetime.now(timezone.utc).date()
        start = dbutils.widgets.get("start_date") or str(now - timedelta(days=30))
        end = dbutils.widgets.get("end_date") or str(now + timedelta(days=1))
        try:
            rows = [
                r.asDict()
                for r in spark.sql(
                    billing_sql(w.get_workspace_id(), start_date=start, end_date=end)
                ).collect()
            ]
            for wid, state in program.states().items():
                if (
                    state.get("classic_run_id")
                    and state.get("run_id")
                    and state["status"]
                    in (
                        "EXECUTED",
                        "VALIDATED",
                        "VALIDATION_FAILED",
                        "MIGRATED_PAUSED",
                        "MIGRATED",
                        "ROLLED_BACK_PAUSED",
                        "ROLLED_BACK",
                    )
                ):
                    observation = compare_costs(
                        rows,
                        classic_job_id=state["classic_job_id"],
                        classic_run_id=state["classic_run_id"],
                        serverless_job_id=state["clone_job_id"],
                        serverless_run_id=state["run_id"],
                        outputs_equal=bool(state.get("validation", {}).get("passed")),
                    )
                    observation.update(
                        start_date=start,
                        end_date=end,
                        observed_at=datetime.now(timezone.utc).isoformat(),
                    )
                    program.billing_observed(wid, observation)
            print(
                json.dumps(
                    {"billing_rows": len(rows), "states": program.states()}, default=str
                )
            )
        except Exception as exc:
            print(
                json.dumps(
                    {
                        "billing_status": "UNAVAILABLE",
                        "error_type": type(exc).__name__,
                        "detail": str(exc),
                        "savings_claimed": False,
                    }
                )
            )
            raise
    elif action in ("keep_classic", "defer"):
        state = program.disposition(
            dbutils.widgets.get("workload_id"),
            decision="KEEP_CLASSIC" if action == "keep_classic" else "DEFERRED",
            reason=dbutils.widgets.get("reason"),
            is_workspace_admin=is_admin,
        )
        print(json.dumps(state, indent=2, default=str))
    elif action in ("reconcile", "cutover", "rollback", "activate"):
        wid, reason = dbutils.widgets.get("workload_id"), dbutils.widgets.get("reason")
        controller = PromotionController(w, program)
        if action == "reconcile":
            root = dbutils.widgets.get("repo_root")
            if not root:
                raise ValueError(
                    "Explicit repository root required for Bundle writeback"
                )
            state = controller.reconcile(wid, root, reason=reason)
        else:
            state = getattr(controller, action)(wid, reason=reason)
        print(json.dumps(state, indent=2, default=str))
    else:
        print(json.dumps(program.states(), indent=2, default=str))
