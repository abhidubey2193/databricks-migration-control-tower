"""Manifest-driven, resumable fleet execution on approved sandbox workloads.

The source job is never run or edited by validation. Two unscheduled validation
jobs execute the same notebook with pinned inputs and separate output tables.
One controller writer per programme; the Bundle job enforces max_concurrent_runs=1.
"""

import base64
import copy
import hashlib
import re

from .ab import safe_table, spark_compare
from .assessment import check_code
from .discovery import get_job_settings, serial
from .orchestration import COMPUTE_FIELDS, TRIGGERS, build_serverless_clone, digest
from .program import utcnow
from .program_runtime import (
    create_or_recover_clone,
    submit_or_recover_run,
    observe_run,
)
from .remediation import (
    dependency_plan,
    read_init_script,
    translate_job,
    assign_task_environments,
)


def authenticated_identity(w, spark=None):
    principal = w.current_user.me()
    if not principal.user_name:
        raise PermissionError("Authenticated user identity required")
    if (
        spark is not None
        and spark.sql("SELECT current_user()").first()[0].lower()
        != principal.user_name.lower()
    ):
        raise PermissionError("SDK and SQL execution identities differ")
    return principal.user_name, any(
        getattr(g, "display", "") == "admins" for g in principal.groups or []
    )


def resolved_source(w, settings):
    resolved = copy.deepcopy(settings)
    for task in resolved.get("tasks", []):
        if task.get("existing_cluster_id"):
            cluster = serial(w.clusters.get(task.pop("existing_cluster_id")))
            # Read-only runtime attributes are not part of a NewCluster definition.
            allowed = {
                "spark_version",
                "node_type_id",
                "driver_node_type_id",
                "num_workers",
                "autoscale",
                "aws_attributes",
                "azure_attributes",
                "gcp_attributes",
                "docker_image",
                "init_scripts",
                "spark_conf",
                "spark_env_vars",
                "cluster_log_conf",
                "ssh_public_keys",
                "data_security_mode",
            }
            task["new_cluster"] = {k: v for k, v in cluster.items() if k in allowed}
    return resolved


def notebook_digests(w, settings):
    from databricks.sdk.service.workspace import ExportFormat

    fingerprints = {}
    for task in settings.get("tasks", []):
        notebook = task.get("notebook_task")
        if not notebook:
            raise ValueError(
                "Fleet A/B currently requires notebook tasks with an explicit isolation contract; classify other task types for manual validation"
            )
        path = notebook["notebook_path"]
        obj = w.workspace.get_status(path)
        language = getattr(
            getattr(obj, "language", None),
            "value",
            str(getattr(obj, "language", "PYTHON")),
        )
        exported = w.workspace.export(path, format=ExportFormat.SOURCE)
        raw = base64.b64decode(exported.content or "")
        if not raw or len(raw) > 2_000_000:
            raise ValueError("Notebook source missing or exceeds review cap")
        code_findings = check_code(raw.decode("utf-8"), language)
        if any(f.severity in ("blocker", "high") for f in code_findings):
            raise ValueError(
                "Notebook code requires remediation before A/B execution: "
                + "; ".join(f.detail for f in code_findings)
            )
        fingerprints[path] = hashlib.sha256(raw).hexdigest()
    return fingerprints


def run_outcome(run, job_id):
    data = serial(run)
    if int(data.get("job_id", -1)) != int(job_id):
        raise ValueError("Run belongs to another job")
    legacy, status = data.get("state") or {}, data.get("status") or {}
    if legacy.get("result_state"):
        return legacy["result_state"]
    if status.get("state") == "TERMINATED":
        return (status.get("termination_details") or {}).get("code", "FAILED")
    if legacy.get("life_cycle_state") in ("SKIPPED", "INTERNAL_ERROR"):
        return "FAILED"
    return None


def check_classic(actual, expected):
    if any(actual.get(k) for k in TRIGGERS):
        raise ValueError("Classic validation clone has a trigger")
    if digest(actual.get("tasks")) != digest(expected.get("tasks")):
        # Jobs readback can supply defaults for the notebook source field.
        def tasks(settings):
            value = copy.deepcopy(settings.get("tasks", []))
            for task in value:
                for field in (
                    "max_retries",
                    "timeout_seconds",
                    "min_retry_interval_millis",
                ):
                    if task.get(field) == 0:
                        task.pop(field)
                task.get("notebook_task", {}).setdefault("source", "WORKSPACE")
            return value

        if tasks(actual) != tasks(expected):
            raise ValueError("Classic validation task drift")
    for field in ("job_clusters", "git_source", "parameters"):
        if actual.get(field) != expected.get(field):
            raise ValueError("Classic validation configuration drift: " + field)
    for key, value in expected.get("tags", {}).items():
        if actual.get("tags", {}).get(key) != value:
            raise ValueError("Classic validation identity drift")


def create_or_recover_job(w, expected, check):
    tags = expected["tags"]
    matches = [
        j.job_id
        for j in w.jobs.list()
        if all(
            serial(j.settings).get("tags", {}).get(k) == tags[k]
            for k in ("smct_program", "smct_workload", "smct_role")
        )
    ]
    if len(matches) > 1:
        raise RuntimeError("Multiple matching validation jobs; reconcile before retry")
    if matches:
        job_id = int(matches[0])
    else:
        response = w.api_client.do(
            "POST", "/api/2.2/jobs/create", body=copy.deepcopy(expected)
        )
        if not response.get("job_id"):
            raise RuntimeError("Ambiguous job creation; recover by tags before retry")
        job_id = int(response["job_id"])
    actual = get_job_settings(w, job_id)
    check(actual, expected)
    return job_id, actual


class FleetRunner:
    def __init__(self, w, spark, program, catalog, schema):
        self.w, self.spark, self.program = w, spark, program
        self.catalog, self.schema = catalog, schema
        actor, self.is_admin = authenticated_identity(w, spark)
        if actor != program.actor:
            raise PermissionError(
                "Journal actor does not match authenticated SDK principal"
            )
        safe_table(f"{catalog}.{schema}.program_events")

    def assess(self, item):
        wid = item["id"]
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,39}", wid):
            raise ValueError(
                "Workload id must be a plain identifier, at most 40 characters"
            )
        source_id = int(item["source_job_id"])
        source = get_job_settings(self.w, source_id)
        if not any(t.get(k) for t in source.get("tasks", []) for k in COMPUTE_FIELDS):
            raise ValueError("Source must contain classic compute")
        state = self.program.assess(
            wid,
            source,
            synthetic=False,
            baseline_source="executed_classic",
            owner=self.program.actor,
        )
        if state.get("fleet_manifest"):
            if state["fleet_manifest"] != item:
                raise ValueError("Manifest drift; create a new workload/programme ID")
            return state
        state.update(source_job_id=source_id, fleet_manifest=copy.deepcopy(item))
        return self.program._write(wid, "FLEET_SOURCE_IMPORTED", state)

    def plan(self, wid):
        state = self.program.state(wid)
        if state["status"] not in ("READY", "BLOCKED", "PLANNED"):
            raise ValueError("Plan only before approval")
        manifest = state["fleet_manifest"]
        if manifest.get("sandbox_execution_reviewed") is not True:
            raise ValueError(
                "Review notebook side effects and set sandbox_execution_reviewed=true"
            )
        original = get_job_settings(self.w, state["source_job_id"])
        if digest(original) != state["source_digest"]:
            raise ValueError("Source job drift")
        resolved = resolved_source(self.w, original)
        scripts = []
        clusters = [t.get("new_cluster", {}) for t in resolved["tasks"]]
        clusters += [j.get("new_cluster", {}) for j in resolved.get("job_clusters", [])]
        for cluster in clusters:
            for script in cluster.get("init_scripts", []):
                uris = [
                    x.get("destination") for x in script.values() if isinstance(x, dict)
                ]
                if len(uris) != 1:
                    raise ValueError("Unknown init-script source")
                text = read_init_script(self.w, uris[0])
                translated = dependency_plan(text)
                scripts.append(dict(translated, uri=uris[0]))
        translated, dependencies = translate_job(
            resolved,
            script_plans=scripts,
            library_replacements=manifest.get("library_replacements"),
            task_scoped=True,
        )
        code = notebook_digests(self.w, translated)
        source_inputs = manifest["inputs"]
        safe_table(source_inputs["table"])
        if type(source_inputs["version"]) is not int or source_inputs["version"] < 0:
            raise ValueError("Pinned nonnegative input version required")
        self.spark.sql(
            f"SELECT * FROM {safe_table(source_inputs['table'])} VERSION AS OF {source_inputs['version']} LIMIT 0"
        )
        # Compile before changing table parameters: production proposal preserves original output contract.
        production = build_serverless_clone(translated, allow_large=True)
        production["name"] = original["name"]
        production["tags"] = copy.deepcopy(original.get("tags", {}))
        production = assign_task_environments(production, dependencies)
        state = self.program.plan(
            wid,
            allow_large=True,
            resolved_settings=translated,
            task_dependencies=dependencies,
        )
        namespace = hashlib.sha256(
            (self.program.program_id + ":" + wid).encode()
        ).hexdigest()[:16]
        outputs = {}
        for role in ("classic", "serverless"):
            outputs[role] = {
                t[
                    "task_key"
                ]: f"{self.catalog}.{self.schema}.smct_{namespace}_{role}_{i}"
                for i, t in enumerate(original["tasks"])
            }
        classic = copy.deepcopy(original)
        for key in TRIGGERS:
            classic.pop(key, None)
        # Do not inherit run-as, deployment ownership, job parameters or notifications.
        classic = {
            k: v
            for k, v in classic.items()
            if k
            in (
                "name",
                "description",
                "tasks",
                "job_clusters",
                "tags",
                "timeout_seconds",
            )
        }
        classic.update(
            name="[SMCT CLASSIC VALIDATION] " + original["name"], max_concurrent_runs=1
        )
        classic["queue"] = {"enabled": True}
        classic.setdefault("tags", {}).update(
            smct_program=self.program.program_id,
            smct_workload=wid,
            smct_role="classic_validation",
        )
        candidate = state["candidate"]
        candidate["tags"]["smct_role"] = "serverless_validation"
        contract = manifest.get("parameter_names", {})
        names = {
            k: contract.get(k, k)
            for k in ("input_table", "input_version", "output_table")
        }
        if len(set(names.values())) != 3 or any(
            not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", n) for n in names.values()
        ):
            raise ValueError("Distinct plain parameter names required")
        for role, definition in (("classic", classic), ("serverless", candidate)):
            for task in definition["tasks"]:
                params = task["notebook_task"].setdefault("base_parameters", {})
                task_input = manifest.get("task_inputs", {}).get(task["task_key"])
                if task_input:
                    dependencies_for_task = [
                        d["task_key"] for d in task.get("depends_on", [])
                    ]
                    if task_input not in dependencies_for_task:
                        raise ValueError(
                            "Task input must reference a direct dependency"
                        )
                    input_table, input_version = outputs[role][task_input], "0"
                else:
                    input_table, input_version = (
                        source_inputs["table"],
                        str(source_inputs["version"]),
                    )
                params.update(
                    {
                        names["input_table"]: input_table,
                        names["input_version"]: input_version,
                        names["output_table"]: outputs[role][task["task_key"]],
                    }
                )
        final = manifest["output_task"]
        if final not in outputs["classic"]:
            raise ValueError("output_task does not exist")
        if manifest.get("wave", 1) < 1:
            raise ValueError("Positive wave required")
        state.update(
            candidate=candidate,
            candidate_digest=digest(candidate),
            classic_candidate=classic,
            classic_candidate_digest=digest(classic),
            production_candidate=production,
            code_digests=code,
            script_translations=scripts,
            outputs=outputs,
            wave=int(manifest.get("wave", 1)),
            input_snapshot=copy.deepcopy(source_inputs),
        )
        return self.program._write(wid, "ISOLATED_AB_PLAN_CREATED", state)

    def _check_drift(self, state):
        if (
            digest(get_job_settings(self.w, state["source_job_id"]))
            != state["source_digest"]
        ):
            raise ValueError("Source configuration drift")
        if notebook_digests(self.w, state["source_settings"]) != state["code_digests"]:
            raise ValueError("Notebook code drift")
        if digest(state["classic_candidate"]) != state["classic_candidate_digest"]:
            raise ValueError("Classic plan drift")

    def approve(self, wid, reason):
        state = self.program.state(wid)
        self._check_drift(state)
        # Bind all execution-relevant contracts to the authenticated approval.
        state["fleet_plan_digest"] = digest(
            {
                k: state[k]
                for k in (
                    "classic_candidate",
                    "candidate",
                    "production_candidate",
                    "code_digests",
                    "input_snapshot",
                    "fleet_manifest",
                )
            }
        )
        self.program._write(wid, "AB_PLAN_REVIEWED", state)
        return self.program.approve(
            wid, is_workspace_admin=self.is_admin, reason=reason
        )

    def step(self, wid):
        state = self.program.state(wid)
        if not state.get("approval"):
            raise PermissionError("Authenticated approval required")
        for upstream in state["fleet_manifest"].get("depends_on_workloads", []):
            if self.program.state(upstream)["status"] not in (
                "VALIDATED",
                "MIGRATED_PAUSED",
                "MIGRATED",
            ):
                raise ValueError("Upstream workload must validate first: " + upstream)
        self._check_drift(state)
        if state.get("fleet_plan_digest") != digest(
            {
                k: state[k]
                for k in (
                    "classic_candidate",
                    "candidate",
                    "production_candidate",
                    "code_digests",
                    "input_snapshot",
                    "fleet_manifest",
                )
            }
        ):
            raise ValueError("Approved fleet contract drift")
        # Classic clone runs first, never the original source job.
        if not state.get("classic_run_id"):
            if not state.get("classic_requested"):
                for name in state["outputs"]["classic"].values():
                    if self.spark.catalog.tableExists(safe_table(name)):
                        raise ValueError(
                            "Classic output already exists before first request"
                        )
                state["classic_requested"] = True
                state["backup"] = dict(
                    settings=copy.deepcopy(state["source_settings"]),
                    digest=state["source_digest"],
                    created_at=utcnow(),
                )
                state["classic_token"] = hashlib.sha256(
                    (self.program.program_id + ":" + wid + ":classic").encode()
                ).hexdigest()
                state = self.program._write(
                    wid, "CLASSIC_BACKUP_AND_EXECUTION_REQUESTED", state
                )
            job_id, actual = create_or_recover_job(
                self.w, state["classic_candidate"], check_classic
            )
            response = self.w.api_client.do(
                "POST",
                "/api/2.2/jobs/run-now",
                body=dict(job_id=job_id, idempotency_token=state["classic_token"]),
            )
            if not response.get("run_id"):
                raise RuntimeError("No classic run ID; idempotent retry required")
            state.update(
                classic_job_id=job_id,
                classic_run_id=int(response["run_id"]),
                classic_clone_digest=digest(actual),
            )
            return self.program._write(wid, "CLASSIC_RUN_SUBMITTED", state)
        if not state.get("classic_success"):
            if (
                digest(get_job_settings(self.w, state["classic_job_id"]))
                != state["classic_clone_digest"]
            ):
                raise ValueError("Classic validation clone drift")
            outcome = run_outcome(
                self.w.jobs.get_run(state["classic_run_id"]), state["classic_job_id"]
            )
            if not outcome:
                return state
            if outcome != "SUCCESS":
                state["status"] = "EXECUTION_FAILED"
                return self.program._write(
                    wid, "CLASSIC_RUN_FAILED", state, {"result": outcome}
                )
            final = state["fleet_manifest"]["output_task"]
            table = state["outputs"]["classic"][final]
            version = int(
                self.spark.sql(f"DESCRIBE HISTORY {safe_table(table)} LIMIT 1").first()[
                    "version"
                ]
            )
            if version != 0:
                raise ValueError(
                    "Classic output must have exactly one immutable creation commit"
                )
            state.update(
                classic_success=True,
                classic_snapshot=dict(
                    table=table,
                    version=version,
                    job_id=state["classic_job_id"],
                    run_id=state["classic_run_id"],
                ),
            )
            state = self.program._write(wid, "CLASSIC_BASELINE_EXECUTED", state)
        if state["status"] in ("APPROVED", "CLONE_REQUESTED"):
            state = create_or_recover_clone(self.w, self.program, wid)
        if state["status"] in ("CLONED", "EXECUTION_REQUESTED"):
            if state["status"] == "CLONED":
                for name in state["outputs"]["serverless"].values():
                    if self.spark.catalog.tableExists(safe_table(name)):
                        raise ValueError(
                            "Target output already exists before run request"
                        )
            self.program.bind_baseline(wid, state["classic_snapshot"])
            return submit_or_recover_run(self.w, self.program, wid)
        if state["status"] == "EXECUTING":
            state = observe_run(self.w, self.program, wid)
        if state["status"] == "EXECUTED":
            # Reverify the baseline run after the target finishes.
            if (
                run_outcome(
                    self.w.jobs.get_run(state["classic_run_id"]),
                    state["classic_job_id"],
                )
                != "SUCCESS"
            ):
                raise ValueError("Classic baseline run is not successful")
            table = state["outputs"]["serverless"][
                state["fleet_manifest"]["output_task"]
            ]
            version = int(
                self.spark.sql(f"DESCRIBE HISTORY {safe_table(table)} LIMIT 1").first()[
                    "version"
                ]
            )
            if version != 0:
                raise ValueError("Target output was modified after creation")
            report = spark_compare(
                self.spark,
                state["classic_snapshot"]["table"],
                table,
                classic_version=state["classic_snapshot"]["version"],
                serverless_version=version,
                key_columns=state["fleet_manifest"].get("key_columns", []),
                ignore_columns=state["fleet_manifest"].get("ignore_columns", []),
            )
            report.update(
                program_id=self.program.program_id,
                workload_id=wid,
                baseline_source="executed_classic",
                target_source="executed_serverless",
                classic_run_id=state["classic_run_id"],
                classic_job_id=state["classic_job_id"],
                serverless_run_id=state["run_id"],
                serverless_job_id=state["clone_job_id"],
                validated_at=utcnow(),
            )
            state = self.program.validate(
                wid,
                report,
                run_id=state["run_id"],
                source_settings=get_job_settings(self.w, state["source_job_id"]),
                clone_settings=get_job_settings(self.w, state["clone_job_id"]),
            )
        return state

    def wave(self, wave, action, *, reason="", max_inflight=2):
        if type(max_inflight) is not int or not 1 <= max_inflight <= 100:
            raise ValueError("max_inflight must be 1..100")
        selected = [
            wid
            for wid, s in self.program.states().items()
            if s.get("wave") == wave and s.get("fleet_manifest")
        ]
        results = {}
        for wid in selected:
            try:
                states = self.program.states()
                inflight = sum(
                    bool(s.get("classic_requested"))
                    and s["status"]
                    in (
                        "APPROVED",
                        "CLONE_REQUESTED",
                        "CLONED",
                        "EXECUTION_REQUESTED",
                        "EXECUTING",
                        "EXECUTED",
                    )
                    for s in states.values()
                )
                if (
                    action == "execute"
                    and not states[wid].get("classic_requested")
                    and inflight >= max_inflight
                ):
                    results[wid] = dict(
                        state=states[wid]["status"],
                        waiting="Inflight validation limit reached",
                    )
                    continue
                results[wid] = (
                    self.approve(wid, reason)
                    if action == "approve"
                    else (
                        self.step(wid)
                        if action == "execute"
                        else self.program.state(wid)
                    )
                )
            except Exception as exc:
                # Preserve other workloads' progress; a rejected action cannot create a PASS.
                results[wid] = dict(
                    error=type(exc).__name__,
                    detail=str(exc),
                    state=self.program.state(wid)["status"],
                )
        return results
