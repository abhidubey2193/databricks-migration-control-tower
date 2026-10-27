"""Guarded SDK clone planning. All mutations require explicit --apply and stored approval.
Does NOT claim to automate the Beta Genie Code migration agent.
"""

import copy
import hashlib
import json
import os
from pathlib import Path
from .discovery import serial, get_job_settings

SUPPORTED = {"notebook_task", "spark_python_task", "python_wheel_task"}
COMPUTE_FIELDS = {"existing_cluster_id", "new_cluster", "job_cluster_key"}
TRIGGERS = ("schedule", "trigger", "continuous", "triggers")


def digest(settings):
    return hashlib.sha256(
        json.dumps(settings, sort_keys=True, default=str).encode()
    ).hexdigest()


def build_serverless_clone(settings, *, allow_large=False):
    """Returns candidate without side effects; unsupported or ambiguous constructs fail closed."""
    tasks = settings.get("tasks") or []
    limit = 1000 if allow_large else 10
    if not tasks or len(tasks) > limit:
        raise ValueError(f"Only jobs with 1-{limit} explicit tasks supported")
    validate_task_graph(tasks)
    if settings.get("git_source"):
        raise ValueError(
            "Git-backed jobs require repository changes before cloning (L9)"
        )
    if settings.get("parameters"):
        raise ValueError("Job-level parameters require manual review")
    from .assessment import check_job

    if any(
        f.severity in ("blocker", "high", "medium")
        and f.limitation not in (("L10", "L2") if allow_large else ("L10",))
        and not f.detail.endswith("uses existing cluster")
        for f in check_job(settings)
    ):
        raise ValueError("Source configuration requires remediation before cloning")
    if settings.get("environments") or settings.get("environment_variables"):
        raise ValueError("Existing environments require explicit reconciliation")
    cluster_keys = {c.get("job_cluster_key") for c in settings.get("job_clusters", [])}
    if any(
        t.get("job_cluster_key") and t["job_cluster_key"] not in cluster_keys
        for t in tasks
    ):
        raise ValueError("Referenced job cluster configuration unavailable")
    clean = copy.deepcopy(settings)
    clean["name"] = "[SMCT VALIDATION - NO SCHEDULE] " + str(
        settings.get("name", "unnamed")
    )
    for field in TRIGGERS:
        clean.pop(field, None)
    clean.pop("job_clusters", None)
    clean.pop("deployment", None)
    clean.pop("run_as", None)
    clean.pop("edit_mode", None)
    clean.pop("environments", None)
    clean.pop("health", None)
    clean.pop("queue", None)
    clean.pop("max_concurrent_runs", None)
    clean["max_concurrent_runs"] = 1
    for task in clean["tasks"]:
        kinds = [k for k in SUPPORTED if task.get(k)]
        if len(kinds) != 1:
            raise ValueError(f"Unsupported or ambiguous task {task.get('task_key')}")
        if task.get("libraries"):
            raise ValueError(
                "Task libraries require environment translation and manual review (L5)"
            )
        if task.get("depends_on") and any("outcome" in d for d in task["depends_on"]):
            raise ValueError("Conditional task dependencies need review")
        for field in COMPUTE_FIELDS:
            task.pop(field, None)
        task.pop("libraries", None)
        task.pop("compute_key", None)
        task_fields = {
            "task_key",
            "description",
            "depends_on",
            "run_if",
            "notebook_task",
            "spark_python_task",
            "python_wheel_task",
            "environment_key",
            "timeout_seconds",
            "max_retries",
            "min_retry_interval_millis",
            "retry_on_timeout",
        }
        unknown = set(task) - task_fields
        if unknown:
            raise ValueError(
                f"Unsupported task attributes require review: {sorted(unknown)}"
            )
        kind = kinds[0]
        if (task[kind].get("source") or "WORKSPACE") != "WORKSPACE":
            raise ValueError("Remote task sources require source reconciliation")
        if kind == "notebook_task" and not task[kind].get(
            "notebook_path", ""
        ).startswith("/"):
            raise ValueError("Notebook path must be absolute")
        if kind == "spark_python_task" and not task[kind].get(
            "python_file", ""
        ).startswith(("/Workspace/", "/Volumes/")):
            raise ValueError(
                "Python file must use an accessible Workspace or Volume path"
            )
        task["environment_key"] = "smct_env"
    clean["environments"] = [
        {
            "environment_key": "smct_env",
            "spec": {"environment_version": "2", "dependencies": []},
        }
    ]
    allowed = {
        "name",
        "description",
        "tasks",
        "environments",
        "tags",
        "email_notifications",
        "notification_settings",
        "timeout_seconds",
        "max_concurrent_runs",
        "format",
        "parameters",
    }
    result = {k: v for k, v in clean.items() if k in allowed}
    result.pop("email_notifications", None)
    result.pop("notification_settings", None)
    result.pop("format", None)
    result["queue"] = {"enabled": True}
    return result


def clone_job(w, store, job_id, backup_dir="backups", apply=False):
    wid = f"job:{job_id}"
    item = store.get(wid)
    if not store.approval(wid):
        raise PermissionError("Explicit recorded approval required")
    if store.migration(wid):
        raise ValueError("Migration already recorded")
    if not item["eligible"]:
        raise ValueError("Readiness contains blockers; refuse clone")
    data = get_job_settings(w, job_id)
    if item["compute_mode"] != "classic":
        raise ValueError("Source is not a classic job")
    if not any(t.get(k) for t in data.get("tasks", []) for k in COMPUTE_FIELDS):
        raise ValueError("Fresh source settings no longer use classic compute")
    # Reassess fresh settings; never rely on eligibility from a previous inventory.
    from .assessment import check_cluster

    for task in data.get("tasks") or []:
        if task.get("existing_cluster_id"):
            config = serial(w.clusters.get(task["existing_cluster_id"]))
            if any(
                f.severity in ("blocker", "high", "medium")
                for f in check_cluster(config)
            ):
                raise ValueError("Existing cluster configuration requires remediation")
    candidate = build_serverless_clone(data)
    source_digest = digest(data)
    if not apply:
        return {"dry_run": True, "candidate": candidate, "source_digest": source_digest}
    backup = Path(backup_dir).resolve() / f"job-{job_id}-{source_digest[:12]}.json"
    backup.parent.mkdir(parents=True, exist_ok=True)
    try:
        fd = os.open(backup, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        if digest(json.loads(backup.read_text())) != source_digest:
            raise ValueError("Existing backup integrity mismatch")
    else:
        with os.fdopen(fd, "w", encoding="utf8") as stream:
            json.dump(data, stream, indent=2, default=str)
    created = w.api_client.do("POST", "/api/2.1/jobs/create", body=dict(candidate))
    clone_id = created.get("job_id")
    if not clone_id:
        raise RuntimeError(
            "Create did not return clone job id. Check workspace and reconcile manually"
        )
    store.save_migration(wid, job_id, int(clone_id), str(backup), source_digest)
    return {
        "dry_run": False,
        "clone_job_id": clone_id,
        "backup": str(backup),
        "source_digest": source_digest,
    }


def cutover_report(w, store, job_id):
    from .governance import evaluate_cutover

    wid = f"job:{job_id}"
    m = store.migration(wid)
    if not m:
        raise ValueError("No migration clone recorded")
    before = get_job_settings(w, job_id)
    clone = get_job_settings(w, m["clone_job_id"])
    backup = Path(m["backup_path"])
    no_triggers = not any(clone.get(x) for x in TRIGGERS)
    no_drift = digest(before) == m["source_digest"]
    backup_valid = False
    if backup.is_file():
        try:
            backup_valid = digest(json.loads(backup.read_text())) == m["source_digest"]
        except (ValueError, OSError):
            pass
    validation = store.validation(wid)
    # This gate remains advisory even when all checks pass. Synthetic/unbound
    # output comparisons cannot satisfy execution evidence.
    execution_evidence = bool(
        validation
        and validation.get("baseline_source") == "executed_classic"
        and validation.get("classic_run_id")
        and validation.get("serverless_run_id")
        and validation.get("source_digest") == m["source_digest"]
        and validation.get("clone_digest") == digest(clone)
        and validation.get("typed_schema_compared")
        and validation.get("schema_equal")
    )
    if execution_evidence:
        try:
            for ref, expected_job in [
                ("classic_run_id", job_id),
                ("serverless_run_id", m["clone_job_id"]),
            ]:
                run = serial(w.jobs.get_run(int(validation[ref])))
                if (
                    run.get("job_id") != expected_job
                    or (run.get("state") or {}).get("result_state") != "SUCCESS"
                ):
                    execution_evidence = False
        except Exception:
            execution_evidence = False
    gate = evaluate_cutover(
        bool(store.approval(wid)),
        backup_valid,
        bool(validation and validation.get("passed") and execution_evidence),
        no_triggers,
        no_drift and not bool(before.get("git_source")),
    )
    return {
        "allowed": gate.allowed,
        "reasons": gate.reasons,
        "original_unchanged": no_drift,
        "clone_trigger_disabled": no_triggers,
        "execution_evidence_verified": execution_evidence,
        "backup_integrity": backup_valid,
        "validation": validation,
        "note": "Manual promotion required; the tool never activates two schedules.",
    }


def validate_task_graph(tasks):
    """Validate unique keys, dependency references and acyclicity without changing DAG semantics."""
    keys = [t.get("task_key") for t in tasks]
    if not all(isinstance(k, str) and k for k in keys) or len(set(keys)) != len(keys):
        raise ValueError("Task keys must be nonempty and unique")
    dependencies = {
        t["task_key"]: {d["task_key"] for d in t.get("depends_on", [])} for t in tasks
    }
    if any(not ds.issubset(set(keys)) for ds in dependencies.values()):
        raise ValueError("Task dependency references an unknown task")
    remaining = set(keys)
    layers = []
    while remaining:
        layer = sorted(k for k in remaining if not dependencies[k] & remaining)
        if not layer:
            raise ValueError("Task graph contains a dependency cycle")
        layers.append(layer)
        remaining.difference_update(layer)
    return layers
