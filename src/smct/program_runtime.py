"""Jobs API adapter for explicitly approved synthetic demonstrations."""

import copy

from .discovery import serial, get_job_settings
from .orchestration import digest


def check_candidate(actual, planned):
    if any(actual.get(k) for k in ("schedule", "trigger", "continuous", "triggers")):
        raise ValueError("Clone has triggers")
    if (
        actual.get("parameters")
        or actual.get("git_source")
        or actual.get("environment_variables")
    ):
        raise ValueError(
            "Unexpected job parameters, Git source or environment variables"
        )
    if actual.get("name") != planned.get("name"):
        raise ValueError("Clone name drift")
    for key, value in planned.get("tags", {}).items():
        if actual.get("tags", {}).get(key) != value:
            raise ValueError("Clone tags drift")
    tasks = actual.get("tasks") or []
    from .orchestration import validate_task_graph

    validate_task_graph(tasks)
    if actual.get("max_concurrent_runs", 1) != planned.get("max_concurrent_runs", 1):
        raise ValueError("Clone concurrency drift")
    expected = planned.get("tasks") or []
    if len(tasks) != len(expected):
        raise ValueError("Clone task count drift")
    by_key = {t["task_key"]: t for t in tasks}
    for task in expected:
        live = by_key.get(task["task_key"], {})
        if live.get("libraries"):
            raise ValueError("Unexpected task libraries")
        if any(
            live.get(k)
            for k in ("new_cluster", "existing_cluster_id", "job_cluster_key")
        ):
            raise ValueError("Clone is not serverless")
        for key in (
            "notebook_task",
            "spark_python_task",
            "python_wheel_task",
            "depends_on",
            "environment_key",
            "run_if",
            "timeout_seconds",
            "max_retries",
        ):
            if key in ("run_if", "timeout_seconds", "max_retries") and key not in task:
                continue
            if task.get(key) != live.get(key):
                raise ValueError(f"Clone {key} drift")
    if actual.get("environments") != planned.get("environments"):
        # SDK serialization may omit an empty dependency list.
        left = copy.deepcopy(actual.get("environments", []))
        right = copy.deepcopy(planned.get("environments", []))
        for env in left + right:
            spec = env.get("spec", {})
            if spec.get("dependencies") == []:
                spec.pop("dependencies")
        if left != right:
            raise ValueError("Clone environment drift")


def create_or_recover_clone(w, program, wid):
    state = program.prepare_clone(wid)
    planned = copy.deepcopy(state["candidate"])
    if (
        planned.get("tags", {}).get("smct_program") != program.program_id
        or planned.get("tags", {}).get("smct_workload") != wid
    ):
        raise ValueError("Approved plan is not bound to this programme/workload")
    # Exact tagged discovery permits recovery if creation succeeded before an interruption.
    matches = []
    for job in w.jobs.list():
        tags = serial(job.settings).get("tags", {})
        if (
            tags.get("smct_program") == program.program_id
            and tags.get("smct_workload") == wid
            and (
                not planned.get("tags", {}).get("smct_role")
                or tags.get("smct_role") == planned["tags"]["smct_role"]
            )
        ):
            matches.append(job.job_id)
    if len(matches) > 1:
        raise RuntimeError("Multiple tagged clones; reconcile manually before running")
    if matches:
        job_id = matches[0]
    else:
        result = w.api_client.do("POST", "/api/2.2/jobs/create", body=planned)
        if not result.get("job_id"):
            raise RuntimeError("Create returned no job ID; reconcile before retry")
        job_id = result["job_id"]
    actual = get_job_settings(w, int(job_id))
    check_candidate(actual, planned)
    return program.attach_clone(wid, int(job_id), actual)


def submit_or_recover_run(w, program, wid):
    state = program.execution_requested(wid)
    actual = get_job_settings(w, state["clone_job_id"])
    if digest(actual) != state["clone_digest"]:
        raise ValueError("Clone changed after approval/readback; refuse run")
    result = w.api_client.do(
        "POST",
        "/api/2.2/jobs/run-now",
        body={"job_id": state["clone_job_id"], "idempotency_token": state["run_token"]},
    )
    if not result.get("run_id"):
        raise RuntimeError("Run returned no ID; idempotent retry or reconcile required")
    return program.attach_run(wid, int(result["run_id"]))


def observe_run(w, program, wid):
    state = program.state(wid)
    if state["status"] != "EXECUTING":
        return state
    run = serial(w.jobs.get_run(state["run_id"]))
    if run.get("job_id") != state["clone_job_id"]:
        raise ValueError("Run belongs to a different job")
    # Legacy state is still returned; status handles newer Jobs API responses.
    legacy = run.get("state") or {}
    status = run.get("status") or {}
    result = legacy.get("result_state")
    if not result and status.get("state") == "TERMINATED":
        code = (status.get("termination_details") or {}).get("code")
        result = "SUCCESS" if code == "SUCCESS" else (code or "FAILED")
    if result:
        return program.execution_finished(wid, result, run["job_id"])
    if legacy.get("life_cycle_state") in ("SKIPPED", "INTERNAL_ERROR"):
        return program.execution_finished(wid, "FAILED", run["job_id"])
    return state
