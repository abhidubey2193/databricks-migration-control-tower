import copy
from types import SimpleNamespace

import pytest

from smct.demo import fixtures
from smct.orchestration import build_serverless_clone, validate_task_graph
from smct.program import MemoryJournal, Program
from smct.program_runtime import (
    create_or_recover_clone,
    observe_run,
    submit_or_recover_run,
)
from smct.validation import compare_rows


def assessed(settings=None):
    journal = MemoryJournal()
    p = Program(journal, "demo", "owner@example.invalid")
    p.assess(
        "ready",
        settings or fixtures()[101],
        synthetic=True,
        baseline_source="imported_synthetic",
        owner=p.actor,
    )
    return p


def approved():
    p = assessed()
    p.wave("ready", 1)
    p.plan("ready")
    p.approve("ready", reason="Reviewed sandbox plan")
    return p


def executed():
    p = approved()
    s = p.prepare_clone("ready")
    actual = copy.deepcopy(s["candidate"])
    p.attach_clone("ready", 10, actual)
    p.bind_baseline("ready", {"table": "c.s.baseline", "version": 0})
    p.execution_requested("ready")
    p.attach_run("ready", 99)
    p.execution_finished("ready", "SUCCESS", 10)
    return p, actual


def report(passed=True):
    r = compare_rows(
        [{"id": 1}],
        [{"id": 1 if passed else 2}],
        classic_schema=[["id", "LONG"]],
        serverless_schema=[["id", "LONG"]],
        key_columns=["id"],
    )
    r.update(
        workload_id="ready",
        program_id="demo",
        serverless_run_id=99,
        baseline_source="imported_synthetic",
        baseline_table="c.s.baseline",
        baseline_table_version=0,
    )
    return r


def test_connected_lifecycle_retains_all_evidence_and_never_promotes_synthetic():
    p, actual = executed()
    s = p.state("ready")
    p.validate(
        "ready",
        report(),
        run_id=99,
        source_settings=s["source_settings"],
        clone_settings=actual,
    )
    p.negative_control("ready", report(False))
    final = p.state("ready")
    assert final["status"] == "VALIDATED" and final["validation"]["passed"]
    assert final["negative_control"]["passed"] is False and not final["cutover_allowed"]
    assert (
        final["approval"]["actor"] == p.actor
        and final["backup"]["digest"] == final["source_digest"]
    )
    assert final["run_id"] == 99 and final["clone_job_id"] == 10 and final["wave"] == 1
    assert [e["event_type"] for e in p.events()] == [
        "ASSESSED",
        "WAVE_ASSIGNED",
        "PLAN_CREATED",
        "APPROVED",
        "BACKUP_SAVED_CLONE_REQUESTED",
        "CLONE_CREATED",
        "BASELINE_SNAPSHOT_BOUND",
        "EXECUTION_REQUESTED",
        "RUN_SUBMITTED",
        "RUN_FINISHED",
        "VALIDATION_PASSED",
        "NEGATIVE_CONTROL_REJECTED",
    ]


def test_clone_without_approval_denied():
    p = assessed()
    p.plan("ready")
    with pytest.raises(PermissionError):
        p.prepare_clone("ready")


def test_other_actor_cannot_approve_owner_fixture():
    p = assessed()
    p.plan("ready")
    p.actor = "stranger@example.invalid"
    with pytest.raises(PermissionError):
        p.approve("ready", reason="attempt")


def test_admin_scope_recorded_and_reason_required():
    p = assessed()
    p.plan("ready")
    p.actor = "admin@example.invalid"
    with pytest.raises(ValueError):
        p.approve("ready", is_workspace_admin=True)
    assert (
        p.approve("ready", is_workspace_admin=True, reason="Reviewed")["approval"][
            "scope"
        ]
        == "workspace_admin"
    )


def test_blocked_container_not_silently_stripped():
    p = assessed(fixtures()[102])
    assert p.state("ready")["status"] == "BLOCKED"
    with pytest.raises(ValueError):
        p.plan("ready", allow_large=True)


def test_large_job_plan_preserves_every_dependency():
    source = fixtures()[103]
    candidate = build_serverless_clone(source, allow_large=True)
    assert (
        len(candidate["tasks"]) == 11
        and len(validate_task_graph(candidate["tasks"])) == 11
    )
    for a, b in zip(source["tasks"], candidate["tasks"]):
        assert a.get("depends_on") == b.get("depends_on")
        assert "new_cluster" not in b
    with pytest.raises(ValueError):
        build_serverless_clone(source)


@pytest.mark.parametrize("kind", ["cycle", "missing", "duplicate"])
def test_invalid_task_graph_refused(kind):
    tasks = fixtures()[103]["tasks"]
    if kind == "cycle":
        tasks[0]["depends_on"] = [{"task_key": "t10"}]
    if kind == "missing":
        tasks[1]["depends_on"] = [{"task_key": "not_present"}]
    if kind == "duplicate":
        tasks[1]["task_key"] = "t0"
    with pytest.raises(ValueError):
        validate_task_graph(tasks)


def test_source_drift_cannot_reuse_program_workload():
    p = assessed()
    s = fixtures()[101]
    s["description"] = "changed"
    with pytest.raises(ValueError, match="Source drift"):
        p.assess(
            "ready",
            s,
            synthetic=True,
            baseline_source="imported_synthetic",
            owner=p.actor,
        )


def test_approved_plan_tampering_refused():
    p = approved()
    p.journal.rows[-1]["state"]["candidate"]["name"] = "tampered"
    with pytest.raises(ValueError, match="drifted"):
        p.prepare_clone("ready")


def test_triggered_clone_cannot_enter_execution_lifecycle():
    p = approved()
    s = p.prepare_clone("ready")
    actual = copy.deepcopy(s["candidate"])
    actual["continuous"] = {"pause_status": "PAUSED"}
    with pytest.raises(ValueError, match="triggers"):
        p.attach_clone("ready", 10, actual)


def test_validation_must_bind_successful_run_and_unchanged_configs():
    p, actual = executed()
    s = p.state("ready")
    with pytest.raises(ValueError):
        p.validate(
            "ready",
            report(),
            run_id=100,
            source_settings=s["source_settings"],
            clone_settings=actual,
        )
    altered = copy.deepcopy(actual)
    altered["description"] = "changed"
    with pytest.raises(ValueError, match="drift"):
        p.validate(
            "ready",
            report(),
            run_id=99,
            source_settings=s["source_settings"],
            clone_settings=altered,
        )


def test_inconsistent_pass_rejected():
    p, actual = executed()
    r = report()
    r["missing_records"] = 2
    with pytest.raises(ValueError, match="Inconsistent"):
        p.validate(
            "ready",
            r,
            run_id=99,
            source_settings=p.state("ready")["source_settings"],
            clone_settings=actual,
        )


def test_duplicate_journal_revision_blocks_further_actions():
    p = assessed()
    p.journal.rows.append(copy.deepcopy(p.journal.rows[0]))
    with pytest.raises(RuntimeError, match="Duplicate"):
        p.states()


def test_negative_control_pass_cannot_be_mislabelled_as_detected_failure():
    p, actual = executed()
    p.validate(
        "ready",
        report(),
        run_id=99,
        source_settings=p.state("ready")["source_settings"],
        clone_settings=actual,
    )
    with pytest.raises(ValueError):
        p.negative_control("ready", report())


class Workspace:
    def __init__(self):
        self.settings = {}
        self.calls = []
        self.run_bodies = []
        self.jobs = SimpleNamespace(
            list=lambda: [
                SimpleNamespace(job_id=i, settings=copy.deepcopy(s))
                for i, s in self.settings.items()
            ],
            get=lambda i: SimpleNamespace(settings=copy.deepcopy(self.settings[i])),
            get_run=lambda i: {"job_id": 10, "state": {"result_state": "SUCCESS"}},
        )
        self.api_client = SimpleNamespace(do=self.do)

    def do(self, method, path, body):
        self.calls.append(path)
        if path.endswith("/create"):
            self.settings[10] = copy.deepcopy(body)
            return {"job_id": 10}
        if path.endswith("/run-now"):
            self.run_bodies.append(copy.deepcopy(body))
            return {"run_id": 99}
        raise AssertionError(path)


def test_actual_adapter_creates_then_executes_only_after_durable_approval():
    p = approved()
    w = Workspace()
    create_or_recover_clone(w, p, "ready")
    p.bind_baseline("ready", {"table": "c.s.baseline", "version": 0})
    submit_or_recover_run(w, p, "ready")
    observe_run(w, p, "ready")
    assert p.state("ready")["status"] == "EXECUTED"
    assert w.calls == ["/api/2.2/jobs/create", "/api/2.2/jobs/run-now"]
    assert len(w.run_bodies[0]["idempotency_token"]) == 64
    assert "schedule" not in w.settings[10]


def test_interrupted_clone_and_run_recovery_use_same_resources():
    p = approved()
    w = Workspace()
    create_or_recover_clone(w, p, "ready")
    actual = w.settings[10]
    # Simulate journal stopping after clone request while resource already exists.
    p.journal.rows.pop()
    create_or_recover_clone(w, p, "ready")
    assert w.calls.count("/api/2.2/jobs/create") == 1
    p.bind_baseline("ready", {"table": "c.s.baseline", "version": 0})
    submit_or_recover_run(w, p, "ready")
    token = w.run_bodies[0]["idempotency_token"]
    p.journal.rows.pop()
    submit_or_recover_run(w, p, "ready")
    assert w.run_bodies[-1]["idempotency_token"] == token


def test_existing_clone_drift_prevents_run_submission():
    p = approved()
    w = Workspace()
    create_or_recover_clone(w, p, "ready")
    p.bind_baseline("ready", {"table": "c.s.baseline", "version": 0})
    w.settings[10]["description"] = "tampered"
    with pytest.raises(ValueError):
        submit_or_recover_run(w, p, "ready")
    assert "/api/2.2/jobs/run-now" not in w.calls


def test_run_refused_until_baseline_pinned():
    p = approved()
    s = p.prepare_clone("ready")
    p.attach_clone("ready", 10, s["candidate"])
    with pytest.raises(ValueError, match="Pin"):
        p.execution_requested("ready")


def test_validation_cannot_substitute_another_baseline_snapshot():
    p, actual = executed()
    r = report()
    r["baseline_table_version"] = 1
    with pytest.raises(ValueError, match="snapshot binding"):
        p.validate(
            "ready",
            r,
            run_id=99,
            source_settings=p.state("ready")["source_settings"],
            clone_settings=actual,
        )


def test_validation_failure_is_persisted_without_promotion():
    p, actual = executed()
    result = p.validate(
        "ready",
        report(False),
        run_id=99,
        source_settings=p.state("ready")["source_settings"],
        clone_settings=actual,
    )
    assert result["status"] == "VALIDATION_FAILED" and not result["cutover_allowed"]
    assert p.events()[-1]["event_type"] == "VALIDATION_FAILED"


def test_pinned_baseline_cannot_change_after_binding():
    p = approved()
    s = p.prepare_clone("ready")
    p.attach_clone("ready", 10, s["candidate"])
    p.bind_baseline("ready", {"table": "c.s.baseline", "version": 0})
    with pytest.raises(ValueError, match="changed"):
        p.bind_baseline("ready", {"table": "c.s.baseline", "version": 1})


def test_large_plan_roundtrips_through_official_sdk():
    from databricks.sdk.service.jobs import JobSettings

    candidate = build_serverless_clone(fixtures()[103], allow_large=True)
    parsed = JobSettings.from_dict(candidate).as_dict()
    assert len(parsed["tasks"]) == 11
    assert parsed["tasks"] == candidate["tasks"]


def test_programme_rejects_unresolved_shared_cluster():
    source = fixtures()[101]
    source["tasks"][0].pop("new_cluster")
    source["tasks"][0]["existing_cluster_id"] = "unknown"
    p = assessed(source)
    assert p.state("ready")["status"] == "BLOCKED"
    with pytest.raises(ValueError, match="Shared cluster"):
        p.plan("ready")


def test_init_script_is_blocked_during_programme_assessment():
    source = fixtures()[101]
    source["tasks"][0]["new_cluster"]["init_scripts"] = [
        {"dbfs": {"destination": "dbfs:/example"}}
    ]
    p = assessed(source)
    assert p.state("ready")["status"] == "BLOCKED"


def test_missing_audit_revision_blocks_state_recovery():
    p = approved()
    p.journal.rows.pop(1)
    with pytest.raises(RuntimeError, match="revision gap"):
        p.states()


def test_snapshot_cannot_be_relabelled_as_another_workload():
    p = assessed()
    p.journal.rows[0]["state"]["workload_id"] = "different"
    with pytest.raises(RuntimeError, match="does not match"):
        p.states()


def test_unsupported_state_schema_is_not_silently_loaded():
    p = assessed()
    p.journal.rows[0]["state"]["schema_version"] = 999
    with pytest.raises(RuntimeError, match="Unsupported"):
        p.states()


def test_infrastructure_tags_are_in_approved_candidate_digest():
    p = approved()
    state = p.state("ready")
    assert state["candidate"]["tags"]["smct_program"] == "demo"
    assert state["candidate"]["tags"]["smct_workload"] == "ready"
    w = Workspace()
    create_or_recover_clone(w, p, "ready")
    assert w.settings[10]["tags"] == state["candidate"]["tags"]


def test_wrong_workload_validation_report_cannot_be_attached():
    p, actual = executed()
    r = report()
    r["workload_id"] = "different"
    with pytest.raises(ValueError, match="workload/run binding"):
        p.validate(
            "ready",
            r,
            run_id=99,
            source_settings=p.state("ready")["source_settings"],
            clone_settings=actual,
        )
