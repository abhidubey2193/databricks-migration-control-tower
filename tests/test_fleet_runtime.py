import base64
import copy
from types import SimpleNamespace as NS

import pytest

from smct.fleet import FleetRunner
from smct.program import Program, MemoryJournal
from smct.promotion import PromotionController, active_triggers
from smct.validation import compare_rows
from smct.orchestration import build_serverless_clone, digest
from smct.discovery import get_job_settings


class Frame:
    def __init__(self, first=None):
        self.row = first

    def first(self):
        return self.row


class Spark:
    def __init__(self):
        self.tables = {"c.s.input": [(1, 10), (2, None), (1, 10)]}
        self.catalog = NS(
            tableExists=lambda table: table.replace("`", "") in self.tables
        )

    def sql(self, sql):
        if sql == "SELECT current_user()":
            return Frame(["admin@example.invalid"])
        if sql.startswith("DESCRIBE HISTORY "):
            name = sql.split()[2].replace("`", "")
            if name not in self.tables:
                raise ValueError("Missing output")
            return Frame({"version": 0})
        if sql.startswith("SELECT * FROM "):
            if sql.split()[3].replace("`", "") not in self.tables:
                raise ValueError("Missing input")
            return Frame()
        raise AssertionError(sql)


class Workspace:
    def __init__(self, spark):
        self.spark = spark
        self.settings = {
            1: {
                "name": "Orders",
                "schedule": {
                    "quartz_cron_expression": "0 0 1 * * ?",
                    "timezone_id": "UTC",
                    "pause_status": "UNPAUSED",
                },
                "tags": {"team": "data"},
                "tasks": [
                    {
                        "task_key": "orders",
                        "new_cluster": {
                            "spark_version": "15.4.x-scala2.12",
                            "num_workers": 0,
                            "node_type_id": "test",
                        },
                        "notebook_task": {
                            "notebook_path": "/Workspace/orders",
                            "source": "WORKSPACE",
                        },
                    }
                ],
            }
        }
        self.runs, self.tokens, self.calls = {}, {}, []
        self.current_user = NS(
            me=lambda: NS(
                user_name="admin@example.invalid", groups=[NS(display="admins")]
            )
        )
        self.workspace = NS(
            get_status=lambda p: NS(language=NS(value="PYTHON")),
            export=lambda *a, **k: NS(content=base64.b64encode(b"x=1\n").decode()),
        )
        self.jobs = NS(
            get=lambda i: NS(settings=copy.deepcopy(self.settings[i])),
            list=lambda: [NS(job_id=i, settings=s) for i, s in self.settings.items()],
            get_run=lambda i: self.runs[i],
            list_runs=lambda **kw: [],
        )
        self.api_client = NS(do=self.do)

    def do(self, method, path, body):
        self.calls.append((path, copy.deepcopy(body)))
        if path.endswith("/create"):
            job_id = max(self.settings) + 1
            self.settings[job_id] = copy.deepcopy(body)
            return {"job_id": job_id}
        if path.endswith("/run-now"):
            token = body["idempotency_token"]
            if token in self.tokens:
                return {"run_id": self.tokens[token]}
            run_id = 100 + len(self.runs)
            self.tokens[token] = run_id
            job_id = body["job_id"]
            self.runs[run_id] = {"job_id": job_id, "state": {"result_state": "SUCCESS"}}
            for task in self.settings[job_id]["tasks"]:
                p = task["notebook_task"]["base_parameters"]
                if p["output_table"] in self.spark.tables:
                    raise ValueError("Output already exists")
                self.spark.tables[p["output_table"]] = [
                    (i, v * 2 if v is not None else None)
                    for i, v in self.spark.tables[p["input_table"]]
                ]
            return {"run_id": run_id}
        if path.endswith("/reset"):
            self.settings[body["job_id"]] = copy.deepcopy(body["new_settings"])
            return {}
        if path.endswith("/update"):
            self.settings[body["job_id"]].update(copy.deepcopy(body["new_settings"]))
            return {}
        raise AssertionError(path)


@pytest.fixture
def setup(monkeypatch):
    spark = Spark()
    w = Workspace(spark)
    p = Program(MemoryJournal(), "fleet1", "admin@example.invalid")
    runner = FleetRunner(w, spark, p, "c", "s")
    item = {
        "id": "orders",
        "source_job_id": 1,
        "wave": 1,
        "sandbox_execution_reviewed": True,
        "inputs": {"table": "c.s.input", "version": 0},
        "output_task": "orders",
        "key_columns": ["id"],
    }

    def compare(spark, classic, target, **kw):
        assert kw["classic_version"] == kw["serverless_version"] == 0
        left = [{"id": i, "amount": v} for i, v in spark.tables[classic]]
        right = [{"id": i, "amount": v} for i, v in spark.tables[target]]
        report = compare_rows(
            left,
            right,
            classic_schema=["id LONG", "amount LONG"],
            serverless_schema=["id LONG", "amount LONG"],
            key_columns=["id"],
        )
        report.update(
            baseline_table=classic,
            baseline_table_version=0,
            serverless_table=target,
            serverless_table_version=0,
        )
        return report

    monkeypatch.setattr("smct.fleet.spark_compare", compare)
    return w, p, runner, item


def execute(setup):
    w, p, runner, item = setup
    runner.assess(item)
    runner.plan("orders")
    runner.approve("orders", "Reviewed pinned inputs and isolated outputs")
    for _ in range(5):
        runner.step("orders")
        if p.state("orders")["status"] == "VALIDATED":
            return w, p, runner
    raise AssertionError(p.state("orders"))


def test_real_fleet_lifecycle_runs_classic_and_serverless_clones_not_source(setup):
    w, p, runner = execute(setup)
    state = p.state("orders")
    assert state["validation"]["passed"]
    assert state["validation"]["baseline_source"] == "executed_classic"
    assert state["classic_job_id"] != state["clone_job_id"] != 1
    assert all(
        body["job_id"] != 1 for path, body in w.calls if path.endswith("/run-now")
    )
    assert active_triggers(w.settings[1])
    assert not active_triggers(w.settings[state["classic_job_id"]])
    assert not active_triggers(w.settings[state["clone_job_id"]])
    assert not state["cutover_allowed"]
    assert state["backup"]["digest"] == digest(w.settings[1])


def test_review_gate_before_any_job_creation(setup):
    w, p, runner, item = setup
    runner.assess(item)
    runner.plan("orders")
    with pytest.raises(PermissionError):
        runner.step("orders")
    assert not w.calls


def test_manifest_changes_cannot_reuse_approval(setup):
    w, p, runner, item = setup
    runner.assess(item)
    altered = dict(item, wave=2)
    with pytest.raises(ValueError, match="Manifest drift"):
        runner.assess(altered)


def test_isolation_contract_required(setup):
    w, p, runner, item = setup
    item["sandbox_execution_reviewed"] = False
    runner.assess(item)
    with pytest.raises(ValueError, match="side effects"):
        runner.plan("orders")
    assert not w.calls


def test_code_drift_blocks_execution_before_mutation(setup):
    w, p, runner, item = setup
    runner.assess(item)
    runner.plan("orders")
    runner.approve("orders", "reviewed")
    w.workspace.export = lambda *a, **k: NS(content=base64.b64encode(b"x=2\n").decode())
    with pytest.raises(ValueError, match="code drift"):
        runner.step("orders")
    assert not w.calls


def test_interrupted_classic_submission_recovers_with_idempotency(setup):
    w, p, runner, item = setup
    runner.assess(item)
    runner.plan("orders")
    runner.approve("orders", "reviewed")
    runner.step("orders")
    run = p.state("orders")["classic_run_id"]
    p.journal.rows.pop()  # durable request survived, run attachment did not
    runner.step("orders")
    assert p.state("orders")["classic_run_id"] == run and len(w.runs) == 1
    assert len(w.settings) == 2


def test_configuration_contract_tampering_blocks_execution(setup):
    w, p, runner, item = setup
    runner.assess(item)
    runner.plan("orders")
    runner.approve("orders", "reviewed")
    p.journal.rows[-1]["state"]["input_snapshot"]["version"] = 2
    with pytest.raises(ValueError, match="contract drift"):
        runner.step("orders")
    assert not w.calls


def test_large_chain_actually_executes_both_sides(setup):
    w, p, runner, item = setup
    template = w.settings[1]["tasks"][0]
    tasks = []
    for i in range(11):
        task = copy.deepcopy(template)
        task["task_key"] = f"t{i}"
        if i:
            task["depends_on"] = [{"task_key": f"t{i - 1}"}]
        tasks.append(task)
    w.settings[1]["tasks"] = tasks
    item["output_task"] = "t10"
    item["task_inputs"] = {f"t{i}": f"t{i - 1}" for i in range(1, 11)}
    w, p, runner = execute(setup)
    assert len(w.settings[p.state("orders")["clone_job_id"]]["tasks"]) == 11
    assert p.state("orders")["validation"]["passed"]
    assert len(w.runs) == 2


def test_cutover_then_activation_then_rollback_round_trip(setup, tmp_path):
    w, p, runner = execute(setup)
    original = copy.deepcopy(w.settings[1])
    controller = PromotionController(w, p)
    assert controller.gate("orders")["allowed"]
    with pytest.raises(ValueError, match="Bundle"):
        controller.cutover("orders", reason="approved")
    controller.reconcile("orders", tmp_path, reason="reviewed Bundle")
    controller.cutover("orders", reason="approved paused cutover")
    assert p.state("orders")["status"] == "MIGRATED_PAUSED"
    assert not active_triggers(w.settings[1])
    assert "new_cluster" not in w.settings[1]["tasks"][0]
    controller.activate("orders", reason="activate original schedule")
    assert p.state("orders")["status"] == "MIGRATED" and active_triggers(w.settings[1])
    controller.rollback("orders", reason="rollback exercise")
    assert p.state("orders")["status"] == "ROLLED_BACK_PAUSED"
    assert w.settings[1]["tasks"] == original["tasks"] and not active_triggers(
        w.settings[1]
    )
    controller.activate("orders", reason="restore classic schedule")
    assert w.settings[1] == original
    assert p.state("orders")["status"] == "ROLLED_BACK"


def test_active_runs_prevent_cutover(setup, tmp_path):
    w, p, runner = execute(setup)
    controller = PromotionController(w, p)
    controller.reconcile("orders", tmp_path, reason="review")
    w.jobs.list_runs = lambda **kw: [NS(run_id=4)]
    with pytest.raises(ValueError, match="active runs"):
        controller.cutover("orders", reason="test")
    assert p.state("orders")["status"] == "CUTOVER_REQUESTED"
    assert not active_triggers(w.settings[1])
    w.jobs.list_runs = lambda **kw: []
    controller.cutover("orders", reason="resume after drain")
    assert p.state("orders")["status"] == "MIGRATED_PAUSED"


def test_cutover_recovery_after_reset_before_event_commit(setup, tmp_path):
    w, p, runner = execute(setup)
    controller = PromotionController(w, p)
    controller.reconcile("orders", tmp_path, reason="review")
    controller.cutover("orders", reason="review")
    p.journal.rows.pop()
    before = len([x for x in w.calls if x[0].endswith("/reset")])
    controller.cutover("orders", reason="recover")
    assert p.state("orders")["status"] == "MIGRATED_PAUSED"
    assert len([x for x in w.calls if x[0].endswith("/reset")]) == before


def test_admin_identity_not_widget_text(setup):
    w, p, runner, item = setup
    runner.assess(item)
    runner.plan("orders")
    w.current_user.me = lambda: NS(user_name="admin@example.invalid", groups=[])
    fresh = FleetRunner(w, runner.spark, p, "c", "s")
    with pytest.raises(PermissionError):
        fresh.approve("orders", "review")


def test_paginated_job_preserves_task_arrays():
    pages = [
        NS(
            settings={
                "tasks": [{"task_key": "a"}],
                "job_clusters": [{"job_cluster_key": "c"}],
            },
            next_page_token="next",
        ),
        NS(
            settings={"tasks": [{"task_key": "b"}], "job_clusters": []},
            next_page_token=None,
        ),
    ]
    calls = []

    def get(i, **kw):
        calls.append(kw)
        return pages[1] if kw else pages[0]

    result = get_job_settings(NS(jobs=NS(get=get)), 1)
    assert [t["task_key"] for t in result["tasks"]] == ["a", "b"]
    assert result["job_clusters"] == [{"job_cluster_key": "c"}]
    assert calls == [{}, dict(page_token="next")]


def test_trigger_array_removed_from_validation_clone():
    source = {
        "name": "test",
        "triggers": [
            {"pause_status": "UNPAUSED", "periodic": {"interval": 1, "unit": "DAYS"}}
        ],
        "tasks": [
            {
                "task_key": "x",
                "notebook_task": {"notebook_path": "/x"},
                "new_cluster": {"spark_version": "15.4"},
            }
        ],
    }
    assert "triggers" not in build_serverless_clone(source)


def test_classic_failure_does_not_create_or_submit_serverless_target(setup):
    w, p, runner, item = setup
    runner.assess(item)
    runner.plan("orders")
    runner.approve("orders", "review")
    runner.step("orders")
    w.runs[p.state("orders")["classic_run_id"]]["state"]["result_state"] = "FAILED"
    state = runner.step("orders")
    assert state["status"] == "EXECUTION_FAILED" and not state.get("clone_job_id")
    assert len(w.runs) == 1


def test_cutover_refuses_a_corrupt_backup(setup):
    w, p, runner = execute(setup)
    p.journal.rows[-1]["state"]["backup"]["settings"]["name"] = "tampered"
    assert not PromotionController(w, p).gate("orders")["allowed"]


def test_source_changes_after_review_cannot_start_any_job(setup):
    w, p, runner, item = setup
    runner.assess(item)
    runner.plan("orders")
    runner.approve("orders", "review")
    w.settings[1]["name"] = "Edited elsewhere"
    with pytest.raises(ValueError, match="configuration drift"):
        runner.step("orders")
    assert not w.calls


def test_rollback_does_not_overwrite_unreviewed_job_changes(setup, tmp_path):
    w, p, runner = execute(setup)
    controller = PromotionController(w, p)
    controller.reconcile("orders", tmp_path, reason="review")
    controller.cutover("orders", reason="cutover")
    w.settings[1]["description"] = "Changed elsewhere"
    with pytest.raises(ValueError, match="drift"):
        controller.rollback("orders", reason="rollback")


def test_retrying_cutover_after_code_edit_is_refused(setup, tmp_path):
    w, p, runner = execute(setup)
    controller = PromotionController(w, p)
    controller.reconcile("orders", tmp_path, reason="review")
    controller.cutover("orders", reason="cutover")
    p.journal.rows.pop()
    w.workspace.export = lambda *a, **k: NS(content=base64.b64encode(b"x=2").decode())
    with pytest.raises(ValueError, match="source drift"):
        controller.cutover("orders", reason="recover")
