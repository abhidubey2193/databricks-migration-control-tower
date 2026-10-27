import copy
import json
from types import SimpleNamespace as NS

import pytest

from smct.evidence import load_evidence
from smct.lease import DeltaLease
from smct.program import Program, MemoryJournal
from smct.secrets import reject_literal_credentials
from smct.demo import fixtures


class LeaseSpark:
    def __init__(self):
        self.owner = None
        self.live = False

    def table(self, table):
        return NS(count=lambda: 1)

    def sql(self, statement):
        import re

        if statement.startswith("CREATE TABLE"):
            return None
        if statement.startswith("SELECT owner"):
            return NS(collect=lambda: [{"owner": self.owner, "live": self.live}])
        owner = re.search(r"owner='([a-f0-9]+)'", statement)
        token = owner.group(1) if owner else None
        if "SET owner=NULL" in statement:
            if self.owner == token:
                self.owner, self.live = None, False
        elif "SET owner=" in statement:
            if not self.live or self.owner == token:
                self.owner, self.live = token, True
        elif self.owner == token and self.live:
            self.live = True
        return None


def test_delta_lease_excludes_second_owner_and_detects_expired_writer():
    spark = LeaseSpark()
    one, two = DeltaLease(spark, "c.s.lock"), DeltaLease(spark, "c.s.lock")
    one.acquire()
    with pytest.raises(RuntimeError, match="Another"):
        two.acquire()
    two.release()
    one.renew()  # nonowner cannot release another controller's lease
    spark.live = False
    two.acquire()
    with pytest.raises(RuntimeError, match="Another"):
        one.renew()
    one.release()
    two.assert_owned()


@pytest.mark.parametrize(
    "settings",
    [
        {"spark_env_vars": {"AWS_SECRET_ACCESS_KEY": "literal"}},
        {"parameters": [{"name": "access_token", "default": "literal"}]},
        {"notebook_task": {"base_parameters": {"password": "literal"}}},
    ],
)
def test_literal_credentials_refused_before_backups(settings):
    with pytest.raises(ValueError, match="Literal credential"):
        reject_literal_credentials(settings)
    assert reject_literal_credentials({"password": "{{secrets/scope/key}}"}) is None


def test_manual_disposition_has_authenticated_reason_and_does_not_claim_migration():
    p = Program(MemoryJournal(), "review", "admin")
    p.assess(
        "ml",
        fixtures()[102],
        synthetic=False,
        baseline_source="not_executed",
        owner="admin",
    )
    with pytest.raises(PermissionError):
        p.disposition("ml", decision="KEEP_CLASSIC", reason="Needs CUDA")
    state = p.disposition(
        "ml", decision="KEEP_CLASSIC", reason="Needs CUDA", is_workspace_admin=True
    )
    assert state["status"] == "KEEP_CLASSIC" and not state["cutover_allowed"]
    assert state["disposition"]["actor"] == "admin"


def test_shared_evidence_reader_joins_source_status_and_never_counts_reports_as_migrations():
    state = {
        "schema_version": 1,
        "sequence": 1,
        "workload_id": "orders",
        "source_job_id": 8,
        "name": "Orders",
        "status": "VALIDATED",
        "baseline_source": "executed_classic",
        "validation": {"passed": True},
    }
    snapshot = {
        "metadata": {"complete": True},
        "workloads": [
            {
                "workload_id": "job:8",
                "attributes": {"job_id": 8},
                "status": "DISCOVERED",
            }
        ],
    }
    row = dict(
        program_id="fleet",
        workload_id="orders",
        sequence=1,
        event_type="ASSESSED",
        actor="admin",
        event_at="2026-10-27T00:00:00",
        state_json=json.dumps(state),
    )

    def query(sql):
        if sql.startswith("SHOW TABLES"):
            return [{}] if "program_events" in sql else []
        if "fleet_snapshots" in sql:
            return [dict(scanned_at="now", payload_json=json.dumps(snapshot))]
        if "program_events" in sql:
            return [row]
        raise AssertionError(sql)

    evidence = load_evidence(query, "c", "s")
    assert evidence["payload"]["workloads"][0]["status"] == "VALIDATED"
    assert evidence["program_states"][0]["baseline_source"] == "executed_classic"
    assert len(evidence["reports"]) == 1


def test_evidence_reader_refuses_duplicate_audit_revisions():
    state = {"workload_id": "orders", "sequence": 1, "status": "VALIDATED"}
    row = dict(
        program_id="fleet",
        workload_id="orders",
        sequence=1,
        event_type="VALIDATION_PASSED",
        actor="admin",
        event_at="now",
        state_json=json.dumps(state),
    )

    def query(sql):
        if sql.startswith("SHOW TABLES"):
            return [{}] if "program_events" in sql else []
        if "fleet_snapshots" in sql:
            return [dict(scanned_at="now", payload_json='{"workloads":[]}')]
        return [row, copy.deepcopy(row)]

    with pytest.raises(ValueError, match="Duplicate"):
        load_evidence(query, "c", "s")
