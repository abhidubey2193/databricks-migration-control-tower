"""A single-workload lifecycle joined by ID, with append-only audit snapshots.

The Delta journal is for one serialized sandbox runner per program. It detects
conflicting revisions but is not a distributed lock or enterprise approval service.
"""

import copy
import hashlib
from datetime import datetime, timezone
from typing import Protocol

from .assessment import check_job, score
from .models import Finding
from .orchestration import build_serverless_clone, digest, validate_task_graph


def utcnow():
    return datetime.now(timezone.utc).isoformat()


class Journal(Protocol):
    def events(self, program_id: str) -> list[dict]: ...
    def append(self, event: dict, expected_sequence: int) -> None: ...


class MemoryJournal:
    def __init__(self):
        self.rows = []

    def events(self, program_id):
        return [copy.deepcopy(e) for e in self.rows if e["program_id"] == program_id]

    def append(self, event, expected_sequence):
        prior = [
            e
            for e in self.rows
            if e["program_id"] == event["program_id"]
            and e["workload_id"] == event["workload_id"]
        ]
        if max((e["sequence"] for e in prior), default=0) != expected_sequence:
            raise RuntimeError("Concurrent state change; reload before acting")
        self.rows.append(copy.deepcopy(event))


class Program:
    def __init__(self, journal: Journal, program_id: str, actor: str):
        if not program_id or not actor:
            raise ValueError("Program ID and authenticated actor are required")
        self.journal = journal
        self.program_id = program_id
        self.actor = actor

    def events(self):
        return sorted(
            self.journal.events(self.program_id),
            key=lambda e: (e["workload_id"], e["sequence"]),
        )

    def states(self):
        latest = {}
        seen = set()
        revisions = {}
        for event in self.events():
            key = (event["workload_id"], event["sequence"])
            if key in seen:
                raise RuntimeError("Duplicate journal revision; stop and reconcile")
            expected = revisions.get(event["workload_id"], 0) + 1
            if event["sequence"] != expected:
                raise RuntimeError("Journal revision gap; stop and reconcile")
            snapshot = event["state"]
            if (
                snapshot.get("workload_id") != event["workload_id"]
                or snapshot.get("sequence") != event["sequence"]
            ):
                raise RuntimeError("Journal state does not match its event")
            if snapshot.get("schema_version") != 1:
                raise RuntimeError(
                    "Unsupported programme state schema; use a new program_id after upgrading"
                )
            revisions[event["workload_id"]] = event["sequence"]
            seen.add(key)
            latest[event["workload_id"]] = copy.deepcopy(event["state"])
        return latest

    def state(self, wid):
        try:
            return self.states()[wid]
        except KeyError:
            raise ValueError(f"Unknown workload {wid}")

    def _write(self, wid, event_type, state, details=None):
        current = self.states().get(wid)
        previous = current["sequence"] if current else 0
        result = copy.deepcopy(state)
        result["sequence"] = previous + 1
        event = {
            "program_id": self.program_id,
            "workload_id": wid,
            "sequence": previous + 1,
            "event_type": event_type,
            "actor": self.actor,
            "event_at": utcnow(),
            "state": result,
            "details": details or {},
        }
        self.journal.append(event, previous)
        return result

    def assess(self, wid, settings, *, synthetic, baseline_source, owner):
        from .secrets import reject_literal_credentials

        reject_literal_credentials(settings)
        existing = self.states().get(wid)
        if existing:
            if existing["source_digest"] != digest(settings):
                raise ValueError(
                    "Source drift: use a new workload ID or reconcile explicitly"
                )
            return existing
        findings = check_job(settings)
        readiness, eligible = score(findings)
        if eligible:
            try:
                if any(t.get("existing_cluster_id") for t in settings.get("tasks", [])):
                    raise ValueError(
                        "Shared cluster configuration has not been resolved"
                    )
                build_serverless_clone(settings)
            except ValueError as exc:
                findings.append(
                    Finding(
                        "L1",
                        "blocker",
                        str(exc),
                        "Resolve the planning restriction before approval",
                    )
                )
                readiness, eligible = score(findings)
        state = {
            "schema_version": 1,
            "workload_id": wid,
            "name": settings.get("name", wid),
            "owner": owner,
            "synthetic": synthetic,
            "baseline_source": baseline_source,
            "source_digest": digest(settings),
            "source_settings": copy.deepcopy(settings),
            "status": "READY" if eligible else "BLOCKED",
            "readiness": readiness,
            "eligible": eligible,
            "findings": [f.__dict__ for f in findings],
            "cutover_allowed": False,
            "approval": None,
            "wave": None,
        }
        return self._write(wid, "ASSESSED", state)

    def wave(self, wid, wave):
        state = self.state(wid)
        if state["status"] not in ("READY", "PLANNED"):
            raise ValueError("Only ready/planned workloads can enter a wave")
        if not isinstance(wave, int) or wave < 1:
            raise ValueError("Wave must be positive")
        state["wave"] = wave
        return self._write(wid, "WAVE_ASSIGNED", state)

    def disposition(self, wid, *, decision, reason, is_workspace_admin=False):
        state = self.state(wid)
        if state["status"] not in (
            "READY",
            "BLOCKED",
            "PLANNED",
            "KEEP_CLASSIC",
            "DEFERRED",
        ):
            raise ValueError("Disposition must precede approval/execution")
        if decision not in ("KEEP_CLASSIC", "DEFERRED") or not reason.strip():
            raise ValueError("KEEP_CLASSIC/DEFERRED and a reason required")
        if not is_workspace_admin:
            raise PermissionError("Authenticated administrator disposition required")
        state.update(
            status=decision,
            disposition=dict(
                decision=decision, reason=reason, actor=self.actor, at=utcnow()
            ),
        )
        return self._write(wid, "DISPOSITION_RECORDED", state)

    def plan(
        self,
        wid,
        *,
        allow_large=False,
        resolved_settings=None,
        dependencies=(),
        task_dependencies=None,
    ):
        state = self.state(wid)
        if state["status"] not in ("READY", "BLOCKED", "PLANNED"):
            raise ValueError("Plan can only be created before approval")
        planning_source = (
            resolved_settings
            if resolved_settings is not None
            else state["source_settings"]
        )
        if resolved_settings is not None:
            before = [
                (t["task_key"], t.get("depends_on", []))
                for t in state["source_settings"]["tasks"]
            ]
            after = [
                (t["task_key"], t.get("depends_on", []))
                for t in resolved_settings["tasks"]
            ]
            if before != after:
                raise ValueError("Resolution cannot change the task graph")
        if any(t.get("existing_cluster_id") for t in planning_source.get("tasks", [])):
            raise ValueError(
                "Shared cluster configuration must be resolved before programme planning"
            )
        candidate = build_serverless_clone(planning_source, allow_large=allow_large)
        if task_dependencies is not None:
            if dependencies:
                raise ValueError("Choose shared or task-specific dependencies")
            from .remediation import assign_task_environments

            candidate = assign_task_environments(candidate, task_dependencies)
        if dependencies:
            from .remediation import dependency_plan
            import shlex

            for dependency in dependencies:
                if not dependency_plan("pip install " + shlex.quote(dependency))[
                    "can_apply"
                ]:
                    raise ValueError("Unreviewable environment dependency")
            candidate["environments"][0]["spec"]["dependencies"] = sorted(
                set(dependencies)
            )
        candidate.setdefault("tags", {}).update(
            smct_program=self.program_id, smct_workload=wid
        )
        state.update(
            candidate=candidate,
            candidate_digest=digest(candidate),
            status="PLANNED",
            plan_method=(
                "direct_jobs_api_preserved_dag"
                if allow_large
                else "guarded_serverless_clone"
            ),
            task_layers=validate_task_graph(candidate["tasks"]),
            resolution_digest=digest(planning_source),
        )
        return self._write(
            wid,
            "PLAN_CREATED",
            state,
            {"allow_large": allow_large, "task_count": len(candidate["tasks"])},
        )

    def approve(self, wid, *, is_workspace_admin=False, reason=""):
        state = self.state(wid)
        if state["status"] != "PLANNED":
            raise ValueError("Review the generated plan before approving")
        if not reason.strip():
            raise ValueError("Approval reason required")
        sandbox_owner = state["synthetic"] and self.actor == state["owner"]
        if not is_workspace_admin and not sandbox_owner:
            raise PermissionError(
                "Workspace administrator or synthetic-fixture owner approval required"
            )
        if not state["synthetic"] and state["baseline_source"] != "executed_classic":
            raise ValueError(
                "Real workload requires executed classic baseline provenance"
            )
        state.update(
            status="APPROVED",
            approval={
                "actor": self.actor,
                "scope": (
                    "workspace_admin"
                    if is_workspace_admin
                    else "authenticated_synthetic_fixture_owner"
                ),
                "reason": reason,
                "approved_at": utcnow(),
                "source_digest": state["source_digest"],
                "candidate_digest": state["candidate_digest"],
            },
        )
        return self._write(wid, "APPROVED", state)

    def prepare_clone(self, wid):
        state = self.state(wid)
        if state["status"] not in ("APPROVED", "CLONE_REQUESTED"):
            raise PermissionError("Approval required before clone creation")
        approval = state["approval"]
        if (
            not approval
            or approval["candidate_digest"] != digest(state["candidate"])
            or approval["source_digest"] != digest(state["source_settings"])
        ):
            raise ValueError("Approved configuration drifted")
        if state["status"] == "CLONE_REQUESTED":
            return state
        state.update(
            status="CLONE_REQUESTED",
            backup={
                "settings": copy.deepcopy(state["source_settings"]),
                "digest": state["source_digest"],
                "created_at": utcnow(),
            },
        )
        return self._write(wid, "BACKUP_SAVED_CLONE_REQUESTED", state)

    def attach_clone(self, wid, job_id, actual_settings):
        state = self.state(wid)
        if state["status"] != "CLONE_REQUESTED":
            raise ValueError("Clone was not requested")
        from .program_runtime import check_candidate

        check_candidate(actual_settings, state["candidate"])
        state.update(
            status="CLONED",
            clone_job_id=int(job_id),
            clone_digest=digest(actual_settings),
        )
        return self._write(wid, "CLONE_CREATED", state)

    def bind_baseline(self, wid, descriptor):
        state = self.state(wid)
        if state["status"] not in ("CLONED", "EXECUTION_REQUESTED"):
            raise ValueError("Bind isolated inputs before submitting the run")
        if (
            not descriptor.get("table")
            or not isinstance(descriptor.get("version"), int)
            or descriptor["version"] < 0
        ):
            raise ValueError("A pinned Delta baseline table/version is required")
        if state.get("baseline"):
            if state["baseline"] != descriptor:
                raise ValueError("Pinned baseline changed")
            return state
        state["baseline"] = copy.deepcopy(descriptor)
        return self._write(wid, "BASELINE_SNAPSHOT_BOUND", state)

    def billing_observed(self, wid, observation):
        state = self.state(wid)
        if state["status"] not in (
            "EXECUTED",
            "VALIDATED",
            "VALIDATION_FAILED",
            "MIGRATED_PAUSED",
            "MIGRATED",
            "ROLLED_BACK_PAUSED",
            "ROLLED_BACK",
        ):
            raise ValueError("Execution evidence required")
        if state.get("billing") == observation:
            return state
        state["billing"] = copy.deepcopy(observation)
        return self._write(wid, "BILLING_OBSERVED", state)

    def execution_requested(self, wid):
        state = self.state(wid)
        if state["status"] not in ("CLONED", "EXECUTION_REQUESTED"):
            raise ValueError("Verified clone required")
        if not state.get("baseline"):
            raise ValueError("Pin the isolated baseline snapshot before execution")
        if state["status"] == "EXECUTION_REQUESTED":
            return state
        state.update(
            status="EXECUTION_REQUESTED",
            run_token=hashlib.sha256(
                (self.program_id + ":" + wid + ":run1").encode()
            ).hexdigest(),
        )
        return self._write(wid, "EXECUTION_REQUESTED", state)

    def attach_run(self, wid, run_id):
        state = self.state(wid)
        if state["status"] != "EXECUTION_REQUESTED":
            raise ValueError("Execution not requested")
        state.update(status="EXECUTING", run_id=int(run_id))
        return self._write(wid, "RUN_SUBMITTED", state)

    def execution_finished(self, wid, result_state, job_id):
        state = self.state(wid)
        if state["status"] != "EXECUTING" or int(job_id) != state["clone_job_id"]:
            raise ValueError("Run/job binding mismatch")
        state.update(
            status="EXECUTED" if result_state == "SUCCESS" else "EXECUTION_FAILED",
            run_result=result_state,
        )
        return self._write(wid, "RUN_FINISHED", state)

    def validate(self, wid, report, *, run_id, source_settings, clone_settings):
        state = self.state(wid)
        if state["status"] != "EXECUTED" or int(run_id) != state["run_id"]:
            raise ValueError("Successful bound run required before validation")
        if (
            digest(source_settings) != state["source_digest"]
            or digest(clone_settings) != state["clone_digest"]
        ):
            raise ValueError("Configuration drift blocks validation")
        if not report.get("typed_schema_compared"):
            raise ValueError("Typed schema comparison required")
        if report.get("baseline_source") != state["baseline_source"]:
            raise ValueError("Baseline provenance mismatch")
        if (
            report.get("workload_id") != wid
            or report.get("program_id") != self.program_id
            or report.get("serverless_run_id") != state["run_id"]
        ):
            raise ValueError("Validation report workload/run binding mismatch")
        baseline = state.get("baseline", {})
        if report.get("baseline_table") != baseline.get("table") or report.get(
            "baseline_table_version"
        ) != baseline.get("version"):
            raise ValueError("Baseline snapshot binding mismatch")
        if report.get("passed") and (
            not report.get("schema_equal")
            or not report.get("columns_equal")
            or report.get("missing_records") != 0
            or report.get("extra_records") != 0
            or report.get("classic_rows") != report.get("serverless_rows")
        ):
            raise ValueError("Inconsistent PASS evidence")
        state.update(
            status="VALIDATED" if report["passed"] else "VALIDATION_FAILED",
            validation=copy.deepcopy(report),
            cutover_allowed=False,
        )
        return self._write(
            wid, "VALIDATION_PASSED" if report["passed"] else "VALIDATION_FAILED", state
        )

    def negative_control(self, wid, report):
        state = self.state(wid)
        if state["status"] != "VALIDATED" or report.get("passed") is not False:
            raise ValueError("Negative control must fail after a successful validation")
        state["negative_control"] = copy.deepcopy(report)
        return self._write(wid, "NEGATIVE_CONTROL_REJECTED", state)
