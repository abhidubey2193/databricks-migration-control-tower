"""Explicit staged cutover/rollback on the original job ID.

Both reset operations leave every source trigger PAUSED. Activation is a separate
admin action after drift and active-run checks. No automatic fallback starts a job.
"""

import copy
from pathlib import Path

from .discovery import get_job_settings
from .fleet import authenticated_identity, notebook_digests, run_outcome
from .orchestration import TRIGGERS, digest
from .program_runtime import check_candidate
from .reconcile import bundle_document, write_bundle, verify_bundle


def paused(settings):
    result = copy.deepcopy(settings)
    for key in TRIGGERS:
        value = result.get(key)
        if not value:
            continue
        if isinstance(value, list):
            for trigger in value:
                trigger["pause_status"] = "PAUSED"
        else:
            value["pause_status"] = "PAUSED"
    return result


def active_triggers(settings):
    for key in TRIGGERS:
        value = settings.get(key)
        if value:
            for trigger in value if isinstance(value, list) else [value]:
                if trigger.get("pause_status", "UNPAUSED") != "PAUSED":
                    return True
    return False


def without_triggers(settings):
    result = copy.deepcopy(settings)
    for field in TRIGGERS:
        result.pop(field, None)
    return result


class PromotionController:
    def __init__(self, w, program):
        self.w, self.program = w, program

    def _admin(self, reason):
        actor, admin = authenticated_identity(self.w)
        if actor != self.program.actor or not admin or not reason.strip():
            raise PermissionError(
                "Authenticated workspace administrator and explicit reason required"
            )

    def _idle(self, state):
        for job_id in (
            state["source_job_id"],
            state["clone_job_id"],
            state["classic_job_id"],
        ):
            if next(
                iter(self.w.jobs.list_runs(job_id=int(job_id), active_only=True)), None
            ):
                raise ValueError(
                    f"Job {job_id} still has active runs; drain before cutover/rollback"
                )

    def _pause_checked(self, wid, state, before, before_digest, field):
        current = get_job_settings(self.w, state["source_job_id"])
        expected = paused(before)
        if digest(current) == before_digest and active_triggers(current):
            fields = {k: expected[k] for k in TRIGGERS if expected.get(k)}
            self.w.api_client.do(
                "POST",
                "/api/2.2/jobs/update",
                body=dict(job_id=state["source_job_id"], new_settings=fields),
            )
            current = get_job_settings(self.w, state["source_job_id"])
        if digest(current) != digest(expected) or active_triggers(current):
            raise ValueError(
                "Source changed while pausing triggers; stop and reconcile"
            )
        if state.get(field) != digest(current):
            state[field] = digest(current)
            state = self.program._write(wid, "SOURCE_TRIGGERS_PAUSED", state)
        self._idle(state)
        return state, current

    def gate(self, wid):
        state = self.program.state(wid)
        reasons = []
        if (
            state["status"] != "VALIDATED"
            or state.get("synthetic")
            or state.get("baseline_source") != "executed_classic"
        ):
            reasons.append("A genuine classic/serverless validation PASS is required")
        if not state.get("fleet_manifest") or not state.get("production_candidate"):
            reasons.append("Fleet isolation/promotion contract missing")
        if reasons:
            return dict(allowed=False, reasons=reasons)
        if (
            digest(get_job_settings(self.w, state["source_job_id"]))
            != state["source_digest"]
        ):
            reasons.append("Original job drift")
        if notebook_digests(self.w, state["source_settings"]) != state["code_digests"]:
            reasons.append("Notebook source drift")
        for job, run in (
            (state["classic_job_id"], state["classic_run_id"]),
            (state["clone_job_id"], state["run_id"]),
        ):
            if run_outcome(self.w.jobs.get_run(run), job) != "SUCCESS":
                reasons.append("Successful bound runs required")
        clone = get_job_settings(self.w, state["clone_job_id"])
        if digest(clone) != state["clone_digest"] or any(
            clone.get(k) for k in TRIGGERS
        ):
            reasons.append("Validation clone drift or duplicate trigger")
        if (
            digest(get_job_settings(self.w, state["classic_job_id"]))
            != state["classic_clone_digest"]
        ):
            reasons.append("Classic validation configuration drift")
        if (
            not state.get("backup")
            or state["backup"]["digest"] != state["source_digest"]
            or digest(state["backup"]["settings"]) != state["source_digest"]
        ):
            reasons.append("Verified rollback backup missing")
        if not state.get("validation", {}).get("passed"):
            reasons.append("Output validation failed")
        return dict(allowed=not reasons, reasons=reasons)

    def reconcile(self, wid, root, *, reason, expected_file_sha256=None):
        self._admin(reason)
        gate = self.gate(wid)
        if not gate["allowed"]:
            raise ValueError("; ".join(gate["reasons"]))
        state = self.program.state(wid)
        proposal = bundle_document(state["production_candidate"], "migrated_" + wid)
        path = write_bundle(
            root,
            f"resources/migrated_{wid}.yml",
            proposal,
            expected_sha256=expected_file_sha256,
        )
        state["reconciliation"] = dict(
            verify_bundle(
                path,
                resource_key="migrated_" + wid,
                expected_definition=state["production_candidate"],
            ),
            actor=self.program.actor,
            reason=reason,
        )
        # The actual Git commit/review remains visible to the operator, not fabricated.
        return self.program._write(wid, "BUNDLE_WRITTEN_AND_VERIFIED", state)

    def cutover(self, wid, *, reason):
        self._admin(reason)
        state = self.program.state(wid)
        if notebook_digests(self.w, state["source_settings"]) != state.get(
            "code_digests"
        ):
            raise ValueError("Notebook source drift blocks cutover")
        if state["status"] == "MIGRATED_PAUSED":
            return state
        if state["status"] != "CUTOVER_REQUESTED":
            gate = self.gate(wid)
            if not gate["allowed"]:
                raise ValueError("; ".join(gate["reasons"]))
            reconciliation = state.get("reconciliation")
            if not reconciliation:
                raise ValueError(
                    "Write/review the Bundle reconciliation before cutover"
                )
            verify_bundle(
                reconciliation["path"],
                resource_key=reconciliation["resource_key"],
                expected_definition=state["production_candidate"],
            )
            target = copy.deepcopy(state["production_candidate"])
            for field in TRIGGERS:
                if state["source_settings"].get(field):
                    target[field] = copy.deepcopy(state["source_settings"][field])
            state.update(
                status="CUTOVER_REQUESTED",
                cutover_target=paused(target),
                cutover_allowed=True,
            )
            state = self.program._write(
                wid, "CUTOVER_REQUESTED", state, {"reason": reason}
            )
        current = get_job_settings(self.w, state["source_job_id"])
        if digest(current) in (
            state["source_digest"],
            digest(paused(state["source_settings"])),
        ):
            state, current = self._pause_checked(
                wid,
                state,
                state["source_settings"],
                state["source_digest"],
                "cutover_pause_digest",
            )
            self.w.api_client.do(
                "POST",
                "/api/2.2/jobs/reset",
                body=dict(
                    job_id=state["source_job_id"], new_settings=state["cutover_target"]
                ),
            )
            current = get_job_settings(self.w, state["source_job_id"])
        # Safe recovery after reset succeeded but event commit was interrupted.
        check_candidate(without_triggers(current), state["production_candidate"])
        if active_triggers(current):
            raise ValueError(
                "Cutover left an active trigger; stop before further mutation"
            )
        state.update(status="MIGRATED_PAUSED", migrated_digest=digest(current))
        return self.program._write(wid, "CUTOVER_VERIFIED_PAUSED", state)

    def rollback(self, wid, *, reason):
        self._admin(reason)
        state = self.program.state(wid)
        if notebook_digests(self.w, state["source_settings"]) != state.get(
            "code_digests"
        ):
            raise ValueError(
                "Notebook source drift requires manual code reconciliation before rollback"
            )
        if state["status"] == "ROLLED_BACK_PAUSED":
            return state
        if state["status"] not in ("MIGRATED_PAUSED", "MIGRATED", "ROLLBACK_REQUESTED"):
            raise ValueError("Only a recorded cutover can be rolled back")
        if digest(state["backup"]["settings"]) != state["source_digest"]:
            raise ValueError("Rollback backup integrity failed")
        if state["status"] != "ROLLBACK_REQUESTED":
            if (
                digest(get_job_settings(self.w, state["source_job_id"]))
                != state["migrated_digest"]
            ):
                raise ValueError("Migrated job drift blocks rollback")
            state["status"] = "ROLLBACK_REQUESTED"
            state["rollback_live_settings"] = get_job_settings(
                self.w, state["source_job_id"]
            )
            state = self.program._write(
                wid, "ROLLBACK_REQUESTED", state, {"reason": reason}
            )
        desired = paused(state["backup"]["settings"])
        if state.get("reconciliation"):
            prior = state["reconciliation"]
            proposal = bundle_document(without_triggers(desired), prior["resource_key"])
            root = Path(prior["path"]).parent.parent
            path = write_bundle(
                root,
                "resources/" + Path(prior["path"]).name,
                proposal,
                expected_sha256=prior["content_sha256"],
            )
            restored_bundle = verify_bundle(
                path,
                resource_key=prior["resource_key"],
                expected_definition=without_triggers(desired),
            )
            if state.get("rollback_reconciliation") != restored_bundle:
                state["rollback_reconciliation"] = restored_bundle
                state = self.program._write(wid, "ROLLBACK_BUNDLE_VERIFIED", state)
        current = get_job_settings(self.w, state["source_job_id"])
        from .fleet import check_classic

        try:
            check_classic(without_triggers(current), without_triggers(desired))
            already_restored = not active_triggers(current)
        except ValueError:
            already_restored = False
        if not already_restored:
            if digest(current) not in (
                state["migrated_digest"],
                digest(paused(state["rollback_live_settings"])),
            ):
                raise ValueError("Ambiguous rollback configuration; reconcile manually")
            state, current = self._pause_checked(
                wid,
                state,
                state["rollback_live_settings"],
                state["migrated_digest"],
                "rollback_pause_digest",
            )
            self.w.api_client.do(
                "POST",
                "/api/2.2/jobs/reset",
                body=dict(job_id=state["source_job_id"], new_settings=desired),
            )
            current = get_job_settings(self.w, state["source_job_id"])
            check_classic(without_triggers(current), without_triggers(desired))
        if active_triggers(current):
            raise ValueError("Restored job must remain paused")
        state.update(
            status="ROLLED_BACK_PAUSED",
            restored_digest=digest(current),
            cutover_allowed=False,
        )
        return self.program._write(wid, "ROLLBACK_VERIFIED_PAUSED", state)

    def activate(self, wid, *, reason):
        self._admin(reason)
        state = self.program.state(wid)
        if notebook_digests(self.w, state["source_settings"]) != state.get(
            "code_digests"
        ):
            raise ValueError("Notebook source drift blocks activation")
        if state["status"] not in (
            "MIGRATED_PAUSED",
            "ROLLED_BACK_PAUSED",
            "ACTIVATION_REQUESTED",
        ):
            raise ValueError(
                "Activation requires a verified paused cutover or rollback"
            )
        self._idle(state)
        if any(
            get_job_settings(self.w, state["clone_job_id"]).get(k) for k in TRIGGERS
        ) or any(
            get_job_settings(self.w, state["classic_job_id"]).get(k) for k in TRIGGERS
        ):
            raise ValueError("A validation clone has triggers; refuse activation")
        if state["status"] != "ACTIVATION_REQUESTED":
            previous = state["status"]
            current = get_job_settings(self.w, state["source_job_id"])
            expected = (
                state["migrated_digest"]
                if previous == "MIGRATED_PAUSED"
                else state["restored_digest"]
            )
            if digest(current) != expected or active_triggers(current):
                raise ValueError("Paused job drift")
            desired = copy.deepcopy(current)
            for key in TRIGGERS:
                if state["source_settings"].get(key):
                    desired[key] = copy.deepcopy(state["source_settings"][key])
            state.update(
                status="ACTIVATION_REQUESTED",
                activation_target=desired,
                activation_origin=previous,
                activation_before_digest=expected,
            )
            state = self.program._write(
                wid, "ACTIVATION_REQUESTED", state, {"reason": reason}
            )
        current = get_job_settings(self.w, state["source_job_id"])
        if digest(current) == state["activation_before_digest"]:
            fields = {
                key: state["activation_target"][key]
                for key in TRIGGERS
                if state["activation_target"].get(key)
            }
            if fields:
                self.w.api_client.do(
                    "POST",
                    "/api/2.2/jobs/update",
                    body=dict(job_id=state["source_job_id"], new_settings=fields),
                )
            current = get_job_settings(self.w, state["source_job_id"])
        if without_triggers(current) != without_triggers(state["activation_target"]):
            raise ValueError("Configuration changed during activation")
        for key in TRIGGERS:
            if current.get(key) != state["activation_target"].get(key):
                raise ValueError("Trigger readback differs from reviewed activation")
        migrated = state["activation_origin"] == "MIGRATED_PAUSED"
        state.update(
            status="MIGRATED" if migrated else "ROLLED_BACK",
            **(
                {"migrated_digest": digest(current)}
                if migrated
                else {"restored_digest": digest(current)}
            ),
        )
        state["bundle_trigger_policy"] = (
            "Runtime trigger state must be reconciled before the next Bundle deployment; generated resources are unscheduled by default"
        )
        return self.program._write(wid, "ACTIVATION_VERIFIED", state)
