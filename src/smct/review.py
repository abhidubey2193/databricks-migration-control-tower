"""Read-only operator summaries. Decisions remain in the authenticated controller."""

import difflib
import json

NEXT_ACTION = {
    "READY": "Review isolation contract and create a plan",
    "BLOCKED": "Review findings; remediate or record a deferred/classic disposition",
    "PLANNED": "Review plan, then request administrator approval",
    "APPROVED": "Execute approved validation wave",
    "CLONE_REQUESTED": "Resume controller to recover clone creation",
    "CLONED": "Resume controller to submit validation run",
    "EXECUTION_REQUESTED": "Resume controller to recover run submission",
    "EXECUTING": "Wait for Jobs, then resume controller",
    "EXECUTED": "Resume controller to compare pinned outputs",
    "EXECUTION_FAILED": "Inspect failed run; do not promote",
    "VALIDATION_FAILED": "Inspect output differences; do not promote",
    "VALIDATED": "Review genuine A/B evidence and reconcile Bundle before cutover",
    "MIGRATED_PAUSED": "Review trigger policy before explicit activation",
    "MIGRATED": "Monitor runs and attributed billing",
    "ROLLED_BACK_PAUSED": "Review restored classic settings before explicit activation",
}


def next_action(state):
    if state.get("synthetic") and state.get("status") == "VALIDATED":
        return "Synthetic acceptance complete; genuine classic A/B is still required for cutover"
    return NEXT_ACTION.get(
        state.get("status"), "Inspect workload state in the controller"
    )


def plan_review(state):
    """Display production changes when present; otherwise show the validation clone."""
    candidate = state.get("production_candidate") or state.get("candidate")
    if not candidate:
        return None

    def lines(value):
        return (json.dumps(value, indent=2, sort_keys=True) + "\n").splitlines(
            keepends=True
        )

    patch = "".join(
        difflib.unified_diff(
            lines(state.get("source_settings", {})),
            lines(candidate),
            fromfile="source settings",
            tofile="proposed settings",
        )
    )
    environments = {
        e["environment_key"]: e.get("spec", {})
        for e in candidate.get("environments", [])
    }
    tasks = [
        {
            "task": t["task_key"],
            "depends_on": [d["task_key"] for d in t.get("depends_on", [])],
            "environment": t.get("environment_key", "classic / external"),
            "dependencies": environments.get(t.get("environment_key"), {}).get(
                "dependencies", []
            ),
        }
        for t in candidate.get("tasks", [])
    ]
    return dict(patch=patch, tasks=tasks, next_action=next_action(state))


def progress_history(events):
    """End-of-day state counts, not event counts. Demo programmes stay separate."""
    latest, rows = {}, []
    days = {}
    for event in events:
        days.setdefault(str(event["event_at"])[:10], []).append(event)
    for day, daily in sorted(days.items()):
        for e in sorted(daily, key=lambda e: (str(e["event_at"]), int(e["sequence"]))):
            key = (e["program_id"], e["workload_id"])
            if int(e["sequence"]) > latest.get(key, {}).get("sequence", -1):
                latest[key] = dict(e["state"], sequence=int(e["sequence"]))
        real = [s for s in latest.values() if not s.get("synthetic", False)]
        rows.append(
            dict(
                day=day,
                tracked=len(real),
                blocked=sum(s.get("status") == "BLOCKED" for s in real),
                validated=sum(s.get("status") == "VALIDATED" for s in real),
                migrated_paused=sum(s.get("status") == "MIGRATED_PAUSED" for s in real),
                migrated=sum(s.get("status") == "MIGRATED" for s in real),
                synthetic=sum(bool(s.get("synthetic")) for s in latest.values()),
            )
        )
    return rows
