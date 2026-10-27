"""One persisted evidence reader shared by notebook and App dashboards."""

import json
from .ab import safe_table


def load_evidence(query, catalog, schema):
    prefix = safe_table(f"{catalog}.{schema}.fleet_snapshots").rsplit(".", 1)[0]

    def exists(name):
        rows = query(f"SHOW TABLES IN {prefix} LIKE '{name}'")
        return bool(rows)

    rows = query(
        f"SELECT scanned_at,payload_json FROM {prefix}.`fleet_snapshots` ORDER BY scanned_at DESC LIMIT 1"
    )
    if not rows:
        raise ValueError("Run fleet_scan first")
    payload = json.loads(rows[0]["payload_json"])
    reports = []
    if exists("validation_demo_evidence"):
        reports = [
            json.loads(r["report_json"])
            for r in query(
                f"SELECT report_json FROM {prefix}.`validation_demo_evidence` ORDER BY validated_at DESC LIMIT 20"
            )
        ]
    safety = []
    if exists("clone_safety_demo_evidence"):
        safety = [
            json.loads(r["report_json"])
            for r in query(
                f"SELECT report_json FROM {prefix}.`clone_safety_demo_evidence` ORDER BY checked_at DESC LIMIT 1"
            )
        ]
    states, events, seen, revisions = {}, [], set(), {}
    if exists("program_events"):
        history = query(
            f"SELECT * FROM {prefix}.`program_events` ORDER BY event_at DESC LIMIT 5001"
        )
        if len(history) > 5000:
            raise ValueError(
                "Dashboard audit cap exceeded; archive history before rendering"
            )
        for r in sorted(
            history,
            key=lambda r: (r["program_id"], r["workload_id"], int(r["sequence"])),
        ):
            key = (r["program_id"], r["workload_id"])
            revision = (*key, int(r["sequence"]))
            if revision in seen:
                raise ValueError("Duplicate programme journal revision")
            seen.add(revision)
            if int(r["sequence"]) != revisions.get(key, 0) + 1:
                raise ValueError("Programme audit revision gap")
            revisions[key] = int(r["sequence"])
            state = json.loads(r["state_json"])
            if state.get("workload_id") != r["workload_id"] or state.get(
                "sequence"
            ) != int(r["sequence"]):
                raise ValueError("Event/state identity mismatch")
            event = dict(
                program_id=r["program_id"],
                workload_id=r["workload_id"],
                sequence=int(r["sequence"]),
                event_type=r["event_type"],
                actor=r["actor"],
                event_at=r["event_at"],
                state=state,
            )
            events.append(event)
            if key not in states or state["sequence"] > states[key]["sequence"]:
                states[key] = dict(
                    state, program_id=r["program_id"], journal_event_at=r["event_at"]
                )
    for state in states.values():
        for field in ("validation", "negative_control"):
            if state.get(field):
                reports.append(dict(state[field], workload=state["name"]))
    # Join genuine source job IDs back to the fleet; retain all programme history separately.
    live = {
        str(s["source_job_id"]): s
        for s in sorted(states.values(), key=lambda s: s.get("journal_event_at", ""))
        if s.get("source_job_id")
    }
    for workload in payload.get("workloads", []):
        state = live.get(str(workload.get("attributes", {}).get("job_id")))
        if state:
            workload.update(
                status=state["status"],
                programme=state["program_id"],
                wave=state.get("wave"),
                billing=state.get("billing"),
            )
    return dict(
        payload=payload,
        reports=reports,
        safety_reports=safety,
        program_states=list(states.values()),
        program_events=sorted(events, key=lambda e: (e["event_at"], e["sequence"])),
        scanned_at=rows[0]["scanned_at"],
    )
