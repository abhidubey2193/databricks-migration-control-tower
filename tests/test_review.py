from smct.review import progress_history, next_action
from smct.notebook_dashboard import render_dashboard


def event(sequence, day, status, *, synthetic=False, wid="orders"):
    return dict(
        program_id="test",
        workload_id=wid,
        sequence=sequence,
        event_at=day + "T12:00:00Z",
        event_type="test",
        actor="admin",
        state=dict(status=status, synthetic=synthetic, workload_id=wid),
    )


def test_progress_reconstructs_latest_state_and_separates_synthetic_programmes():
    events = [
        event(1, "2026-10-26", "PLANNED"),
        event(2, "2026-10-26", "VALIDATED"),
        event(3, "2026-10-27", "MIGRATED_PAUSED"),
        event(1, "2026-10-26", "VALIDATED", synthetic=True, wid="fixture"),
    ]
    rows = progress_history(list(reversed(events)))
    assert rows[0] == dict(
        day="2026-10-26",
        tracked=1,
        blocked=0,
        validated=1,
        migrated_paused=0,
        migrated=0,
        synthetic=1,
    )
    assert rows[1]["validated"] == 0 and rows[1]["migrated_paused"] == 1
    assert rows[1]["migrated"] == 0
    assert "classic A/B" in next_action(dict(synthetic=True, status="VALIDATED"))


def test_plan_review_escapes_settings_and_filters_programme_inventory():
    state = dict(
        program_id="selected",
        workload_id="orders",
        status="PLANNED",
        name="Orders",
        candidate={
            "tasks": [{"task_key": "t", "environment_key": "e"}],
            "environments": [
                {"environment_key": "e", "spec": {"dependencies": ["pkg==1"]}}
            ],
            "description": "<script>danger</script>",
        },
    )
    hidden = dict(state, program_id="other", name="HIDDEN_PROGRAMME")
    payload = {
        "metadata": {"complete": True},
        "workloads": [
            dict(name="Orders", status="PLANNED"),
            dict(name="HIDDEN_FLEET", status="BLOCKED"),
        ],
    }
    html = render_dashboard(
        payload,
        [],
        program_states=[state, hidden],
        programme="selected",
        status="PLANNED",
        search="orders",
    )
    assert "HIDDEN_PROGRAMME" not in html and "HIDDEN_FLEET" not in html
    assert "Plan review" in html and "pkg==1" in html
    assert "<script>" not in html and "&lt;script&gt;" in html
    assert "request administrator approval" in html
