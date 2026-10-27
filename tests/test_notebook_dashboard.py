from smct.notebook_dashboard import render_dashboard


def test_persisted_dashboard_escapes_untrusted_names():
    payload = {
        "metadata": {"complete": True},
        "workloads": [
            {
                "workload_id": "notebook:a",
                "name": "<script>alert(1)</script>",
                "kind": "NOTEBOOK",
                "compute_mode": "unknown",
                "readiness": 100,
                "eligible": True,
                "status": "DISCOVERED",
                "attributes": {},
                "findings": [],
            }
        ],
    }
    html = render_dashboard(payload, [])
    assert "<script>" not in html and "&lt;script&gt;" in html
    assert "No accessible validation reports" in html
    assert html.count("<section id=") == 7


def test_synthetic_reports_cannot_be_displayed_as_live_classic():
    html = render_dashboard(
        {"metadata": {"complete": True}, "workloads": []},
        [
            {
                "passed": True,
                "baseline_source": "imported_synthetic",
                "classic_rows": 3,
                "serverless_rows": 3,
            }
        ],
    )
    assert "Imported synthetic baseline. No classic-compute run is claimed." in html
    assert "Cutover: NOT ESTABLISHED." in html


def test_dashboard_filter_and_partial_warning():
    payload = {
        "metadata": {"complete": False, "errors": ["permission <denied>"]},
        "workloads": [
            {"name": "NOTEBOOK_ONLY", "kind": "NOTEBOOK", "readiness": 100},
            {"name": "JOB_ONLY", "kind": "JOB", "readiness": 40},
        ],
    }
    html = render_dashboard(payload, [], object_type="JOB", minimum_score=50)
    assert "PARTIAL INVENTORY" in html and "permission &lt;denied&gt;" in html
    assert "Showing 0 of 2 objects" in html
    assert "NOTEBOOK_ONLY" not in html


def test_dashboard_safety_reports_supported_after_module_refresh():
    import importlib
    import smct.notebook_dashboard as module

    # Reproduce a function retained from the earlier notebook session.
    original = module.render_dashboard
    try:
        module.render_dashboard = lambda payload, reports: "old version"
        module = importlib.reload(module)
        html = module.render_dashboard(
            {"metadata": {"complete": True}, "workloads": []},
            [],
            safety_reports=[
                {
                    "source_job_id": 123,
                    "clone_job_id": 456,
                    "source_schedule_paused": True,
                    "original_unchanged": True,
                    "clone_has_triggers": False,
                    "backup_persisted": True,
                    "jobs_started_by_demo": 0,
                    "passed": True,
                }
            ],
        )
        assert "Live synthetic scheduling-safety evidence" in html
        assert "<td>123</td>" in html and "<td>456</td>" in html
    finally:
        module.render_dashboard = original


def test_programme_state_and_audit_are_shown_separately_from_fleet():
    state = {
        "program_id": "demo",
        "workload_id": "ready",
        "name": "Synthetic orders",
        "status": "VALIDATED",
        "readiness": 100,
        "wave": 1,
        "synthetic": True,
        "approval": {"scope": "authenticated_synthetic_fixture_owner"},
        "clone_job_id": 10,
        "run_id": 99,
        "validation": {"passed": True},
        "cutover_allowed": False,
    }
    event = {
        "program_id": "demo",
        "workload_id": "ready",
        "sequence": 1,
        "event_type": "VALIDATION_PASSED",
        "actor": "owner@example.invalid",
        "event_at": "2026-10-27",
        "state": state,
    }
    html = render_dashboard(
        {"metadata": {"complete": True}, "workloads": []},
        [],
        program_states=[state],
        program_events=[event],
    )
    assert "Connected migration programme" in html and "Programme audit trail" in html
    assert "authenticated_synthetic_fixture_owner" in html
    assert "VALIDATION_PASSED" in html and "production cutover" in html
