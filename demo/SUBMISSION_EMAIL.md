# Submission draft

Complete the bracketed fields and update the connected-run result before sending.

To: nivethan.venkatachalam@diggibyte.com; anuj.sen@diggibyte.com; veerabadrachari.r@diggibyte.com; william.r@diggibyte.com
Subject: Serverless Challenge - Submission - [entrant name]

Serverless Migration Challenge - Submission

Name(s) and team: [confirm]
Repo: https://github.com/abhidubey2193/databricks-migration-control-tower/tree/enhanced-control-tower
Final commit SHA: [confirm final remote SHA]
Demo video: [link required]
Cloud tested: AWS personal trial sandbox; operator-run notebooks. No classic-compute execution.

Limitations addressed:
- L1: SDK inventory/readiness and a notebook dashboard. The new connected programme stores decisions, approvals, backups, clone/run references and validation in Delta. Its workspace execution is pending.
- L2: Generates an 11-task serverless candidate through the direct Jobs API path, preserving the dependency graph. The large candidate is planned, not executed.
- L8: Typed, duplicate-aware output comparison with null/count/key diagnostics. The standalone notebook demonstrated PASS and intentional FAIL. The new connected target-job route adds pinned Delta snapshots and run binding; its workspace execution is pending. Baseline data is synthetic.
- L10: Live synthetic fixture produced a paused unchanged source, no-trigger clone and persisted backup, with zero jobs started. Source was already serverless.
- L9: Configuration/code fingerprints and drift checks; no Git/Bundle writeback.
- L3/L4/L5/L6/L7/L11/L13: Selected static/metadata detection with remediation guidance. No general automatic code or dependency conversion.
- L12: Target-job usage query where permitted; no before/after savings claim.

Architecture:
The SDK inventories workloads and passes them through a shared assessment engine.
A Delta event history joins programme decisions and execution/validation evidence by workload.
Jobs run only explicitly approved synthetic fixtures into isolated output tables.
The notebook dashboard reads saved state; a separate CLI/Streamlit route uses SQLite.
Promotion remains manual and synthetic evidence cannot authorize cutover.

Databricks components: SDK, Jobs API, serverless notebooks/jobs, Unity Catalog Delta, optional billing system tables, Bundle configuration and an unverified App definition.

Deploy/reproduce:
Clone the repo as a Git folder and select enhanced-control-tower. On authorized Serverless compute, use catalog=auto and schema=serverless_migration. Run program_demo with action=assess, inspect its plan, then action=run. Rerun with the same program_id if a job is still executing. Run fleet_scan and control_tower_dashboard to inspect results. The earlier standalone demos remain available.

Tests:
87 local tests pass. Earlier workspace outputs cover a complete 9-object scan, rendered dashboard, matching/mismatched comparison and live scheduling-safety fixture. The new connected route requires a workspace run before claiming it as tested.

Known gaps:
No genuine classic execution, automatic production cutover/rollback, enterprise approval service, verified savings or completed Bundle/App deployment test. The programme runner is single-writer. The required video and final access checks must be completed before sending.

Confirmation: no customer data, credentials or client workspace details are included: [confirm after checking the final recording].
