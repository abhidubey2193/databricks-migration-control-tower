# Submission draft

Complete the bracketed fields before sending.

To: nivethan.venkatachalam@diggibyte.com; anuj.sen@diggibyte.com; veerabadrachari.r@diggibyte.com; william.r@diggibyte.com
Subject: Serverless Challenge - Submission - [entrant name]

Serverless Migration Challenge - Submission

Name(s) and team: [confirm]
Repo: https://github.com/abhidubey2193/databricks-migration-control-tower/tree/enhanced-control-tower
Final commit SHA: [confirm final remote SHA]
Demo video: [link required]
Cloud tested: AWS personal trial sandbox; operator-run notebooks. No classic-compute execution.

Limitations addressed:
- L1: Paginated SDK fleet/code inventory, readiness/blockers, manifest waves, authenticated approval, Delta lease/journal, resumable bulk execution and joined dashboard.
- L2: Direct Jobs API DAG compilation through 1,000 tasks; queue/inflight control; 11-task lifecycle adapter tests and workspace acceptance harness.
- L3/L5: Bounded script retrieval, pinned dependency translation, explicit binary replacements and reviewed serverless JAR packaging.
- L4/L7/L11: Runtime/image/IAM/language/observability detection and explicit keep-classic/deferred decisions.
- L6/L13: Static detection plus reviewable Python AST and SQL parser proposals for mapped Delta views, streaming triggers and TRY_* operations.
- L8: Real classic/serverless validation-clone adapter, pinned inputs/outputs, distributed typed duplicate-aware comparison and negative controls. Current live proof uses a synthetic baseline; genuine classic execution requires another workspace.
- L9/L10: Fingerprints, atomic Bundle writeback/CAS, rollback resource restoration, all trigger forms removed from validation clones, original-ID paused cutover/rollback and separate activation. Bundle binding and trigger-policy reconciliation are explicit.
- L12: Correction-aware job/run/team/SKU DBUs and effective USD list prices, with pending/unpriced states and no invented savings.

Architecture:
SDK inventory and source scans feed a deterministic assessment engine and reviewed manifest.
A leased Delta journal binds decisions, approvals, source backups, notebook/config hashes and run evidence.
Separate unscheduled classic/serverless clones use pinned inputs and isolated outputs; distributed comparison decides PASS/FAIL.
Genuine evidence gates Bundle writeback and paused original-ID cutover/rollback; activation is separate.
Notebook and App dashboards share one persisted reader, including observed billing and provenance.

Databricks components: SDK, Jobs API, serverless notebooks/jobs, Unity Catalog Delta, optional billing system tables, Bundle configuration and an unverified App definition.

Deploy/reproduce:
Clone the repo as a Git folder and select enhanced-control-tower. On authorized Serverless compute, use catalog=auto and schema=serverless_migration. Use workspace_acceptance with action=assess, inspect its plans, then action=run; use fleet_controller plus a reviewed manifest for the general migration workflow. Rerun with the same program_id if a job is still executing. Run fleet_scan and control_tower_dashboard to inspect results. The earlier standalone demos remain available.

Tests:
135 local tests passed, including real local Spark comparisons; Jobs lifecycle tests use a simulated service. Official CLI Bundle schema check passed. Operator-supplied workspace outputs confirm 14-object inventory, rendered programme dashboard, one validated real serverless job with synthetic baseline, intentional FAIL and live scheduling-safety fixture. New general paths are not claimed as live-tested without their acceptance outputs.

Known gaps:
Current workspace has no classic compute, so genuine classic A/B and before/after savings remain unverified. Full authenticated Bundle/App deployment and the new acceptance notebook still need workspace checks. Arbitrary shell, language and mount semantics require explicit remediation/keep-classic review. Table ACLs provide the authorization boundary; the journal is not tamper-proof against admins. Generated resources require binding and runtime trigger-policy reconciliation before subsequent deployment.

Confirmation: no customer data, credentials or client workspace details are included: [confirm after checking the final recording].
