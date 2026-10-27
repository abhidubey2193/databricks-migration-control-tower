# Serverless Migration Control Tower

Databricks classic-to-serverless challenge implementation. SDK discovery, shared readiness assessment, durable local approvals/audit, deterministic migration waves, guarded unscheduled job clones, duplicate-aware validation and a seven-section Streamlit dashboard.

**Current status: local MVP tested; live workspace deployment and video outstanding.** No genuine classic execution, cloud deployment or savings are claimed. See [verification record](demo/VERIFICATION.md), [limitation examples](demo/LIMITATION_EXAMPLES.md) and [submission draft](demo/SUBMISSION_EMAIL.md).

## Reproduce the working local MVP

Python 3.10+ is required. Tests were executed with Python 3.12.

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e '.[app,dev]'
python -m pytest -q
python -m smct.cli demo --output demo-output
SMCT_DB=demo-output/smct.sqlite python -m streamlit run app/dashboard.py
```

The demo requires no credentials and never contacts Databricks. It exercises the actual assessment, store, clone builder, comparator and advisory gate through an explicitly simulated SDK client. Use a fresh output directory for each run; existing state is never overwritten.

Outputs include four job definitions, isolated baseline/PASS/FAIL JSON, an unscheduled clone, backup, SQLite audit/status records and `evidence.json`. Expected results are committed in [demo/expected-results.json](demo/expected-results.json): ready job 100, blocked image/script job 47, 11-task job 65, scheduled job 82; PASS true, FAIL false, original unchanged, clone triggers absent, cutover blocked.

Scores reflect retrieved metadata, not full runtime compatibility. The synthetic baseline is never an executed classic run. The static L13 scanner provides review suggestions, not semantic repair.

## Architecture

1. `discovery.py` reads SDK paginated jobs, clusters and workspace notebook paths; maps job notebook references and propagates shared-cluster findings. Errors mark inventory incomplete.
2. `assessment.py` produces explainable findings and scores; unsupported task types fail closed. `static_scan.py` provides opt-in source export/scanning without persisting notebook content.
3. `Store` persists workloads, manually entered approvals, clone references, validation and audit history in SQLite. `smct waves` records a proposed plan; cross-job dependencies require review.
4. `orchestration.py` reassesses fresh job and referenced-cluster settings, writes a private backup and creates only a no-trigger serverless candidate. Production promotion remains manual.
5. `validation.py` compares exact record multisets, typed schema, null counts and optional key groups. SQL mode reads separate bounded tables and refuses over-limit data.
6. Bundle notebooks share the same Python engine, append inventory and integration-demo evidence to Unity Catalog Delta. The dashboard reads local SQLite, JSON or UC snapshots through a warehouse.

## Authorized sandbox discovery and cloning

Use an internal sandbox only. Never use customer workspaces or data.

```bash
databricks auth login --profile SANDBOX
export DATABRICKS_CONFIG_PROFILE=SANDBOX
smct --db smct.sqlite scan --output inventory.json
smct --db smct.sqlite list
smct --db smct.sqlite waves --size 5
smct --db smct.sqlite approve 123 --actor owner@example.invalid --reason 'Isolated sandbox reviewed'
smct --db smct.sqlite clone 123
smct --db smct.sqlite clone 123 --apply
smct --db smct.sqlite audit
```

Only 1–10 task notebook/Python/wheel jobs without unresolved configuration are accepted. Git sources, libraries needing translation, init scripts, unsupported task fields, existing environments and other ambiguous configurations are rejected. Shared-cluster configuration is fetched afresh. The source job is never modified or executed by clone creation. The clone can still have business side effects if manually run; first review all input/output paths and parameters.

Notebook source scanning is available through the Python `scan_notebook` function; the default CLI inventory scans metadata only. Notebook-to-cluster attachment and owner identity are not comprehensively inferred.

### Exact output validation

Run variants manually with identical immutable inputs and distinct isolated output tables, then:

```bash
smct --db smct.sqlite validate 123 \
  --http-path '/sql/1.0/warehouses/<SANDBOX_WAREHOUSE_ID>' \
  --classic-table sandbox.smct.classic_output \
  --serverless-table sandbox.smct.serverless_output
smct --db smct.sqlite gate 123
```

The comparator preserves duplicates and value types, includes SQL metadata for empty-table schema checks and rejects unsupported/non-finite values. Exact zero-difference thresholds apply. The CLI compares full contents up to the row cap, not a random sample. It does not freeze tables or bind Delta versions: keep snapshots immutable. Business semantics, side effects, numerical tolerance and nondeterministic outputs need additional checks.

An arbitrary or synthetic PASS cannot satisfy the cutover gate. It additionally requires configuration-bound execution references, typed schema evidence, successful SDK run lookups matching both job IDs, source digest and backup integrity. The current CLI does not assemble that complete execution-provenance record; the gate therefore remains blocked for its unbound comparisons. There is no automatic promotion, schedule activation or rollback mutation. Backups support manual restoration after review.

## Deploy the Bundle in Free Edition or another authorized sandbox

Install the official Databricks CLI. Set an authenticated SANDBOX profile, choose an existing Unity Catalog catalog (or use `catalog=auto` for the current catalog) and grant CREATE SCHEMA/table permissions. Serverless Jobs, Databricks Apps and warehouse access depend on workspace entitlements.

```bash
databricks bundle validate -t dev --profile SANDBOX \
  --var catalog=auto --var schema=serverless_migration
databricks bundle deploy -t dev --profile SANDBOX \
  --var catalog=auto --var schema=serverless_migration
databricks bundle run smct_fleet_scan -t dev --profile SANDBOX
databricks bundle run smct_validation_demo -t dev --profile SANDBOX
databricks bundle run smct_dashboard -t dev --profile SANDBOX
```

Notebook catalog widgets now default to `auto`, which resolves `SELECT current_catalog()` and checks `SHOW CATALOGS` before any writes. Existing widget values survive Git updates: if an earlier run created `catalog=main`, change that widget to `auto` before rerunning. Explicit unavailable catalogs fail with the accessible catalog names; no catalog is created automatically. CREATE SCHEMA/table permissions are still required.

The Bundle defines two **unscheduled** notebook jobs and a Streamlit App. Source modules are synced alongside notebooks, which import `../src` relative to the notebook working directory. The app installs the package through root `requirements.txt`; root `app.yaml` is its entry point.

- Fleet scan appends timestamped inventory JSON to `<catalog>.<schema>.fleet_snapshots`, including serverless/other jobs and incomplete-inventory warnings. Cluster inventory failure is recorded rather than aborting the whole snapshot.
- Validation demo executes a Spark transformation of generated sample data, compares against an **imported synthetic baseline**, writes three distinct append-only output tables and PASS/FAIL reports in `validation_demo_evidence`. It does not execute classic compute. Both assertions must succeed.
- In the App select **Unity Catalog snapshots**, enter catalog/schema and a SQL warehouse ID. Grant its service principal CAN USE on that warehouse plus USE CATALOG, USE SCHEMA and SELECT on the evidence tables. These permissions are not granted automatically. Optional defaults are `SMCT_CATALOG`, `SMCT_SCHEMA`, `SMCT_WAREHOUSE_ID`.
- Local SQLite is not durable storage for a hosted App. Use UC mode to view persistent inventory and validation reports. Full approvals/migrations/audit remain in the local control plane and are not replicated to Delta.

Live Bundle validation/deployment has not completed in this session. `bundle validate` currently stops at missing workspace credentials. A source/schema check alone does not prove deployability. If Apps are unavailable, use the same dashboard locally against persisted UC data with authorized authentication.

## Coverage and limits

| ID | Working implementation | Boundary |
|---|---|---|
| L1 | SDK inventory, scores/findings, local status/approvals/audit, notebook/shared-cluster mapping, wave plans, dashboard | Privilege/cap limited; manual approver identity; no bulk execution or cross-job dependency inference |
| L2 | Detects >10 tasks and blocks automatic clone | No task splitting |
| L3 | Detects init scripts, refuses unsafe stripping | No retrieval/translation |
| L4 | Old DBR, ML Runtime, images, AWS profiles, configs/env variables detected | No automatic rewrite |
| L5 | Egg/JAR/Maven/task-library detection and guarded rejection | No library conversion |
| L6/L7 | Opt-in code/language detection | No automatic rewrites |
| L8 | Exact duplicate-aware typed comparator, null/count/key-group diagnostics, persisted synthetic PASS/FAIL | No verified classic run; Spark integration notebook unverified |
| L9 | Source/backup digest checks; rejects Git-backed automatic cloning | No Git/Bundle patch generation |
| L10 | No-trigger clone, approval requirement, fresh reassessment, backup and advisory gate | Live API creation not yet verified; no atomic promotion |
| L11 | Cluster log and SSH-key findings | No alternative log delivery setup |
| L12 | Billing SQL and explicit unavailable-data indicator | No actual before/after DBU or savings |
| L13 | Heuristic ANSI/streaming findings and suggestions | May flag comments/string literals; no parser or semantic repair |

Do not automate the Beta `/compute migrate to serverless` agent through an assumed API. This project uses Jobs APIs directly for its narrow clone path; agent use is manual.

## Safety and submission

- Keep backups, SQLite, generated scans and credentials out of Git. Backup files are restricted locally; review them for sensitive settings.
- Synthetic data is labeled. Tests using mock clients are not live integration tests.
- Repository visibility was public when checked on 27 October 2026. Privacy and judge Admin access require owner approval; neither was changed.
- [Recording script](demo/DEMO_SCRIPT.md) is ready. An actual video under five minutes is outstanding.
- [Submission email](demo/SUBMISSION_EMAIL.md) is a draft; final name, remote SHA, live cloud and video must be confirmed. Nothing is sent automatically.
- Revised deadline supplied in the handover: **27 October 2026, 23:59 IST / 19:29 Stockholm**.

## Official references

- [Jobs API and environments](https://docs.databricks.com/api/jobs/v2/job)
- [Bundle resources](https://docs.databricks.com/aws/en/dev-tools/bundles/resources)
- [Deploy Databricks Apps with Bundles](https://docs.databricks.com/aws/en/dev-tools/bundles/apps-tutorial)
- [Billing system tables](https://docs.databricks.com/aws/en/admin/system-tables/billing)
