# Serverless Migration Control Tower (SMCT)

**Databricks Classic → Serverless Migration Challenge implementation, 26 October 2026.**

This project implements SDK-based discovery, blocker/readiness assessment, durable local approvals and status history, a guarded **no-trigger** job clone for simple workloads, a bounded classic/serverless output-table comparator, a Databricks Bundle inventory-snapshot job and a Streamlit dashboard.

**This is not a one-click fleet migration tool.** The Beta Genie Code migration agent is not automated. Sandbox deployment and end-to-end Databricks execution have not been verified.

## Quick start

Requires Python 3.10+, Databricks SDK/CLI authentication and access to an **internal sandbox workspace only**.

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e '.[app,dev]'
pytest -q
databricks auth login --profile SANDBOX
export DATABRICKS_CONFIG_PROFILE=SANDBOX
smct --db smct.sqlite scan --output inventory.json
smct --db smct.sqlite list
streamlit run app/dashboard.py
```

The scanner can discover jobs, clusters and notebook paths. Code analysis is opt-in; notebook source is never stored in inventory. Workspace discovery may be truncated by the configured notebook cap or privileges.

## Deployment using Databricks Asset Bundles

```bash
databricks bundle validate -t dev --profile SANDBOX
databricks bundle deploy -t dev --profile SANDBOX
databricks bundle run smct_fleet_scan -t dev --profile SANDBOX
```

Requires serverless-job availability and permissions to create the chosen Unity Catalog schema/table. The Bundle provisions a snapshot job, **not the full Streamlit App or local SQLite control plane**. The bundled notebook writes classic-job and cluster snapshots into `fleet_snapshots`.

## Sandbox-only migration workflow

```bash
smct scan --output inventory.json
smct approve 123 --actor admin@example.invalid --reason 'Sandbox authorized'
smct clone 123                     # dry-run, no mutation
smct clone 123 --apply             # creates no-trigger clone and local backup
```

Only supported 1–10 task notebook/Python/wheel jobs are candidates. Git-backed jobs, task libraries, unsupported constructs and other ambiguous configurations are rejected. **Clone creation is not proof of compatibility.** The original job is never modified. Clone creation is a real workspace mutation and requires explicit recorded approval and `--apply`.

Run classic and serverless workloads **manually** with equivalent snapshots and completely isolated output tables. When both tables are static:

```bash
smct validate 123 \
  --http-path '/sql/1.0/warehouses/<SANDBOX_WAREHOUSE_ID>' \
  --classic-table sandbox.smct.classic_output \
  --serverless-table sandbox.smct.serverless_output
smct gate 123
smct audit
```

The comparator checks the full contents up to its configured row cap; it fails on over-limit data rather than reporting a sampled PASS. Its PASS does **not** prove type precision, business semantics, nondeterministic output, side-effect equivalence or production safety. The gate is read-only and does not promote, schedule or cut over jobs.

### Safety

- Never execute against customer workspaces; never publish customer data, hostnames or secrets.
- The CLI uses local SQLite. Approver identity is entered manually and is **not** authenticated enterprise-admin approval.
- Backups can contain sensitive job settings and are stored locally with restricted permissions. Never commit backups.
- Never run original and validation clone against the same production output paths.
- Clone schedule, trigger and continuous configuration are removed by construction; never enable two schedules concurrently.
- Source digest and backup existence are reviewed before cutover, but actual promotion remains manual.

## Challenge coverage

| ID | Implemented | Known gap |
|---|---|---|
| L1 | SDK job/cluster inventory; notebook-path scanning; readiness, dashboard, local approvals and audit; Delta snapshots | Not exhaustive workspace coverage; no bulk migration |
| L2 | Detects jobs above ten tasks | No task splitting |
| L3 | Detects init scripts | No translation |
| L4 | Detects old DBR, ML Runtime, custom images and AWS profiles | No automatic fix |
| L5 | Flags dependency libraries and Spark JAR tasks | No conversion |
| L6 | Optional code scan for selected blockers | No code rewrites |
| L7 | Detects Scala/R metadata | No rewrite |
| L8 | Real bounded output-table comparator | No automated A/B workload execution |
| L9 | Refuses Git jobs; source-digest drift check | No Git/Bundle writeback |
| L10 | No-trigger clone + cutover safety gate | No atomic promotion |
| L11 | Detects log delivery | No full alternative mapping |
| L12 | Billing SQL example | No integrated before/after cost dashboard |
| L13 | Limited ANSI and streaming static rules | No semantic repair |

## Project layout

- `src/smct/` — discovery, scoring, persistence, orchestration, A/B comparison, governance
- `app/dashboard.py` — Streamlit dashboard
- `resources/`, `notebooks/`, `databricks.yml` — Databricks Bundle inventory job
- `sql/control_tables.sql` — Unity Catalog control-table definitions
- `tests/` — mocked and unit tests; `demo/` — synthetic inventory and demo script

Run `pytest -q` for local validation. Mocked test success is not a Databricks sandbox deployment test, and no demo video is included.

## References

- [Databricks Bundle resource configuration](https://docs.databricks.com/aws/en/dev-tools/bundles/resources)
- [Databricks Jobs API](https://docs.databricks.com/api/jobs/v2/create-job)
- [Serverless environment dependencies](https://docs.databricks.com/aws/en/compute/serverless/dependencies)
- [Databricks billing tables](https://docs.databricks.com/aws/en/admin/system-tables/billing)
