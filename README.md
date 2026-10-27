# Serverless Migration Control Tower

Assess a Databricks workspace, review migration plans, run approved validation waves, and keep the decisions and evidence together. The controller uses the Jobs API directly; it does not depend on the Beta Genie Code migration agent.

The main implementation is a set of workspace notebooks backed by Unity Catalog Delta. A read-only Streamlit App reads the same evidence. The older SQLite CLI remains available for local exploration.

## Start in a workspace

Clone this repo as a Git folder and select `enhanced-control-tower`. Use **Serverless notebook compute**, not a SQL warehouse. Run each notebook's first parameter cell before changing its widgets. Defaults are `catalog=auto` and `schema=serverless_migration`; old widget values survive Git pulls.

| Notebook | Use |
|---|---|
| `fleet_controller` | Discover and scan code; import a manifest; plan, approve and execute waves; collect billing; reconcile, cut over and roll back |
| `control_tower_dashboard` | Fleet readiness, joined programme status, validation, cost and audit history |
| `workspace_acceptance` | Generated-data acceptance: actual 1-task and 11-task serverless Jobs runs, distributed comparisons and negative controls |
| `classic_acceptance_setup` | In a classic-capable sandbox, create paused source fixtures and a manifest for genuine A/B acceptance |
| `remediation_workbench` | Retrieve/translate init scripts; propose Python/SQL edits and serverless JAR packaging |
| `program_demo` | Earlier connected example with a synthetic baseline |
| `clone_safety_demo` | Earlier paused-source/no-trigger-clone safety fixture |

For a serverless-only account, run `workspace_acceptance` with `action=assess`, review the plans, then choose `action=run`. Rerun the execution cell with the same ID until both workloads finish. Reference inputs and baselines are generated; these are not classic runs.

For real workload migration, start with [the manifest example](examples/fleet-manifest.json) and [the operating procedure](OPERATIONS.md). The controller does not execute arbitrary discovered jobs. Each selected notebook needs an explicit pinned-input/isolated-output contract and administrator approval.

## Deploy

Python 3.10+ and the official Databricks CLI are required locally.

```bash
pip install -e '.[app,dev]'
databricks auth login --profile SANDBOX
bash scripts/deploy.sh SANDBOX dev
```

The script validates and deploys the Bundle, runs the enhanced fleet scan and renders a dashboard snapshot. It requires authenticated workspace access, serverless entitlement and a writable catalog. Bundle resources include six jobs and the App definition. No job has a schedule. The fleet controller and acceptance jobs default to read-only assessment actions.

For the App, select **Unity Catalog snapshots**, provide a warehouse ID and use `catalog=auto` or an explicit catalog. Grant its principal warehouse CAN USE and SELECT on the evidence schema. The notebook dashboard needs no App deployment or SQL warehouse.

## Design

[Architecture](ARCHITECTURE.md) describes the state machine and failure handling. [Coverage](demo/LIMITATION_EXAMPLES.md) maps the implementation to L1–L13.

- Jobs 2.2 arrays are paginated explicitly. The direct compiler preserves DAGs up to 1,000 tasks; unsupported task/configuration combinations are rejected.
- Plans, notebook fingerprints, input versions and dependencies are bound before approval. Literal credential-like settings are rejected before durable backup.
- The fleet controller acquires a cooperative Delta lease. Each state revision and audit event is appended together. Restrict table write permissions to controller operators.
- Real A/B validation creates two unscheduled clones: one classic, one serverless. It never runs the source job. Outputs use separate tables and pinned input versions.
- Distributed `EXCEPT ALL` compares complete Delta snapshots with typed schemas, duplicate multiplicity, null counts and key diagnostics. Unsupported Spark set-operation types fail without recording PASS.
- Bulk execution is resumable, run submission uses idempotency tokens, and wave execution limits inflight workloads. A failed baseline cannot launch its target.
- Cutover changes the original job ID only after genuine A/B evidence, drift checks, backup verification and Bundle writeback. Cutover and rollback leave triggers paused; activation is separate.
- Billing nets original/retraction/restatement records and joins effective USD list prices. Missing prices, delayed usage and unattributed shared compute remain explicit; they are never treated as zero cost.

The generated Bundle resource is unscheduled. Bind it to the original job before deploying it, and reconcile the runtime trigger policy before subsequent deployments. The controller records that boundary rather than claiming a Git commit or deployment it has not performed.

## Tests and evidence

```bash
pip install -e '.[app,dev,spark]'
SPARK_LOCAL_IP=127.0.0.1 python -m pytest -q
smct demo --output demo-output
SMCT_DB=demo-output/smct.sqlite python -m streamlit run app/dashboard.py
```

Tests include the fleet lifecycle, 11-task execution through a simulated Jobs service, crash recovery, authenticated approval, source drift, Bundle compare-and-swap, cutover/activation/rollback, billing and real local Spark comparisons. See [verification](demo/VERIFICATION.md) for counts and the distinction between simulated, local Spark and workspace checks.

Operator-supplied workspace evidence currently shows 14 discovered objects, one validated connected serverless workload, passing and intentionally failing comparisons, a scheduling-safety fixture and a rendered programme dashboard. The subsequent workspace acceptance completed both the one-task and eleven-task serverless Jobs, with passing comparisons and failing negative controls (24 audit events). The general classic fleet path still needs suitable workspace acceptance.

## Boundaries

Free Edition has no classic compute. An Express trial starts with a serverless workspace too; genuine classic A/B needs an authorized classic-capable workspace. No classic execution or cost saving has been demonstrated in the current sandbox.

Automatic fleet A/B currently supports notebook tasks with the explicit I/O contract. Python/wheel jobs can be compiled; JAR jobs have a separate reviewed packaging path. Arbitrary shell scripts, Scala/R source conversion, mounts and unmanaged side effects are not guessed: record a keep-on-classic/deferred decision or remediate and reassess.

Local Bundle schema validation is not an authenticated clean-workspace deployment test. The code includes both deployment paths, but full Bundle/App deployment remains to be verified. The App and notebooks rely on workspace/Unity Catalog permissions; this project is not a separate enterprise identity provider.

Keep credentials, inventory exports, runtime databases and backups out of Git. All published examples use generated data.

[Recording outline](demo/DEMO_SCRIPT.md) · [Submission draft](demo/SUBMISSION_EMAIL.md)

## References

[Migration](https://docs.databricks.com/aws/en/compute/serverless/migration) · [Current serverless limits](https://docs.databricks.com/aws/en/compute/serverless/limitations) · [Jobs API](https://docs.databricks.com/api/jobs/v2/job) · [Bundle resources](https://docs.databricks.com/aws/en/dev-tools/bundles/resources) · [Billing](https://docs.databricks.com/aws/en/admin/system-tables/billing) · [Free Edition limits](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations)
