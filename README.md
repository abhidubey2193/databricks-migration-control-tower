# Serverless Migration Control Tower

A small control plane for assessing Databricks workloads and keeping migration decisions, job references and validation results together.

The project has two ways to use it: Databricks notebooks backed by Delta, and a local CLI backed by SQLite. The notebook dashboard is the route tested in the sandbox so far. The Streamlit app and Bundle deployment still need a full deployment check.

## Run it in Databricks

Create a Git folder from this repo and select `enhanced-control-tower`. Use an internal or personal sandbox. The notebooks default to the current catalog (`catalog=auto`) and write to `serverless_migration`.

For the connected demo:

1. Open `notebooks/program_demo` and select Serverless.
2. Set `action=assess` and run it to inspect three examples: a ready workload, an image/init-script blocker and an 11-task chain.
3. Set `action=run` to approve the ready example, save its backup, create an unscheduled job and execute four generated rows. Approval records the SQL/SDK identity. Non-admin self-approval is limited to the operator's synthetic fixture.
4. If it reports `EXECUTING`, rerun with the same `program_id`. Run submission uses an idempotency token.
5. Run `notebooks/control_tower_dashboard` to see the workload states and audit trail.

Use one runner per `program_id`. The audit table detects conflicting revisions; it is not a distributed lock. A failed run stays failed. Inspect its run ID before starting a new programme.

The source definitions and baseline are synthetic. The target is a real serverless Jobs run. A successful comparison records `VALIDATED`, never `MIGRATED`, and does not enable a schedule.

The earlier, separately tested notebooks are still useful:

| Notebook | Purpose |
|---|---|
| `fleet_scan` | Save the workspace inventory to Delta |
| `validation_demo` | Compare a generated transformation with a synthetic baseline; show PASS and deliberate FAIL |
| `clone_safety_demo` | With `apply=true`, create a paused source fixture and a no-trigger clone; start neither |
| `control_tower_dashboard` | Read the saved inventory, comparisons and programme history |

The dashboard notebook reloads its renderer after Git updates. Existing widget values survive pulls; change an old `catalog=main` value to `auto`.

## Local setup

Python 3.10+ is required. Tests run on Python 3.12.

```bash
pip install -e '.[app,dev]'
python -m pytest -q
smct demo --output demo-output
SMCT_DB=demo-output/smct.sqlite python -m streamlit run app/dashboard.py
```

Use a fresh output directory for each local demo. It uses a simulated SDK and needs no workspace credentials.

For a real read-only inventory:

```bash
databricks auth login --profile SANDBOX
export DATABRICKS_CONFIG_PROFILE=SANDBOX
smct scan --output inventory.json
smct waves --size 5
```

The CLI also provides `approve`, `clone`, `validate`, `gate` and `audit`; use `smct <command> --help`. Clone creation requires a recorded approval and `--apply`. The CLI never modifies the original job or promotes a clone.

## Design

[Architecture and failure handling](ARCHITECTURE.md) describes the component boundaries, state model and production tradeoffs.

- Discovery and assessment share one Python engine across the CLI and notebooks. Inventory records missing permissions and notebook caps.
- The connected programme appends state revisions to a single Delta table, `program_events`. Each event carries the programme/workload ID, actor, status and evidence snapshot. The older CLI keeps its state in SQLite.
- Plans are fingerprinted before approval. The approved source definition is backed up before clone creation. Clone readback must match the plan and have no triggers.
- Runs use the stored clone ID and an idempotency token. Validation checks a successful run, unchanged configurations and pinned Delta table versions.
- Comparisons preserve duplicate multiplicity and check typed schemas, counts, nulls and keyed differences. A negative control deliberately alters the output and must fail.
- The 11-task example uses a direct Jobs API plan that retains the original dependency graph. Our guarded converter caps this path at 100 tasks. The example is planned, not executed by the demo.
- Billing reads, when permitted, show usage for the target job. They do not establish before/after savings.

## Bundle

```bash
databricks bundle validate -t dev --profile SANDBOX
databricks bundle deploy -t dev --profile SANDBOX
databricks bundle run smct_fleet_scan -t dev --profile SANDBOX
```

The Bundle defines the inventory/validation jobs, an assessment-only programme job and the Streamlit app. `smct_program_demo` defaults to assessment; run the interactive notebook for the full demo. App warehouse/Unity Catalog permissions are not granted automatically.

Full Bundle deployment is unverified. Its notebooks import the synced `src` directory relative to their working directory.

## What has actually been checked

The earlier notebooks produced sandbox outputs for inventory (9 objects, no warnings), a rendered dashboard, PASS/FAIL comparison and a live trigger-safety fixture. The fixture used an already-serverless source and started no jobs. Those results are supplied by the operator and saved under `demo/workspace-*-evidence.json`.

The operator-run connected programme reached VALIDATED with a real serverless job, a passing comparison and an intentionally failing negative control. The source definitions and baseline remain synthetic. See [verification notes](demo/VERIFICATION.md) for the current test count and boundaries.

## Limits

No genuine classic-compute execution has been demonstrated. There is no automatic cutover, production rollback, enterprise approval service or verified cost saving. The notebook programme stores authenticated approvals for the sandbox; it does not replace platform access controls.

Detection covers selected runtime, library, storage and code risks. Most findings require manual remediation. The direct large-job converter handles supported notebook/Python/wheel tasks; unsupported settings fail closed. The project does not invoke the Beta Genie Code migration agent.

Keep credentials, scans, SQLite databases and backups out of Git. The examples use generated data. Repo privacy and judge access are owner decisions.

[Limitation examples](demo/LIMITATION_EXAMPLES.md) · [Recording outline](demo/DEMO_SCRIPT.md) · [Submission draft](demo/SUBMISSION_EMAIL.md)

## References

[Jobs API](https://docs.databricks.com/api/jobs/v2/job) · [Large jobs](https://docs.databricks.com/aws/en/jobs/large-jobs) · [Bundle resources](https://docs.databricks.com/aws/en/dev-tools/bundles/resources) · [Billing tables](https://docs.databricks.com/aws/en/admin/system-tables/billing)
