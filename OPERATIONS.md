# Operating procedure

Use an internal or personal sandbox. The scanning principal sees only objects it can access; a complete scan is relative to that visibility and the configured notebook cap. Review permission errors rather than treating them as an empty inventory.

## Workspace setup

Use the Git folder's enhanced branch or `bash scripts/deploy.sh SANDBOX dev`. The current user needs:

- Serverless compute access and permission to create/manage validation Jobs.
- USE CATALOG, USE SCHEMA, CREATE TABLE and read/write rights on the evidence schema.
- Read/export permission on notebooks being assessed.
- Access to the selected classic compute for genuine A/B.
- Optional SELECT on `system.billing.usage` and `system.billing.list_prices`.

The fleet runner verifies that SDK and SQL user identities agree. Approval and production lifecycle actions require the authenticated `admins` group. Synthetic acceptance also permits its authenticated fixture owner. Configure UC table ACLs so ordinary users cannot rewrite the audit or lease table.

The first controller run creates `program_events` and the single-row `controller_lease`. Do not manually insert rows into the lease table. A controller crash leaves a lease for up to 15 minutes; wait for expiry and rerun. Do not delete the row to force concurrent execution.

## Select workloads

Open `fleet_controller`, run its parameter cell, and use action `scan`. This exports notebook source for static assessment but stores findings rather than source in the fleet snapshot. Failed exports are visible as incomplete code coverage.

Copy `examples/fleet-manifest.json` into `manifest_json` and replace the job ID, input table/version and final output task. Choose a new `program_id` for a new reviewed configuration. The `sandbox_execution_reviewed` flag must be true only after checking that each notebook:

1. Reads the supplied `input_table` at `input_version`.
2. Writes only its supplied `output_table`, creating it once without replacement.
3. Has no external writes, notifications or unreviewed side effects.

Parameter names can be mapped. For a DAG where a task reads its parent's output, set `task_inputs` to a task-key-to-parent-key mapping. Only direct dependencies are accepted. Other tasks use the same pinned external input. The last task to compare is explicit; it is not inferred from task order.

`depends_on_workloads` can name other manifest workload IDs. They must validate before execution starts. Keep dependent workload waves in order. The direct task compiler checks missing keys, duplicate keys and cycles, preserving the original graph and run conditions.

## Plan and approve

Run `assess`, then `plan`. Planning resolves shared classic cluster configurations, retrieves init scripts and creates a dependency translation. Only complete pinned pip-install scripts can be translated automatically. Unsupported shell steps block planning. S3 reads use the caller's ambient AWS credentials; where unavailable, use an authorized Workspace/Volume copy. No script is executed.

PyPI packages and Volume wheels become serverless environment dependencies. Init-script packages apply only to tasks on the corresponding cluster. Task libraries stay scoped to their task. Equal package sets share an environment; different sets receive separate environments, so independent tasks can use different package versions. Conflicting pins within a single task stop planning. Egg/JAR/Maven replacements require an explicit `library_replacements` mapping; rebuilding a binary is an owner task. `remediation_workbench` supplies reviewed JAR packaging for current serverless support.

Open `control_tower_dashboard` (or the App with Unity Catalog snapshots) to inspect each plan’s task graph, environment packages, settings diff and suggested next action. Use `search`, `status` and `programme` to narrow review; programme selection applies to plans and history. Daily history counts programme workloads at their latest revision that day, with synthetic workloads separate.

Inspect source findings, both validation definitions, production proposal, notebook hashes, input version and output tables. Set `reason`, select a wave and run `approve`. A plan change cannot reuse an approval. Source or code changes require a fresh reviewed programme/workload ID.

Use `keep_classic` or `defer`, a workload ID and a reason for unresolved Scala/R, cluster, networking or semantic constraints. These are decisions, not completed migrations.

## Execute and validate

Run `execute` for the wave. Each invocation advances ready work and returns without waiting indefinitely. The default inflight limit is two validation workloads, and job queueing is enabled. Rerun with the same programme ID to continue.

The classic clone executes first with the original compute/libraries and isolated parameters. Its successful run and Delta output version are stored. Only then is the approved serverless clone created/run. The original job is never used as the validation runner.

A target write must create a fresh table at Delta version 0. Both clones must use create-once output semantics. The supplied `ab_target` notebook demonstrates this contract. Retries that would replace existing output fail rather than silently validating an overwritten table.

Validation pins both table versions, compares complete records using distributed Spark set operations, checks typed schemas and records null/duplicate/key diagnostics. Maps or other unsupported set-operation types need an explicit normalization design; Spark errors cannot become PASS. Ignored columns and key columns are part of the approved manifest. Keep ignores minimal.

Failed Jobs remain failed. Inspect the recorded job/run IDs and events; fix the cause and use a new reviewed ID instead of deleting evidence or overwriting outputs.

## Billing

After both runs complete, use action `cost`. Dates are UTC, start inclusive and end exclusive. Default is the preceding 30 days through tomorrow so today's runs are included.

The query restricts the workspace, nets billing corrections, attributes by exact job/run ID, and groups SKU/team/programme. Price joins include cloud, usage unit, effective interval and USD currency. Multiple/missing price matches make the price incomplete. Records spanning a price boundary remain unpriced rather than using an arbitrary rate.

A savings percentage requires matching output, positive DBUs and complete prices for both runs. It compares observed list cost of those runs, not an invoice or a projected monthly saving. Missing billing remains pending/unattributed. Classic shared-cluster usage may have no job-run attribution. Refresh later or report that limitation; do not allocate it by guesswork.

## Bundle reconciliation, cutover and rollback

These actions are for real executed classic/serverless evidence. Synthetic baseline comparisons cannot satisfy the gate.

1. Set `workload_id`, `repo_root` to the local/synced repository root and a reason. Run `reconcile`. It writes `resources/migrated_<id>.yml` atomically and records its digest. A changed existing file requires an explicit compare-and-swap hash via the Python API; the notebook never blindly overwrites it.
2. Review and commit the generated resource. Before using Bundle deployment to manage the existing job, bind that resource to the original job ID: `databricks bundle deployment bind migrated_<id> <source_job_id> -t dev --profile SANDBOX`. Confirm the target/profile and ownership first. A generated resource without binding would create a separate unscheduled job.
3. Run `cutover`. The gate verifies real successful bound runs, source/code/clone digests, backup integrity, validation and the current Bundle file. The source triggers are paused first, then all related Jobs must drain. The original job ID is reset to the serverless definition with its original triggers paused.
4. Inspect `MIGRATED_PAUSED`. Run `activate` only when ready. This restores the original trigger policy; an originally paused source stays paused. Both validation clones must still have no triggers.
5. To restore classic compute, run `rollback`. It verifies the saved backup and current job digest, restores the classic Bundle resource, resets the original job with triggers paused and records `ROLLED_BACK_PAUSED`. Activation is separate again.

Generated Bundle resources intentionally omit triggers. After runtime activation, the journal flags that the trigger policy must be reconciled before the next Bundle deployment. This prevents claiming repository parity while runtime triggers differ. Source resources owned by another Bundle must be reconciled in that owning repository, not deployed as an unrelated resource.

Requests are journaled before REST mutations. If a reset succeeds but the final event fails, rerunning checks live readback and records the completed transition. Unexpected drift stops recovery; it is not overwritten. Notebook code edits also stop cutover, activation and rollback because restoring settings cannot restore externally edited code.

## Acceptance and platform limits

`workspace_acceptance` works on serverless-only accounts. It runs one- and eleven-task jobs, uses full distributed comparisons, exercises dependency/SQL/Python proposal helpers and requires negative controls to fail. Run `assess`, review, then `run`; rerun until terminal. It explicitly labels references as generated, not classic execution.

For genuine A/B, use `classic_acceptance_setup` in an authorized classic-capable workspace. Give an existing compatible cluster ID, inspect its dry run, then set `apply=true`. It creates only paused fixtures and generated inputs. Copy the resulting manifest into the controller and follow the plan/approval/execution steps. Cutover/rollback can then be exercised safely on those paused fixtures.

Free Edition has no classic compute. AWS Express trials start with serverless workspaces; classic workspace provisioning requires separate account/cloud setup and can incur AWS charges. No setup step in this repo upgrades an account or adds a payment method.
