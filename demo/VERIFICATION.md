# Verification

27 October 2026. Python 3.12, Databricks SDK 0.157.0, Streamlit 1.66.1 and CLI 1.22.1.

## Local checks

`python -m pytest -q`: **136 passed**.

Coverage includes the general fleet lifecycle, two-sided 11-task execution through a simulated Jobs service, classic failure isolation, approval identity, code/config/contract drift, run recovery, Bundle CAS, paused cutover/activation/rollback, lease exclusion, credential rejection, dependency/AST/SQL proposals, billing attribution, shared dashboard reading and real local Spark comparison. Python compilation and `git diff --check` also pass. GitHub Actions passed for the full fleet implementation, commit `0c9408a` ([run](https://github.com/abhidubey2193/databricks-migration-control-tower/actions/runs/40558179493)). A subsequent comparison regression covers literal dotted column names and keys named count.

## Workspace checks

The operator supplied these outputs from the personal AWS trial sandbox. They were not independently retrieved by the assistant.

| Run | Result | Saved evidence |
|---|---|---|
| Fleet scan | 14 objects, complete=true, no warnings | workspace-fleet-evidence.json |
| Validation notebook | Matching output PASS; changed output FAIL | workspace-validation-evidence.json |
| Trigger-safety fixture | Paused source unchanged, no-trigger clone, backup saved, zero jobs started | workspace-clone-safety-evidence.json |
| Connected programme | Ready VALIDATED, real job/run IDs, comparison PASS, negative control FAIL, 17 audit events | workspace-program-evidence.json |
| Refreshed dashboard | 14 objects, 9 programme states, 1 validated, 34 programme events, 1 safety report, 6 comparison reports | workspace-dashboard-evidence.json |

The connected run summary reports job `817177348613471`, run `1014105518777189`, ready workload VALIDATED, blocked workload BLOCKED and the 11-task candidate PLANNED. The negative control failed as intended. The refreshed dashboard includes one validated programme workload. Its 9 programme states and 34 events aggregate earlier programme attempts as well.

The validation baseline is synthetic. The trigger-safety source was already serverless. Neither result proves classic-compute execution.

## New implementation checks

- 136 local tests passed, including 4 real local Spark tests (Spark 3.5.9, Java 17). Jobs API lifecycle tests use a simulated service; they are not live classic execution.
- Bundle configuration was checked against the installed official CLI 1.22.1 JSON schema with zero schema errors. Authentication/deployment is a separate check.
- Compilation and selected Ruff correctness checks passed.
- `workspace_acceptance` adds operator-run 1-task/11-task serverless validation and negative controls; its output has not yet been supplied.
- The new general controller adds real classic A/B, cost, reconciliation and paused cutover/rollback. These require suitable workspace resources for live acceptance.

## Still to check

- Full authenticated Bundle validation and deployment. The local CLI attempt stopped at missing credentials.
- Streamlit App deployment. The demonstrated progress view is the notebook dashboard.
- Genuine classic execution and before/after billing. No savings are claimed.

The new fleet controller uses a cooperative Delta lease; the earlier demo runner remains single-writer. Sandbox self-approval is limited to the authenticated fixture owner. Restrict UC journal/lease write privileges.
