# Verification

27 October 2026. Python 3.12, Databricks SDK 0.157.0, Streamlit 1.66.1 and CLI 1.22.1.

## Local checks

`python -m pytest -q`: **87 passed**.

Coverage includes record comparison, clone restrictions, the programme state machine, approval ownership, configuration drift, pinned baseline binding, idempotent run requests, interrupted clone recovery and dashboard rendering. Python compilation and `git diff --check` also pass. GitHub Actions also passed for the serverless write fix, commit `b4bd26c` ([run](https://github.com/abhidubey2193/databricks-migration-control-tower/actions/runs/40549071165)).

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

## Still to check

- Full authenticated Bundle validation and deployment. The local CLI attempt stopped at missing credentials.
- Streamlit App deployment. The demonstrated progress view is the notebook dashboard.
- Genuine classic execution and before/after billing. No savings are claimed.

The programme runner assumes one writer per programme. Duplicate revisions fail closed; the journal is not a distributed lock. Sandbox self-approval is limited to the authenticated fixture owner.
