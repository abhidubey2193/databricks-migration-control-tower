# Verification

27 October 2026. Python 3.12, Databricks SDK 0.157.0, Streamlit 1.66.1 and CLI 1.22.1.

## Local checks

`python -m pytest -q`: **82 passed**.

Coverage includes record comparison, clone restrictions, the programme state machine, approval ownership, configuration drift, pinned baseline binding, idempotent run requests, interrupted clone recovery and dashboard rendering. Python compilation and `git diff --check` also pass.

## Workspace checks

The operator supplied these outputs from the personal AWS trial sandbox. They were not independently retrieved by the assistant.

| Run | Result | Saved evidence |
|---|---|---|
| Fleet scan | 9 objects, complete=true, no warnings | workspace-fleet-evidence.json |
| Validation notebook | Matching output PASS; changed output FAIL | workspace-validation-evidence.json |
| Trigger-safety fixture | Paused source unchanged, no-trigger clone, backup saved, zero jobs started | workspace-clone-safety-evidence.json |
| Refreshed dashboard | 9 objects, 1 safety report, 4 comparison reports | workspace-dashboard-evidence.json |

The validation baseline is synthetic. The trigger-safety source was already serverless. Neither result proves classic-compute execution.

## Still to check

- Run the new `program_demo` notebook with `action=run`. It should record one connected lifecycle, an actual serverless Jobs run, a version-pinned comparison and a failed negative control.
- Confirm the new programme state/audit view in the dashboard.
- Full authenticated Bundle validation and deployment. The local CLI attempt stopped at missing credentials.
- Streamlit App deployment. The demonstrated progress view is the notebook dashboard.
- Genuine classic execution and before/after billing. No savings are claimed.

The programme runner assumes one writer per programme. Duplicate revisions fail closed; the journal is not a distributed lock. Sandbox self-approval is limited to the authenticated fixture owner.
