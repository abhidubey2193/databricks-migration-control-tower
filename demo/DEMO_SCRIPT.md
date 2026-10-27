# Five minute recording script

A recording is still required. This script is not a video link.

## Prepare the workspace view

1. Pull enhanced-control-tower in the existing sandbox Git folder.
2. Rerun notebooks/fleet_scan.py with catalog=auto and schema=serverless_migration. This refresh includes the newly created safety-fixture jobs.
3. Rerun notebooks/control_tower_dashboard.py with the same catalog/schema. It should load inventory, validation history and the latest scheduling-safety evidence.
4. Keep the completed validation_demo and clone_safety_demo outputs available. Do not rerun the safety demo for recording unless needed: its saved evidence is enough.
5. Record the screen locally using your usual recording tool. Avoid credential screens and customer environments. The workspace is a personal AWS trial sandbox.

## Recording sequence

| Time | What to show and say |
|---|---|
| 0:00–0:25 | Introduce workspace-wide discovery, readiness and controlled validation. Explain SDK → shared assessment → Delta evidence → notebook dashboard, with local SQLite for full approval/audit workflows. |
| 0:25–1:20 | Show the notebook dashboard Overview/Fleet. Point to the scan timestamp, object types, compute classification and scores. State that this is a small sandbox inventory, scoped to the scanning principal. Do not claim a large classic fleet. |
| 1:20–2:00 | Show demo/expected-results.json or src/smct/demo.py. Identify the explicitly synthetic ready job, image/init-script blocker and 11-task dependency chain. Explain scoring, recommendations and blocked clone path. These are assessment examples, not live classic jobs. |
| 2:00–3:00 | Show Governance and the completed clone_safety_demo output. Actual source/clone job IDs were created through Jobs API. Source remained PAUSED and unchanged, clone has no triggers, backup persisted, zero jobs started. Say the source is already serverless: this proves L10 trigger safety, not classic migration. |
| 3:00–4:10 | Show Validation. Matching data PASSes; intentionally changed data FAILs with 3 missing/3 extra records and 2 changed key groups. Typed schemas and duplicate groups are checked. State clearly that the baseline is imported synthetic data; no classic-compute execution is claimed. Multiple report rows are append-only demo history, not multiple migrated workloads. |
| 4:10–4:40 | Show the deployment commands and committed test evidence: 54 local tests. Notebook path has user-reported workspace success; full Bundle and Streamlit App deployment remain unverified. |
| 4:40–5:00 | Summarize implemented L1/L8/L10, static detection examples and manual cutover controls. Acknowledge no actual cost savings, no genuine classic run, no authenticated enterprise approver and no automatic promotion. |

Keep the final video under five minutes. Add its actual shareable link to the submission email and verify judges can open it before sending.

## Optional local reproduction

```bash
pip install -e '.[app,dev]'
python -m pytest -q
python -m smct.cli demo --output demo-output
SMCT_DB=demo-output/smct.sqlite python -m streamlit run app/dashboard.py
```

The local demo is fully synthetic and makes no workspace calls. Use a fresh output directory on repeat runs.
