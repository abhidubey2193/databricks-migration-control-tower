# Five minute recording script

Recording is outstanding. This script is not a video link.

## Preparation

```bash
pip install -e '.[app,dev]'
python -m pytest -q
python -m smct.cli demo --output demo-output
SMCT_DB=demo-output/smct.sqlite python -m streamlit run app/dashboard.py
```

Use a new directory on repeat runs. No credentials, workspace or network are required for the synthetic demonstration. Inspect `demo-output/evidence.json` and the source fixture module `src/smct/demo.py`. Expected results from a real local run are in `demo/expected-results.json`.

## Recording

- **0:00–0:30** Explain the workspace-level migration tracking gap and architecture: SDK inventory → shared assessment → SQLite/Delta evidence → dashboard; guarded unscheduled clones; manual production promotion.
- **0:30–1:30** Show the Overview and Fleet tabs. Point to the SYNTHETIC banner. Show ready orders (100), blocked container (47, ineligible), 11-task chain (65, ineligible), scheduled orders (82). Scores are metadata indicators, not compatibility proof.
- **1:30–2:15** Open remediation findings L2/L3/L4/L10. Show wave planning excludes blocked jobs. Show the streaming/ANSI findings in `evidence.json`. Static findings require review.
- **2:15–3:00** Show approval audit, backup and `clone.json`. Original `workloads.json` retains its schedule. Clone has no schedule, continuous or trigger. Explain the client is simulated and no API calls reached Databricks.
- **3:00–4:00** Open Validation tab. Equivalent shuffled data PASSes, deliberately changed duplicate record FAILs. Show null counts, typed schema, missing/extra counts and changed key group. Baseline is imported synthetic data, never classic execution.
- **4:00–4:30** Show the blocked cutover gate. Synthetic or unbound PASS cannot authorize promotion. Backup integrity and source drift are checked. Local approval identity is manually entered.
- **4:30–5:00** If workspace testing has been completed, show the genuine Bundle run and Delta tables in UC mode. Otherwise state deployment is unverified and show the exact deployment commands. State cost data and genuine classic execution remain unavailable.

Do not display authentication, tokens, client workspaces or customer data in the video. Keep the video under five minutes and add its actual link to the submission before sending.
