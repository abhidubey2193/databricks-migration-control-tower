# Executable limitation examples

Run `python -m smct.cli demo --output demo-output` from the repository root.

| ID | Input | Observable output | Depth |
|---|---|---|---|
| L1 | Four synthetic classic job definitions | Persisted inventory, readiness, statuses, approval events and deterministic wave plan | Local implementation; SDK discovery separately requires workspace authentication |
| L2 | 11-task chain with explicit dependencies | Blocker, rejected clone path, manual dependency-preserving migration recommendation | Detection; no splitting |
| L3/L4 | Custom image and init script | Container blocker and script finding with remediation | Detection; no container conversion |
| L8 | Three synthetic records with duplicates, reordered and changed outputs | PASS and FAIL, null counts, typed schema comparison, changed key groups | Local exact comparator; no classic run |
| L9 | Original settings digest and backup | Cutover checks digest and backup integrity | Configuration drift; no Git patch generation |
| L10 | Unpaused synthetic schedule | Clone has no schedule/continuous/trigger, original unchanged | Local simulated SDK plus live serverless-source safety fixture reported by user; no classic migration |
| L13 | CAST, division and streaming without trigger source | Findings and try_cast/try_divide/explicit-trigger suggestions | Heuristic detection; no automatic semantic rewrite |

`notebooks/validation_demo.py` is a separate workspace integration example. It writes a synthetic baseline and executes a Spark transformation, persists target outputs in distinct append-only tables, compares PASS/FAIL cases and records reports in Delta. Until this notebook succeeds in an authorized workspace, it is unverified. It never represents its synthetic baseline as an executed classic run.

The comparator uses exact equality with zero missing/extra tolerance. Key groups preserve duplicate multiplicity. SQL-warehouse mode compares DB-API type metadata, including empty tables, and refuses over-limit tables rather than sampling. Inputs must be immutable isolated snapshots during comparison; concurrent writes and table-version binding are not automated.
