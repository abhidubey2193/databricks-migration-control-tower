# Limitation examples

For the connected workspace example, run `notebooks/program_demo` with `action=assess`, inspect the plans, then use `action=run` for the ready fixture. All classic definitions and baseline records are synthetic.

| ID | Example | Result and boundary |
|---|---|---|
| L1 | Ready orders, blocked image/init-script workload, 11-task chain | Assessment, waves, approval, backup, clone/run references and validation share a Delta programme/workload key. New connected route awaits workspace execution. |
| L2 | 11-task dependency chain | Generates a direct Jobs API candidate with all dependencies retained. No split is needed for the agent-specific ten-task limit. The demo does not execute this large plan. |
| L3/L4 | Custom container and init script | Detected and blocked; scripts are not translated. |
| L8 | Four sample rows, including a duplicate and a null | Real target-job path, pinned Delta baseline/output versions, exact comparison and a deliberate failed negative control. New job path awaits workspace execution. The earlier standalone comparison ran successfully. |
| L9 | Source/code fingerprint, approved plan and clone readback | Configuration drift blocks progress. No Git patch or Bundle writeback. |
| L10 | Paused serverless source and no-trigger clone | Earlier live fixture passed without starting either job. Source was already serverless. |
| L12 | Target job ID in system.billing.usage | Reads target usage when available. Billing can lag; no classic baseline or savings calculation. |
| L13 | Cast, division and missing streaming trigger | Heuristic findings with review suggestions. No automatic semantic rewrite. |

The ordinary clone path remains capped at ten tasks. The explicit `allow_large=True` planner supports up to 100 supported tasks and rejects missing dependencies, cycles and unsupported configurations.

For the local synthetic demo:

```bash
smct demo --output demo-output
```

It uses a simulated SDK and SQLite. Expected output is in `expected-results.json`. It is separate from the workspace evidence files.
