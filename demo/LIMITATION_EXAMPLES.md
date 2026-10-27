# Challenge coverage

The IDs describe the challenge agent's limitations. They are not a claim that every limitation still exists in current serverless compute. For example, DataFrame cache/persist and some JAR workloads now have serverless support.

| ID | Implemented behaviour | Boundary / evidence |
|---|---|---|
| L1 | SDK pagination; jobs, clusters and recursive notebooks; code findings; readiness; manifest waves; authenticated approval; leased Delta lifecycle; joined dashboard; explicit keep-classic/defer decisions | 14-object scan and connected programme demonstrated in the sandbox. New general fleet lifecycle has local adapter regression coverage; visibility remains permission-scoped |
| L2 | Direct Jobs API compiler up to 1,000 tasks; cycle/reference checks; preserved DAG; queues and inflight control | 11-task two-sided execution tested against simulated Jobs; workspace_acceptance can execute the 11-task serverless chain live |
| L3 | Bounded S3/DBFS/Workspace/Volume retrieval; pinned pip-install translation into environment dependencies; script hash; unresolved shell steps block apply | Retrieval/translation tested with bounded fake readers. Live cloud retrieval needs the caller's storage permissions; scripts are never executed |
| L4 | Detect old/ML runtimes, images, profiles, Spark configs and environment variables; resolve shared clusters; block unsupported compilation | No guessed replacement for OS/image/IAM semantics; remediate explicitly or keep classic |
| L5 | Translate pinned PyPI/Volume wheels; explicit egg/JAR/Maven replacement mapping; serverless JAR environment proposal using java_dependencies | Native JAR proposal round-trips through the official SDK. Binary rebuilding and runtime compatibility still require validation |
| L6 | Detect catalog-cache/checkpoint/global-view/mount operations; AST proposals for mapped global views and optional cache-call removal | Proposals never edit production source in place. No automatic mount or checkpoint semantic guess. Current supported DataFrame caching is not falsely blocked |
| L7 | Language detection and code-scan propagation; authenticated KEEP_CLASSIC/DEFERRED decisions | No claim of general Scala/R-to-Python translation; JAR packaging is a separate reviewed route |
| L8 | Genuine classic/serverless validation clones; same pinned inputs; separate outputs; run/config/code binding; distributed typed EXCEPT ALL; nulls/duplicates/keys; negative-control harness | Earlier synthetic-baseline serverless run is live-verified. Genuine classic execution cannot be demonstrated in the current serverless-only workspace |
| L9 | Configuration/notebook fingerprints; atomic Bundle file writeback; compare-and-swap drift checks; rollback resource restoration; explicit trigger-policy drift notice | Resource binding and Git review remain explicit owner operations. No fabricated Git reconciliation or deployment claim |
| L10 | Strip all legacy and new trigger-array forms from validation clones; verify readback; cutover/rollback paused; separate activation; idle-run checks | Earlier live paused-source fixture passed. Source fixture was already serverless. Full transition/recovery paths have local regression coverage |
| L11 | Cluster log-delivery flag, SSH key count, init locations and environment-variable names; per-workload warnings | Metadata inventory avoids publishing literal environment values; classic observability is not replicated on serverless |
| L12 | Correction-aware billing query; exact job/run/SKU/team attribution; effective USD list prices; before/after DBUs/list cost; incomplete-price and pending states | No savings without matching outputs and complete attributed usage. Shared-cluster cloud costs/discounts are not guessed |
| L13 | Missing/time-based streaming-trigger detection; availableNow Python proposal; SQL parser proposals for TRY_CAST/TRY_DIVIDE | Changes require semantic review and A/B; no silent error-to-null rewrite |

`tests/test_fleet_runtime.py`, `test_remediation_cost.py`, `test_control_safety.py` and `test_spark_comparison.py` cover the new paths. Local simulations are distinct from live workspace evidence in VERIFICATION.md.
