# Databricks Classic → Serverless Migration Control Tower

**Safe, independent static-assessment prototype**, not an automated migration tool and not the larger hackathon implementation.

## Run
```bash
python scanner.py demo/sample_inventory.json
python -m unittest discover -s tests -v
```

Reads a local synthetic/redacted job inventory JSON and reports some migration review signals: existing classic cluster use, init scripts, Spark configuration, environment variables, triggered schedules, legacy DBFS notebook paths, and large task counts.

A `CANDIDATE` result only means **no risks were detected by these limited rules**, not that a job is serverless compatible.

**Not implemented:** Databricks API discovery, agent invocation, live migration, approvals, clones, A/B validation, Unity Catalog storage, cost attribution, rollback and cutover. No production changes or API calls are made. Never publish enterprise workspace names, paths, secrets or customer artifacts without authorization.
