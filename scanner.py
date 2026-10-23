"""Offline, read-only static risk scan of a synthetic Databricks inventory."""
import argparse
import json
from pathlib import Path

RISK_FIELDS = {
    "spark_conf": "Custom Spark configuration requires review",
    "init_scripts": "Init scripts require replacement review",
    "environment_variables": "Environment variables require review",
}

def assess(job):
    issues = []
    tasks = job.get("tasks", [])
    if len(tasks) > 10:
        issues.append("More than 10 tasks: review orchestration")
    for task in tasks:
        task_name = task.get("task_key", "task")
        for key, label in RISK_FIELDS.items():
            if task.get(key):
                issues.append(f"{task_name}: {label}")
        if task.get("existing_cluster_id"):
            issues.append(f"{task_name}: references existing classic cluster")
        path = task.get("notebook_task", {}).get("notebook_path", "")
        if path.startswith("/dbfs/"):
            issues.append(f"{task_name}: review legacy DBFS notebook path")
    if job.get("schedule") or job.get("continuous") or job.get("trigger"):
        issues.append("Triggered job: test clones must not run automatically")
    return {"job_id": job.get("job_id"), "name": job.get("name", "Unnamed"),
            "status": "REVIEW" if issues else "CANDIDATE", "blockers": issues}

def scan(inventory):
    return {"mode": "read-only", "results": [assess(job) for job in inventory.get("jobs", [])]}

def main():
    parser = argparse.ArgumentParser(description="Offline migration readiness demonstration")
    parser.add_argument("inventory", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = json.dumps(scan(json.loads(args.inventory.read_text())), indent=2)
    if args.output:
        args.output.write_text(result)
    else:
        print(result)

if __name__ == "__main__":
    main()
