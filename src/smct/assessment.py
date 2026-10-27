"""Read-only, deterministic detection. Findings never trigger automatic mutations."""
import re
from .models import Finding

SEVERITY_PENALTY = {"blocker": 35, "high": 18, "medium": 9, "low": 3}

CODE_RULES = [
    ("L6", "high", r"\.(?:checkpoint|localCheckpoint)\s*\(|spark\.catalog\.(?:cacheTable|uncacheTable|clearCache)\s*\(|(?i:(?:cache|uncache|refresh)\s+table|clear\s+cache)", "Unsupported catalog cache/checkpoint API", "Review recomputation and materialise required intermediate data to Delta."),
    ("L6", "blocker", r"\b(?:sc|spark\.sparkContext)\.setCheckpointDir\s*\(", "RDD checkpoint API", "Review serverless-compatible checkpoint strategy."),
    ("L6", "high", r"\bglobal_temp\b|createGlobalTempView\s*\(", "Global temporary view", "Persist intermediate output to governed Delta tables."),
    ("L6", "blocker", r"dbutils\.fs\.(?:mount|unmount|updateMount|refreshMounts)\s*\(", "DBFS mount management", "Migrate storage access to Unity Catalog volumes or external locations."),
    ("L13", "medium", r"(?i)\b(?<!try_)cast\s*\(|(?<!try_)\bdivide\s*\(|\s/\s|\bdate_parse\b", "Potential ANSI semantic difference", "Review casts and divisions; prefer try_cast/try_divide where applicable."),
    ("L13", "low", r"(?i)\.writeStream\b", "Streaming query", "Check trigger configuration and compare classic/serverless behavior."),
]

def check_code(source: str, language: str = "PYTHON") -> list[Finding]:
    result: list[Finding] = []
    if language.upper() in ("SCALA", "R"):
        result.append(Finding("L7", "blocker", f"Unsupported notebook language: {language}", "Consider PySpark/SQL rewrite or keep on classic."))
    for limit, severity, pattern, description, remediation in CODE_RULES:
        if re.search(pattern, source):
            result.append(Finding(limit, severity, description, remediation))
    if re.search(r'\.trigger\s*\([^)]*(?:processingTime|continuous)\s*=', source):
        result.append(Finding('L13','high','Unsupported time-based streaming trigger','Review bounded availableNow processing or a continuous Lakeflow pipeline'))
    if '.writeStream' in source and not re.search(r'\.trigger\s*\(', source):
        result.append(Finding('L13','high','Streaming write has no explicit trigger','Use availableNow=True for bounded processing after reviewing semantics; validate output'))
    return result

def check_job(settings: dict) -> list[Finding]:
    findings = []
    tasks = settings.get("tasks") or []
    for task in tasks:
        if not any(task.get(k) for k in ('notebook_task','spark_python_task','python_wheel_task')):
            findings.append(Finding('L1','blocker',f"Unsupported task type: {task.get('task_key','unknown')}",'Use manual assessment; automatic clone supports notebook, Python and wheel tasks only'))
    if len(tasks) > 10:
        findings.append(Finding("L2", "blocker", f"Job contains {len(tasks)} tasks (>10)", "Plan task-group migration; do not split dependencies automatically."))
    if settings.get("schedule") or settings.get("trigger") or settings.get("continuous") or settings.get("triggers"):
        findings.append(Finding("L10", "high", "Job has an active trigger definition", "Never enable clone and original simultaneously; require a cutover gate."))
    for t in tasks:
        key = t.get("task_key", "unknown")
        for lib in t.get("libraries") or []:
            if "egg" in lib or "jar" in lib or "maven" in lib:
                findings.append(Finding("L5", "blocker", f"Task {key} uses egg/JAR/Maven dependency", "Review package compatibility; replace with wheel/PyPI or supported task type."))
        for c in [t.get("new_cluster") or {}]:
            findings.extend(check_cluster(c, f"task {key}"))
        if t.get("existing_cluster_id"):
            findings.append(Finding("L4", "medium", f"Task {key} uses existing cluster", "Resolve cluster configuration and assess compatibility."))
        if t.get("spark_jar_task"):
            findings.append(Finding("L5", "high", f"Task {key} is a Spark JAR task", "Check supported serverless task types and repackage as needed."))
    for jc in settings.get("job_clusters") or []:
        findings.extend(check_cluster(jc.get("new_cluster") or {}, f"job cluster {jc.get('job_cluster_key','')}") )
    return findings

def check_cluster(c: dict, label: str = "cluster") -> list[Finding]:
    findings = []
    runtime = str(c.get("spark_version") or "")
    major = re.match(r"^(\d+)", runtime)
    if major and int(major.group(1)) < 13:
        findings.append(Finding("L4", "blocker", f"{label} runtime below DBR 13: {runtime}", "Upgrade or rewrite workload before migration."))
    if "ml" in runtime.lower():
        findings.append(Finding("L4", "blocker", f"{label} uses ML Runtime", "Review ML dependencies and supported serverless alternatives."))
    if c.get("docker_image"):
        findings.append(Finding("L4", "blocker", f"{label} uses custom container image", "Replace container-provided dependencies with supported environments."))
    if (c.get("aws_attributes") or {}).get("instance_profile_arn"):
        findings.append(Finding("L4", "blocker", f"{label} uses AWS instance profile", "Use Unity Catalog managed access and credentials."))
    if c.get("init_scripts"):
        findings.append(Finding("L3", "high", f"{label} uses init scripts", "Review scripts; extract supported dependencies into serverless environment spec."))
    if c.get('ssh_public_keys'):
        findings.append(Finding('L11','medium',f'{label} has SSH keys','Serverless has no cluster SSH access; use supported observability'))
    if c.get("cluster_log_conf"):
        findings.append(Finding("L11", "medium", f"{label} configures cluster log delivery", "Document lost log delivery and alternative telemetry."))
    if c.get("spark_conf"):
        findings.append(Finding("L4", "medium", f"{label} has custom Spark configs", "Review each setting for serverless restrictions."))
    if c.get("spark_env_vars"):
        findings.append(Finding("L4", "medium", f"{label} has environment variables", "Move permitted values to supported environments; never export secret values."))
    return findings

def score(findings: list[Finding]) -> tuple[int, bool]:
    unique = {(f.limitation, f.detail, f.severity) for f in findings}
    result = max(0, 100 - sum(SEVERITY_PENALTY[x[2]] for x in unique))
    eligible = not any(f.severity == "blocker" for f in findings)
    return result, eligible
