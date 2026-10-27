"""Workspace inventory using SDK pagination. No workspace mutations."""
from .models import Workload, Finding
from .assessment import check_cluster, check_job, check_code, score

def serial(obj):
    if obj is None:
        return {}
    if hasattr(obj, 'as_dict'):
        return obj.as_dict()
    if isinstance(obj, dict):
        return obj
    return {}

def build_job(job_id: int, settings: dict) -> Workload:
    tasks = settings.get("tasks") or []
    is_classic = any(t.get("existing_cluster_id") or t.get("new_cluster") or t.get("job_cluster_key") for t in tasks)
    findings = check_job(settings)
    readiness, eligible = score(findings)
    return Workload(f"job:{job_id}", "JOB", settings.get("name") or str(job_id), compute_mode="classic" if is_classic else "serverless_or_other", task_count=len(tasks), readiness=readiness, eligible=eligible, findings=findings, attributes={"job_id": job_id, "has_schedule": bool(settings.get("schedule") or settings.get("trigger") or settings.get("continuous")), "task_keys": [t.get("task_key") for t in tasks], "notebook_paths": [t["notebook_task"]["notebook_path"] for t in tasks if t.get("notebook_task",{}).get("notebook_path")], "assessment_scope": "metadata_only"})

def build_cluster(cluster: dict) -> Workload:
    cid = cluster.get("cluster_id", "unknown")
    findings = check_cluster(cluster)
    readiness, eligible = score(findings)
    return Workload(f"cluster:{cid}", "CLUSTER", cluster.get("cluster_name") or cid, compute_mode="classic", readiness=readiness, eligible=eligible, findings=findings, attributes={"cluster_id": cid, "state": cluster.get("state"), "runtime": cluster.get("spark_version")})

def inventory(w=None, include_notebooks=True, root="/Workspace", max_notebooks=500):
    if w is None:
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient()
    workloads = []
    errors = []
    try:
        jobs = list(w.jobs.list())
    except Exception as exc:
        jobs = []
        errors.append(f"Jobs inventory unavailable: {type(exc).__name__}")
    for item in jobs:
        try:
            full = w.jobs.get(item.job_id)
            settings = serial(full.settings)
            workloads.append(build_job(item.job_id, settings))
        except Exception as exc:
            errors.append(f"Job {item.job_id}: {type(exc).__name__}: {exc}")
    try:
        for c in w.clusters.list():
            workloads.append(build_cluster(serial(c)))
    except Exception as exc:
        errors.append(f"Clusters: {type(exc).__name__}: {exc}")
    # Resolve classic shared-cluster blockers into their referencing jobs.
    cluster_map={x.attributes['cluster_id']:x for x in workloads if x.kind=='CLUSTER'}
    for job in workloads:
        if job.kind!='JOB': continue
        try:
            settings=serial(w.jobs.get(job.attributes['job_id']).settings)
            for task in settings.get('tasks') or []:
                cid=task.get('existing_cluster_id')
                if not cid: continue
                if cid in cluster_map: job.findings.extend(cluster_map[cid].findings)
                else: job.findings.append(Finding('L1','blocker','Referenced cluster configuration unavailable','Restore permissions and rescan before approval'))
            job.readiness,job.eligible=score(job.findings)
        except Exception as exc:
            job.eligible=False
            errors.append(f'Cluster mapping for {job.workload_id}: {type(exc).__name__}')
    if include_notebooks:
        pending = [root]
        seen = set()
        notebook_count = 0
        while pending and notebook_count < max_notebooks:
            parent = pending.pop()
            if parent in seen: continue
            seen.add(parent)
            try:
                for o in w.workspace.list(parent):
                    data = serial(o)
                    typ = str(data.get("object_type") or "").upper()
                    path = data.get("path") or ""
                    if typ == "DIRECTORY":
                        pending.append(path)
                    elif typ == "NOTEBOOK":
                        lang = str(data.get("language") or "")
                        findings = check_code("", lang)
                        s, eligible = score(findings)
                        workloads.append(Workload(f"notebook:{path}", "NOTEBOOK", path.split('/')[-1], source_path=path, compute_mode="unknown_unattached", readiness=s, eligible=eligible, findings=findings, attributes={"language":lang, "code_scanned": False, "compute_mapping": "unknown"}))
                        notebook_count += 1
                        if notebook_count >= max_notebooks:
                            errors.append(f"Notebook inventory truncated at cap {max_notebooks}; increase --max-notebooks for completeness")
                            break
            except Exception as exc:
                errors.append(f"Workspace path {parent}: {type(exc).__name__}: {exc}")
    notebook_jobs={}
    for job in workloads:
        if job.kind=='JOB':
            for path in job.attributes.get('notebook_paths',[]): notebook_jobs.setdefault(path,[]).append(job.workload_id)
    for item in workloads:
        if item.kind=='NOTEBOOK': item.attributes['referencing_jobs']=notebook_jobs.get(item.source_path,[])
    return workloads, errors
