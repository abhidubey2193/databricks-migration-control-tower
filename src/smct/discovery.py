"""Workspace inventory using SDK pagination. No workspace mutations."""
import copy
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


def get_job_settings(w, job_id):
    """Jobs 2.2 paginates arrays even on GET; never assess a partial DAG."""
    first = w.jobs.get(int(job_id))
    settings = copy.deepcopy(serial(first.settings))
    token = getattr(first, "next_page_token", None)
    seen = set()
    while token:
        if token in seen or len(seen) >= 100:
            raise ValueError("Job pagination did not terminate")
        seen.add(token)
        page = w.jobs.get(int(job_id), page_token=token)
        extra = serial(page.settings)
        for key in ("tasks", "job_clusters", "environments", "parameters"):
            if extra.get(key):
                settings.setdefault(key, []).extend(extra[key])
        token = getattr(page, "next_page_token", None)
    keys = [t.get("task_key") for t in settings.get("tasks", [])]
    if len(keys) != len(set(keys)):
        raise ValueError("Duplicate task keys across job pages")
    return settings

def build_job(job_id: int, settings: dict) -> Workload:
    tasks = settings.get("tasks") or []
    is_classic = any(t.get("existing_cluster_id") or t.get("new_cluster") or t.get("job_cluster_key") for t in tasks)
    findings = check_job(settings)
    readiness, eligible = score(findings)
    return Workload(f"job:{job_id}", "JOB", settings.get("name") or str(job_id), compute_mode="classic" if is_classic else "serverless_or_other", task_count=len(tasks), readiness=readiness, eligible=eligible, findings=findings, attributes={"job_id": job_id, "has_schedule": bool(settings.get("schedule") or settings.get("trigger") or settings.get("continuous") or settings.get("triggers")), "task_keys": [t.get("task_key") for t in tasks], "notebook_paths": [t["notebook_task"]["notebook_path"] for t in tasks if t.get("notebook_task",{}).get("notebook_path")], "assessment_scope": "metadata_only", "owner": settings.get("run_as", {}).get("user_name", "Not recorded"), "team": settings.get("tags", {}).get("team", "Not recorded"), "tags": {k:v for k,v in settings.get("tags", {}).items() if k in ("team","owner","cost_center","smct_program","smct_workload")}})

def build_cluster(cluster: dict) -> Workload:
    cid = cluster.get("cluster_id", "unknown")
    findings = check_cluster(cluster)
    readiness, eligible = score(findings)
    return Workload(f"cluster:{cid}", "CLUSTER", cluster.get("cluster_name") or cid, compute_mode="classic", readiness=readiness, eligible=eligible, findings=findings, attributes={"cluster_id": cid, "state": cluster.get("state"), "runtime": cluster.get("spark_version"), "cluster_log_delivery_configured": bool(cluster.get("cluster_log_conf")), "ssh_key_count": len(cluster.get("ssh_public_keys", [])), "environment_variable_names": sorted(cluster.get("spark_env_vars", {})), "init_script_locations": [next((v.get("destination") for v in x.values() if isinstance(v,dict)), "Unknown") for x in cluster.get("init_scripts", [])]})

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
            settings = get_job_settings(w, item.job_id)
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
            settings=get_job_settings(w, job.attributes['job_id'])
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
