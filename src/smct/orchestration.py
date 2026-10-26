"""Guarded SDK clone planning. All mutations require explicit --apply and stored approval.
Does NOT claim to automate the Beta Genie Code migration agent.
"""
import copy
import hashlib
import json
from pathlib import Path
from .discovery import serial

SUPPORTED={'notebook_task','spark_python_task','python_wheel_task'}
COMPUTE_FIELDS={'existing_cluster_id','new_cluster','job_cluster_key'}
TRIGGERS=('schedule','trigger','continuous')

def digest(settings):
    return hashlib.sha256(json.dumps(settings,sort_keys=True,default=str).encode()).hexdigest()

def build_serverless_clone(settings):
    """Returns candidate without side effects; unsupported or ambiguous constructs fail closed."""
    tasks=settings.get('tasks') or []
    if not tasks or len(tasks)>10: raise ValueError('Only jobs with 1-10 explicit tasks supported')
    if settings.get('git_source'): raise ValueError('Git-backed jobs require repository changes before cloning (L9)')
    if settings.get('parameters'): raise ValueError('Job-level parameters require manual review')
    clean=copy.deepcopy(settings)
    clean['name']='[SMCT VALIDATION - NO SCHEDULE] '+str(settings.get('name','unnamed'))
    for field in TRIGGERS: clean.pop(field,None)
    clean.pop('job_clusters',None)
    clean.pop('deployment',None)
    clean.pop('run_as',None)
    clean.pop('edit_mode',None)
    clean.pop('environments',None)
    clean.pop('health',None)
    clean.pop('queue',None)
    clean.pop('max_concurrent_runs',None)
    clean['max_concurrent_runs']=1
    for task in clean['tasks']:
        kinds=[k for k in SUPPORTED if task.get(k)]
        if len(kinds)!=1: raise ValueError(f"Unsupported or ambiguous task {task.get('task_key')}")
        if task.get('libraries'): raise ValueError('Task libraries require environment translation and manual review (L5)')
        if task.get('depends_on') and any('outcome' in d for d in task['depends_on']): raise ValueError('Conditional task dependencies need review')
        for field in COMPUTE_FIELDS: task.pop(field,None)
        task.pop('libraries',None)
        task.pop('compute_key',None)
        task.pop('run_if',None) if False else None
        task['environment_key']='smct_env'
    clean['environments']=[{'environment_key':'smct_env','spec':{'environment_version':'2','dependencies':[]}}]
    allowed={'name','description','tasks','environments','tags','email_notifications','notification_settings','timeout_seconds','max_concurrent_runs','format','parameters'}
    result={k:v for k,v in clean.items() if k in allowed}
    result.pop('email_notifications',None)
    result.pop('notification_settings',None)
    result.pop('format',None)
    return result

def clone_job(w, store, job_id, backup_dir='backups',apply=False):
    wid=f'job:{job_id}'
    item=store.get(wid)
    if not store.approval(wid): raise PermissionError('Explicit recorded approval required')
    if store.migration(wid): raise ValueError('Migration already recorded')
    if not item['eligible']: raise ValueError('Readiness contains blockers; refuse clone')
    data=serial(w.jobs.get(job_id).settings)
    if item['compute_mode']!='classic': raise ValueError('Source is not a classic job')
    candidate=build_serverless_clone(data)
    source_digest=digest(data)
    if not apply: return {'dry_run':True,'candidate':candidate,'source_digest':source_digest}
    backup=Path(backup_dir).resolve() / f'job-{job_id}-{source_digest[:12]}.json'
    backup.parent.mkdir(parents=True, exist_ok=True)
    backup.write_text(json.dumps(data,indent=2,default=str),encoding='utf8')
    backup.chmod(0o600)
    created=w.api_client.do('POST','/api/2.1/jobs/create',body=candidate)
    clone_id=created.get('job_id')
    if not clone_id: raise RuntimeError('Create did not return clone job id. Check workspace and reconcile manually')
    store.save_migration(wid,job_id,int(clone_id),str(backup),source_digest)
    return {'dry_run':False,'clone_job_id':clone_id,'backup':str(backup),'source_digest':source_digest}

def cutover_report(w,store,job_id):
    from .governance import evaluate_cutover
    wid=f'job:{job_id}'
    m=store.migration(wid)
    if not m: raise ValueError('No migration clone recorded')
    before=serial(w.jobs.get(job_id).settings)
    clone=serial(w.jobs.get(m['clone_job_id']).settings)
    backup=Path(m['backup_path'])
    no_triggers=not any(clone.get(x) for x in TRIGGERS)
    no_drift=digest(before)==m['source_digest']
    validation=store.validation(wid)
    gate=evaluate_cutover(bool(store.approval(wid)),backup.is_file(),bool(validation and validation.get('passed')),no_triggers,no_drift and not bool(before.get('git_source')))
    return {'allowed':gate.allowed,'reasons':gate.reasons,'original_unchanged':no_drift,'clone_trigger_disabled':no_triggers,'validation':validation,'note':'Manual promotion required; the tool never activates two schedules.'}
