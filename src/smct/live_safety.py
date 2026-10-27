"""Sandbox-only Jobs API trigger safety fixture; never executes any job.

The source is already serverless. This demonstrates L10 safety, not a classic
migration. A callback must persist its backup before clone creation.
"""
import copy
from .discovery import serial
from .orchestration import build_serverless_clone, digest, TRIGGERS, COMPUTE_FIELDS

SOURCE_NAME='SMCT SYNTHETIC L10 PAUSED SOURCE'
FIXTURE_TAG='serverless_schedule_safety'


def run_safety_demo(w, notebook_path, persist_backup, *, apply=False):
    if not notebook_path.startswith('/'): raise ValueError('Absolute workspace notebook path required')
    source={'name':SOURCE_NAME,'description':'Synthetic serverless trigger-safety fixture. Keep paused. Never run for this demo.',
            'tags':{'smct_fixture':FIXTURE_TAG,'smct_role':'source'},
            'schedule':{'quartz_cron_expression':'0 0 1 * * ?','timezone_id':'UTC','pause_status':'PAUSED'},
            'max_concurrent_runs':1,
            'tasks':[{'task_key':'harmless_fixture','notebook_task':{'notebook_path':notebook_path,'source':'WORKSPACE'}}]}
    candidate=build_serverless_clone(source)
    candidate['tags']['smct_role']='clone'
    if not apply:
        return {'dry_run':True,'source':source,'candidate':candidate,'jobs_started_by_demo':0,'source_compute':'serverless','classic_migration_claimed':False}

    def existing(name,role):
        matches=[j for j in w.jobs.list() if serial(j.settings).get('name')==name]
        if len(matches)>1: raise ValueError(f'Multiple fixture jobs named {name}; reconcile manually')
        if not matches: return None
        full=w.jobs.get(matches[0].job_id)
        settings=serial(full.settings)
        tags=settings.get('tags') or {}
        if tags.get('smct_fixture')!=FIXTURE_TAG or tags.get('smct_role')!=role:
            raise ValueError('Name conflict with a job not owned by this fixture; no changes made')
        return full

    def create(body):
        response=w.api_client.do('POST','/api/2.1/jobs/create',body=copy.deepcopy(body))
        if not response.get('job_id'): raise RuntimeError('No job ID returned; reconcile workspace before retry')
        return w.jobs.get(int(response['job_id']))

    original=existing(SOURCE_NAME,'source')
    if original is None: original=create(source)
    source_id=int(original.job_id)
    before=serial(original.settings)
    if (before.get('schedule') or {}).get('pause_status')!='PAUSED' or before.get('trigger') or before.get('continuous') or before.get('triggers'):
        raise ValueError('Fixture source is not safely paused; refuse to proceed')
    tasks=before.get('tasks') or []
    if len(tasks)!=1 or tasks[0].get('notebook_task',{}).get('notebook_path')!=notebook_path or any(tasks[0].get(k) for k in COMPUTE_FIELDS):
        raise ValueError('Existing fixture configuration differs; review manually')
    source_digest=digest(before)
    # Required durable backup occurs before any clone API call.
    persist_backup({'source_job_id':source_id,'source_digest':source_digest,'settings':before,'synthetic_fixture':True})
    clone=existing(candidate['name'],'clone')
    if clone is None: clone=create(candidate)
    clone_id=int(clone.job_id)
    clone_settings=serial(clone.settings)
    clone_has_triggers=any(clone_settings.get(k) for k in TRIGGERS)
    clone_tasks=clone_settings.get('tasks') or []
    target_matches=len(clone_tasks)==1 and clone_tasks[0].get('notebook_task',{}).get('notebook_path')==notebook_path and not any(clone_tasks[0].get(k) for k in COMPUTE_FIELDS)
    source_after=serial(w.jobs.get(source_id).settings)
    report={'dry_run':False,'synthetic_fixture':True,'source_compute':'serverless','classic_migration_claimed':False,
            'source_job_id':source_id,'clone_job_id':clone_id,'source_digest':source_digest,
            'original_unchanged':source_digest==digest(source_after),'source_schedule_paused':(source_after.get('schedule') or {}).get('pause_status')=='PAUSED',
            'clone_has_triggers':clone_has_triggers,'clone_target_matches':target_matches,'backup_persisted':True,
            'jobs_started_by_demo':0,'approval':'explicit apply=true for synthetic fixture only','cutover_allowed':False}
    report['passed']=report['original_unchanged'] and report['source_schedule_paused'] and not clone_has_triggers and target_matches
    if not report['passed']: raise RuntimeError(f'Fixture verification failed; no run or promotion attempted. Inspect jobs {source_id} and {clone_id}')
    return report
