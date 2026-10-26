import json
import sqlite3
from types import SimpleNamespace
import pytest
from smct.store import Store
from smct.orchestration import build_serverless_clone,clone_job,cutover_report
from smct.ab import safe_table,warehouse_compare

JOB={'name':'test job','schedule':{'quartz_cron_expression':'0 0 1 * * ?'},'tasks':[{'task_key':'t','existing_cluster_id':'abc','notebook_task':{'notebook_path':'/Users/sandbox/clean'}}]}

def test_candidate_removes_compute_and_triggers():
    v=build_serverless_clone(JOB)
    assert 'schedule' not in v and 'trigger' not in v and 'continuous' not in v
    assert v['tasks'][0]['environment_key']=='smct_env'
    assert 'existing_cluster_id' not in v['tasks'][0]
    assert JOB['tasks'][0]['existing_cluster_id']=='abc'

def test_candidate_rejects_unsupported():
    with pytest.raises(ValueError): build_serverless_clone({'tasks':[{'task_key':'x','spark_jar_task':{}}]})
    with pytest.raises(ValueError): build_serverless_clone({'tasks':[dict(JOB['tasks'][0],libraries=[{'jar':'s3://x'}])]})
    with pytest.raises(ValueError): build_serverless_clone({'git_source':{'git_url':'https://x'},'tasks':JOB['tasks']})
    with pytest.raises(ValueError): build_serverless_clone({'tasks':JOB['tasks']*11})

def test_store_approval_inventory_preserved(tmp_path):
    db=Store(tmp_path/'state.sqlite')
    sample={'workload_id':'job:7','name':'x','kind':'JOB','status':'DISCOVERED','readiness':90,'eligible':True,'compute_mode':'classic','findings':[]}
    db.upsert_inventory([sample])
    assert db.approval('job:7') is None
    db.approve('job:7','human@example.com','Sandbox reviewed')
    db.upsert_inventory([sample])
    assert db.get('job:7')['status']=='APPROVED'
    assert db.events('job:7')

def test_cloning_guard_and_no_trigger(tmp_path):
    store=Store(tmp_path/'state.sqlite')
    store.upsert_inventory([{'workload_id':'job:7','name':'x','kind':'JOB','status':'DISCOVERED','readiness':100,'eligible':True,'compute_mode':'classic','findings':[]}])
    calls=[]
    class Api:
        def do(self,method,path,body):
            calls.append((method,path,body))
            return {'job_id':19}
    class Jobs:
        def get(self,job_id):
            return SimpleNamespace(settings=JOB if job_id==7 else calls[0][2])
    w=SimpleNamespace(jobs=Jobs(),api_client=Api())
    with pytest.raises(PermissionError): clone_job(w,store,7)
    store.approve('job:7','platform owner','approved isolated experiment')
    plan=clone_job(w,store,7)
    assert plan['dry_run'] and not calls
    created=clone_job(w,store,7,backup_dir=tmp_path/'backups',apply=True)
    assert created['clone_job_id']==19 and len(calls)==1
    assert 'schedule' not in calls[0][2]
    assert cutover_report(w,store,7)['allowed'] is False
    store.save_validation('job:7',{'passed':True,'classic_rows':2,'serverless_rows':2})
    assert cutover_report(w,store,7)['allowed']

def test_table_validation_rejects_injection_and_large():
    assert safe_table('main.smct.classic')=='`main`.`smct`.`classic`'
    with pytest.raises(ValueError): safe_table('main.foo;drop.table')
    class Cursor:
        description=[('id',)]
        def __init__(self,rows): self.rows=rows
        def __enter__(self): return self
        def __exit__(self,*args): pass
        def execute(self,q): pass
        def fetchall(self): return self.rows
    class Db:
        def __init__(self,all_rows): self.rows=iter(all_rows)
        def cursor(self): return Cursor(next(self.rows))
    assert warehouse_compare(Db([[('a',),('b',)],[('b',),('a',)]]),'c.s.t','c.s.x')['passed']
    with pytest.raises(ValueError): warehouse_compare(Db([[('a',),('b',)],[]]),'c.s.t','c.s.x',max_rows=1)
