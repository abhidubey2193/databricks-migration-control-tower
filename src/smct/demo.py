"""Reproducible synthetic exercise of the same scanner, store and clone guards."""
import copy
import json
from pathlib import Path
from types import SimpleNamespace
from .discovery import build_job
from .orchestration import clone_job, cutover_report, digest
from .assessment import check_code
from .validation import compare_rows
from .store import Store, now


def fixtures():
    ready={'name':'SYNTHETIC ready orders','tasks':[{'task_key':'orders','new_cluster':{'spark_version':'15.4.x-scala2.12'},'notebook_task':{'notebook_path':'/Workspace/Shared/smct-demo/orders'}}]}
    blocked=copy.deepcopy(ready)
    blocked['name']='SYNTHETIC blocked container'
    blocked['tasks'][0]['new_cluster'].update({'docker_image':{'url':'example.invalid/container'},'init_scripts':[{'dbfs':{'destination':'dbfs:/synthetic.sh'}}]})
    large=copy.deepcopy(ready)
    large['name']='SYNTHETIC 11-task dependency chain'
    large['tasks']=[dict(copy.deepcopy(ready['tasks'][0]),task_key=f't{i}',**({'depends_on':[{'task_key':f't{i-1}'}]} if i else {})) for i in range(11)]
    scheduled=copy.deepcopy(ready)
    scheduled.update(name='SYNTHETIC scheduled orders',schedule={'quartz_cron_expression':'0 0 1 * * ?','timezone_id':'UTC','pause_status':'UNPAUSED'})
    return {101:ready,102:blocked,103:large,104:scheduled}


def plan_waves(workloads, size=5):
    if not 1<=size<=100: raise ValueError('Wave size must be 1..100')
    candidates=sorted((w for w in workloads if w['kind']=='JOB' and w['compute_mode']=='classic' and w['eligible']),key=lambda w:(-w['readiness'],w['workload_id']))
    return [{'wave':i//size+1,'workload_id':w['workload_id'],'order':i+1,'approval_status':'APPROVED' if w['status']=='APPROVED' else 'PENDING','dependency_review':'Required: cross-job dependencies are not inferred'} for i,w in enumerate(candidates)]


def run_demo(output):
    out=Path(output).resolve()
    out.mkdir(parents=True, exist_ok=True)
    db_path=out/'smct.sqlite'
    if db_path.exists(): raise ValueError('Use a fresh output directory; demo never overwrites existing state')
    db=Store(db_path)
    jobs=fixtures()
    workloads=[build_job(i,j) for i,j in jobs.items()]
    for w in workloads:
        w.attributes['synthetic']=True
        w.status='READY' if w.eligible else 'BLOCKED'
    db.upsert_inventory(workloads)
    db.approve('job:104','SYNTHETIC platform owner','Synthetic isolated-clone demonstration only')
    created={}
    class Api:
        def do(self, method, path, body):
            assert method=='POST' and path=='/api/2.1/jobs/create'
            created[904]=copy.deepcopy(body)
            return {'job_id':904}
    client=SimpleNamespace(api_client=Api(),jobs=SimpleNamespace(get=lambda i:SimpleNamespace(settings=copy.deepcopy(jobs.get(i) or created[i]))))
    original_digest=digest(jobs[104])
    clone_job(client,db,104,backup_dir=out/'backups',apply=True)
    baseline=[{'order_id':1,'amount':12,'region':'SE'},{'order_id':2,'amount':None,'region':'SE'},{'order_id':2,'amount':None,'region':'SE'}]
    equivalent=list(reversed(baseline))
    different=copy.deepcopy(equivalent)
    different[0]['amount']=99
    schema=[['order_id','LONG'],['amount','LONG'],['region','STRING']]
    reports={}
    for label,target,wid in [('PASS',equivalent,'job:104'),('FAIL',different,'job:101')]:
        report=compare_rows(baseline,target,classic_schema=schema,serverless_schema=schema,key_columns=['order_id'])
        report.update(baseline_source='imported_synthetic',execution_mode='synthetic_local',classic_run_id=None,serverless_run_id=None,validated_at=now())
        db.save_validation(wid,report)
        reports[label]=report
    result={'synthetic':True,'live_workspace_tested':False,'generated_at':now(),
            'workloads':db.workloads(),'waves':plan_waves(db.workloads()),'validation':reports,
            'safety':{'original_unchanged':digest(jobs[104])==original_digest,'clone_has_triggers':any(k in created[904] for k in ('schedule','continuous','trigger')),'cutover':cutover_report(client,db,104)},
            'L13':[f.__dict__ for f in check_code("spark.sql('SELECT CAST(raw AS INT), a / b FROM src')\ndf.writeStream.toTable('target')")],
            'cost':{'status':'UNAVAILABLE','reason':'No live billing records; no savings claimed'}}
    for name,value in {'workloads.json':jobs,'classic-baseline.json':baseline,'serverless-pass.json':equivalent,'serverless-fail.json':different,'clone.json':created[904],'evidence.json':result}.items():
        (out/name).write_text(json.dumps(value,indent=2),encoding='utf8')
    return {'synthetic':True,'database':str(db_path),'evidence':str(out/'evidence.json'),'pass_scenario':reports['PASS']['passed'],'fail_scenario':reports['FAIL']['passed'],'original_unchanged':result['safety']['original_unchanged'],'clone_has_triggers':result['safety']['clone_has_triggers']}
