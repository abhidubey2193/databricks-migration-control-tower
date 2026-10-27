from pathlib import Path
import copy
import json
from decimal import Decimal
from types import SimpleNamespace
import pytest
from smct.validation import compare_rows
from smct.orchestration import build_serverless_clone, clone_job, cutover_report
from smct.discovery import build_job, inventory
from smct.assessment import check_code
from smct.demo import run_demo, fixtures, plan_waves
from smct.store import Store
from smct.ab import warehouse_compare

@pytest.mark.parametrize('left,right',[(1,'1'),(1,True),(Decimal('1'),'1'),(1,1.0),({'a':1},'{"a": 1}')])
def test_types_do_not_collapse(left,right):
    assert not compare_rows([{'v':left}],[{'v':right}])['passed']

def test_empty_typed_tables_differ():
    r=compare_rows([],[],classic_schema=[['id','INT']],serverless_schema=[['id','STRING']])
    assert r['typed_schema_compared'] and not r['passed']

def test_null_counts_duplicate_keys():
    r=compare_rows([{'id':1,'v':None}]*2,[{'id':1,'v':None},{'id':1,'v':3}],key_columns=['id'])
    assert r['changed_key_groups']==1 and r['classic_null_counts']['v']==2
    assert r['serverless_null_counts']['v']==1 and r['missing_records']==1
    assert r['duplicate_key_groups']['classic']==1

@pytest.mark.parametrize('v',[float('nan'),float('inf'),object()])
def test_unsupported_values_fail_closed(v):
    with pytest.raises(ValueError): compare_rows([{'v':v}],[{'v':v}])

def test_same_output_table_rejected():
    with pytest.raises(ValueError,match='separate'): warehouse_compare(None,'c.s.t','c.s.t')

@pytest.mark.parametrize('config',[{'init_scripts':[{}]},{'docker_image':{'url':'test'}},{'spark_version':'12.2.x-scala2.12'},{'spark_env_vars':{'X':'Y'}}])
def test_clone_reassesses_config(config):
    j=fixtures()[101]
    j['tasks'][0]['new_cluster'].update(config)
    with pytest.raises(ValueError,match='remediation'): build_serverless_clone(j)

def test_clone_rejects_unknown_task_fields():
    j=fixtures()[101]
    j['tasks'][0]['webhook_notifications']={'on_success':[]}
    with pytest.raises(ValueError,match='attributes'): build_serverless_clone(j)

@pytest.mark.parametrize('trigger',['schedule','continuous','trigger'])
def test_all_trigger_types_removed_without_mutating_source(trigger):
    j=fixtures()[101];j[trigger]={'pause_status':'UNPAUSED'}
    before=copy.deepcopy(j)
    assert trigger not in build_serverless_clone(j)
    assert j==before

def test_demo_proves_pass_fail_and_schedule_safety(tmp_path):
    result=run_demo(tmp_path/'demo')
    assert result['pass_scenario'] and not result['fail_scenario']
    assert result['original_unchanged'] and not result['clone_has_triggers']
    evidence=json.loads((tmp_path/'demo/evidence.json').read_text())
    assert not evidence['safety']['cutover']['allowed']
    assert not evidence['live_workspace_tested']
    assert len(evidence['workloads'])==4
    with pytest.raises(ValueError): run_demo(tmp_path/'demo')

def test_partial_inventory_reports_jobs_permission_error():
    def denied(): raise PermissionError()
    w=SimpleNamespace(jobs=SimpleNamespace(list=denied),clusters=SimpleNamespace(list=lambda:[]))
    workloads,errors=inventory(w,include_notebooks=False)
    assert workloads==[] and errors

def test_mapping_missing_shared_cluster_blocks():
    settings={'name':'shared','tasks':[{'task_key':'t','existing_cluster_id':'missing','notebook_task':{'notebook_path':'/a'}}]}
    w=SimpleNamespace(jobs=SimpleNamespace(list=lambda:[SimpleNamespace(job_id=1)],get=lambda i:SimpleNamespace(settings=settings)),clusters=SimpleNamespace(list=lambda:[]))
    items,errors=inventory(w,include_notebooks=False)
    assert not items[0].eligible

def test_source_drift_and_backup_corruption_block(tmp_path):
    db=Store(tmp_path/'s.sqlite');jobs=fixtures()
    db.upsert_inventory([build_job(101,jobs[101])]);db.approve('job:101','owner','test')
    clones={}
    def create(method,path,body): clones[9]=body;return {'job_id':9}
    w=SimpleNamespace(jobs=SimpleNamespace(get=lambda i:SimpleNamespace(settings=jobs[101] if i==101 else clones[i])),api_client=SimpleNamespace(do=create))
    clone_job(w,db,101,backup_dir=tmp_path/'backup',apply=True)
    backup=db.migration('job:101')['backup_path']
    from pathlib import Path
    Path(backup).write_text('{}')
    jobs[101]['description']='changed'
    report=cutover_report(w,db,101)
    assert not report['allowed'] and not report['original_unchanged'] and not report['backup_integrity']

def test_wave_plan_excludes_blocked():
    ws=[build_job(i,j).to_dict() for i,j in fixtures().items()]
    plan=plan_waves(ws,size=1)
    assert [w['workload_id'] for w in plan]==['job:101','job:104']
    assert [w['wave'] for w in plan]==[1,2]

def test_streaming_missing_trigger_specific_finding():
    assert any('no explicit trigger' in f.detail for f in check_code('df.writeStream.toTable("x")'))
    assert not any('no explicit trigger' in f.detail for f in check_code('df.writeStream.trigger(availableNow=True).toTable("x")'))

def test_sdk_clone_serialization():
    from databricks.sdk.service.jobs import JobSettings
    candidate=build_serverless_clone(fixtures()[101])
    parsed=JobSettings.from_dict(candidate).as_dict()
    assert parsed['tasks']==candidate['tasks']
    assert parsed['environments'][0]['spec']['environment_version']=='2'

def test_dashboard_renders_synthetic_evidence(tmp_path,monkeypatch):
    from streamlit.testing.v1 import AppTest
    run_demo(tmp_path/'demo')
    monkeypatch.setenv('SMCT_DB',str(tmp_path/'demo/smct.sqlite'))
    app=AppTest.from_file(Path(__file__).resolve().parents[1]/'app/dashboard.py').run(timeout=20)
    assert not app.exception
    assert len(app.tabs)==7
    assert any('SYNTHETIC' in warning.value for warning in app.warning)
