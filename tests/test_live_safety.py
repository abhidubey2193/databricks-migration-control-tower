import copy
from types import SimpleNamespace
import pytest
from smct.live_safety import run_safety_demo

class Workspace:
    def __init__(self):
        self.settings={};self.calls=[]
        self.jobs=SimpleNamespace(list=lambda:[SimpleNamespace(job_id=i,settings=copy.deepcopy(s)) for i,s in self.settings.items()],get=lambda i:SimpleNamespace(job_id=i,settings=copy.deepcopy(self.settings[i])))
        self.api_client=SimpleNamespace(do=self.create)
    def create(self,method,path,body):
        assert method=='POST' and path=='/api/2.1/jobs/create'
        self.calls.append((method,path,copy.deepcopy(body)))
        i=len(self.settings)+1;self.settings[i]=copy.deepcopy(body)
        return {'job_id':i}

def test_safety_dry_run_never_mutates():
    w=Workspace()
    report=run_safety_demo(w,'/Workspace/test/safety_fixture',lambda x:pytest.fail('Unexpected backup'))
    assert report['dry_run'] and not w.calls
    assert report['source']['schedule']['pause_status']=='PAUSED'
    assert not report['classic_migration_claimed']

def test_live_safety_persists_backup_before_clone_and_reuses_jobs():
    w=Workspace();backups=[]
    def persist(backup):
        assert len(w.settings) in (1,2)
        backups.append(backup)
    result=run_safety_demo(w,'/Workspace/test/safety_fixture',persist,apply=True)
    assert len(w.calls)==2 and result['passed'] and result['original_unchanged']
    assert result['jobs_started_by_demo']==0 and not result['clone_has_triggers']
    assert len(backups)==1 and backups[0]['source_job_id']==1
    again=run_safety_demo(w,'/Workspace/test/safety_fixture',persist,apply=True)
    assert again['clone_job_id']==2 and len(w.calls)==2

def test_active_source_refused_without_modification():
    w=Workspace();run_safety_demo(w,'/Workspace/test/safety_fixture',lambda x:None,apply=True)
    w.settings[1]['schedule']['pause_status']='UNPAUSED'
    with pytest.raises(ValueError,match='safely paused'):
        run_safety_demo(w,'/Workspace/test/safety_fixture',lambda x:None,apply=True)
    assert len(w.calls)==2

def test_backup_failure_prevents_clone():
    w=Workspace()
    def fail(backup): raise OSError('Storage denied')
    with pytest.raises(OSError): run_safety_demo(w,'/Workspace/test/safety_fixture',fail,apply=True)
    assert len(w.settings)==1 and w.settings[1]['schedule']['pause_status']=='PAUSED'
