"""Read-only UI for local/in-app SQLite control plane and uploaded inventory."""
import json
import os
from pathlib import Path
import pandas as pd
import streamlit as st
from smct.store import Store

st.set_page_config(page_title='Serverless Migration Control Tower',page_icon='🛡️',layout='wide')
st.title('🛡️ Serverless Migration Control Tower')
st.caption('Fleet inventory · migration readiness · durable audit · validation evidence')
mode=st.sidebar.radio('Data source',['SQLite control plane','JSON inventory'])
if mode=='SQLite control plane':
    db=Store(os.getenv('SMCT_DB','smct.sqlite'))
    workloads=db.workloads()
else:
    uploaded=st.sidebar.file_uploader('Upload inventory.json',type='json')
    if not uploaded: st.info('Upload a scan result or switch to SQLite to view persisted state.'); st.stop()
    workloads=json.load(uploaded).get('workloads',[])
if not workloads: st.warning('No workloads discovered. Run `smct scan` first.'); st.stop()
records=[{'ID':w['workload_id'],'Name':w['name'],'Type':w['kind'],'Compute':w['compute_mode'],'Score':w['readiness'],'Status':w['status'],'Eligible':w['eligible'],'Blockers':sum(f['severity']=='blocker' for f in w['findings'])} for w in workloads]
df=pd.DataFrame(records)
cols=st.columns(5)
for col,(name,value) in zip(cols,[('Objects',len(df)),('Classic jobs',len(df[(df.Type=='JOB')&(df.Compute=='classic')])),('Eligible',int(df.Eligible.sum())),('Blocked',int((df.Blockers>0).sum())),('Validated',int((df.Status=='VALIDATED').sum()))]): col.metric(name,value)
tabs=st.tabs(['Fleet','Blockers','Migration evidence','Audit & safety'])
with tabs[0]:
    types=st.multiselect('Types',sorted(df.Type.unique()),default=sorted(df.Type.unique()))
    st.dataframe(df[df.Type.isin(types)],hide_index=True,use_container_width=True)
    st.bar_chart(df.groupby('Type')['Score'].mean())
with tabs[1]:
    findings=[{'Workload':w['name'],**f} for w in workloads for f in w['findings']]
    st.dataframe(pd.DataFrame(findings),hide_index=True,use_container_width=True) if findings else st.success('No findings in retrieved metadata; full compatibility is not guaranteed')
with tabs[2]:
    if mode=='SQLite control plane':
        rows=[]
        for w in workloads:
            m=db.migration(w['workload_id'])
            if m:
                v=db.validation(w['workload_id'])
                rows.append({'Workload':w['name'],'Original':m['original_job_id'],'Clone':m['clone_job_id'],'Validation':('PASS' if v['passed'] else 'FAIL') if v else 'PENDING','Backup':m['backup_path']})
        if rows: st.dataframe(pd.DataFrame(rows),hide_index=True,use_container_width=True)
        else: st.info('No migration clones registered. Use `smct approve` and `smct clone --apply` in a sandbox.')
    else: st.info('Migration evidence requires the SQLite control-plane database.')
with tabs[3]:
    if mode=='SQLite control plane':
        events=db.events()
        if events: st.dataframe(pd.DataFrame(events),hide_index=True,use_container_width=True)
        else: st.info('Audit log is empty.')
    else: st.info('Audit events require SQLite control-plane data.')
st.caption('Controls fail closed: no automatic production promotion or workload schedule activation. Workspace testing required.')
