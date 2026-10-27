from smct.notebook_dashboard import render_dashboard

def test_persisted_dashboard_escapes_untrusted_names():
    payload={'metadata':{'complete':True},'workloads':[{'workload_id':'notebook:a','name':'<script>alert(1)</script>','kind':'NOTEBOOK','compute_mode':'unknown','readiness':100,'eligible':True,'status':'DISCOVERED','attributes':{},'findings':[]}]}
    html=render_dashboard(payload,[])
    assert '<script>' not in html and '&lt;script&gt;' in html
    assert 'No accessible validation reports' in html
    assert html.count('<section id=')==7

def test_synthetic_reports_cannot_be_displayed_as_live_classic():
    html=render_dashboard({'metadata':{'complete':True},'workloads':[]},[{'passed':True,'baseline_source':'imported_synthetic','classic_rows':3,'serverless_rows':3}])
    assert 'Imported synthetic baseline. No classic-compute run is claimed.' in html
    assert 'Cutover: NOT ESTABLISHED.' in html

def test_dashboard_filter_and_partial_warning():
    payload={'metadata':{'complete':False,'errors':['permission <denied>']},'workloads':[{'name':'NOTEBOOK_ONLY','kind':'NOTEBOOK','readiness':100},{'name':'JOB_ONLY','kind':'JOB','readiness':40}]}
    html=render_dashboard(payload,[],object_type='JOB',minimum_score=50)
    assert 'PARTIAL INVENTORY' in html and 'permission &lt;denied&gt;' in html
    assert 'Showing 0 of 2 objects' in html
    assert 'NOTEBOOK_ONLY' not in html
