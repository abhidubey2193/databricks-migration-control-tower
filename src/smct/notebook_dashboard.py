"""Read-only HTML view of persisted inventory and validation reports."""
from html import escape
from .demo import plan_waves


def _text(value):
    return escape(str(value), quote=True)


def _table(headers, rows):
    body=''.join('<tr>'+''.join('<td>'+_text(value)+'</td>' for value in row)+'</tr>' for row in rows)
    if not body: body=f'<tr><td colspan="{len(headers)}">No records in this snapshot.</td></tr>'
    return '<div class="scroll"><table><thead><tr>'+''.join('<th>'+_text(h)+'</th>' for h in headers)+'</tr></thead><tbody>'+body+'</tbody></table></div>'


def render_dashboard(payload, reports, *, scanned_at='', object_type='All', minimum_score=0, safety_reports=()):
    workloads=payload.get('workloads',[])
    meta=payload.get('metadata',{})
    classic=[w for w in workloads if w.get('kind')=='JOB' and w.get('compute_mode')=='classic']
    filtered=[w for w in workloads if (object_type=='All' or w.get('kind')==object_type) and w.get('readiness',0)>=minimum_score]
    findings=[(w.get('name',''), f) for w in workloads for f in w.get('findings',[])]
    synthetic=meta.get('synthetic',False) or any(w.get('attributes',{}).get('synthetic') for w in workloads)
    cards=[('Inventory objects',len(workloads)),('Classic jobs',len(classic)),('Metadata eligible',sum(bool(w.get('eligible')) for w in classic)),('Blocked jobs',sum(not w.get('eligible') for w in classic)),('Validation PASS',sum(r.get('passed') is True for r in reports)),('Validation FAIL',sum(r.get('passed') is False for r in reports))]
    style='''<style>
    *{box-sizing:border-box}body{margin:0;padding:30px;background:#f3f5f9;color:#162334;font:14px system-ui,sans-serif}main{max-width:1500px;margin:auto}header{padding:30px;background:#14283f;color:#fff;border-radius:14px}header p{color:#c0d3e5}h1{font-size:30px;margin:0}h2{font-size:20px;margin:0 0 15px}h3{font-size:16px}nav{display:flex;gap:18px;flex-wrap:wrap;margin:20px 0}a{color:#226285}section{background:white;border:1px solid #dde5ed;border-radius:12px;padding:24px;margin:18px 0}.cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:12px;margin-top:20px}.card{background:white;border:1px solid #dde5ed;border-radius:10px;padding:18px}.card b{display:block;font-size:30px;margin-top:8px}.muted{color:#586b7e}.notice{padding:12px 16px;background:#fff5d8;border-left:4px solid #cb9300;margin:15px 0}table{width:100%;border-collapse:collapse;text-align:left}th{background:#f0f5f9;color:#38516b;font-size:12px;text-transform:uppercase}th,td{padding:12px;border-bottom:1px solid #e5ebf0;vertical-align:top}.scroll{overflow:auto}code{background:#edf1f5;padding:2px 5px;border-radius:3px}details{margin:14px 0;border:1px solid #dde5ed;border-radius:8px;padding:16px}summary{cursor:pointer;font-weight:600}.ok{color:#146b4a}.fail{color:#ab2837}footer{color:#586b7e;padding:16px}
    </style>'''
    parts=['<!doctype html><html><head><meta charset="utf-8">'+style+'</head><body><main>', '<header><h1>Serverless Migration Control Tower</h1><p>Fleet readiness · validation evidence · controlled migration</p><span>Inventory snapshot: '+_text(scanned_at or 'timestamp unavailable')+'</span></header>']
    if synthetic: parts.append('<div class="notice">SYNTHETIC INVENTORY — these objects do not establish live workload discovery.</div>')
    if not meta.get('complete',False): parts.append('<div class="notice">PARTIAL INVENTORY: '+_text('; '.join(str(e) for e in meta.get('errors',[])) or 'Completeness not recorded')+'</div>')
    parts.append('<nav>'+''.join(f'<a href="#{anchor}">{label}</a>' for anchor,label in [('overview','Overview'),('fleet','Fleet'),('readiness','Remediation'),('waves','Waves'),('validation','Validation'),('cost','Cost'),('governance','Governance')])+'</nav>')
    parts.append('<div class="cards">'+''.join('<div class="card">'+_text(label)+'<b>'+_text(value)+'</b></div>' for label,value in cards)+'</div>')
    parts.append('<section id="overview"><h2>Overview</h2><p>Inventory complete: <b>'+_text(meta.get('complete','Unknown'))+'</b>. The metrics describe the saved snapshot; rerun fleet_scan to refresh.</p><p class="muted">Metadata eligibility is not runtime compatibility. Validation reports below identify their baseline source. Production migration completion is not established by these snapshots.</p></section>')
    rows=[(w.get('workload_id',''),w.get('name',''),w.get('kind',''),w.get('compute_mode',''),w.get('attributes',{}).get('owner','Not recorded'),w.get('readiness',0),w.get('status','DISCOVERED'),sum(f.get('severity')=='blocker' for f in w.get('findings',[]))) for w in filtered]
    parts.append('<section id="fleet"><h2>Fleet inventory</h2><p class="muted">Showing '+_text(len(filtered))+' of '+_text(len(workloads))+' objects. Filters: '+_text(object_type)+', minimum score '+_text(minimum_score)+'.</p>'+_table(['ID','Name','Type','Compute','Owner','Score','Status','Blockers'],rows)+'</section>')
    parts.append('<section id="readiness"><h2>Readiness and remediation</h2>'+_table(['Workload','Limitation','Severity','Finding','Recommended action'],[(name,f.get('limitation',''),f.get('severity',''),f.get('detail',''),f.get('remediation','')) for name,f in findings])+'</section>')
    parts.append('<section id="waves"><h2>Proposed migration waves</h2><p class="muted">Eligible classic jobs only. Cross-job dependencies and approval must be reviewed before execution.</p>'+_table(['Wave','Order','Workload','Approval'],[(r['wave'],r['order'],r['workload_id'],r['approval_status']) for r in plan_waves([w for w in classic if w.get('eligible')])])+'</section>')
    parts.append('<section id="validation"><h2>Validation evidence</h2><p class="muted">Latest available report rows. PASS does not establish genuine classic execution when the baseline is synthetic.</p>')
    if not reports: parts.append('<p>No accessible validation reports. Run validation_demo first, then refresh this notebook.</p>')
    for r in reports:
        passed=r.get('passed') is True
        parts.append('<details open><summary class="'+('ok' if passed else 'fail')+'">'+('PASS' if passed else 'FAIL')+' · '+_text(r.get('scenario','comparison'))+' · '+_text(r.get('validated_at',''))+'</summary>')
        parts.append('<p>Baseline: <b>'+_text(r.get('baseline_source','Not recorded'))+'</b> · Target: '+_text(r.get('target_source','Not recorded'))+'</p>')
        if r.get('baseline_source')=='imported_synthetic': parts.append('<div class="notice">Imported synthetic baseline. No classic-compute run is claimed.</div>')
        parts.append(_table(['Baseline rows','Target rows','Typed schema','Schema equal','Missing','Extra','Changed key groups'],[(r.get('classic_rows','?'),r.get('serverless_rows','?'),r.get('typed_schema_compared','?'),r.get('schema_equal','?'),r.get('missing_records','?'),r.get('extra_records','?'),r.get('changed_key_groups','?'))]))
        parts.append(_table(['Metric','Baseline','Target'],[('Null counts',r.get('classic_null_counts',{}),r.get('serverless_null_counts',{})),('Duplicate key groups',r.get('duplicate_key_groups',{}).get('classic','?'),r.get('duplicate_key_groups',{}).get('serverless','?'))])+'</details>')
    parts.append('</section><section id="cost"><h2>Cost and DBUs</h2><p>Unavailable: no live before/after billing records are attached to this snapshot. No savings are claimed.</p></section>')
    parts.append('<section id="governance"><h2>Governance and cutover</h2>')
    if safety_reports:
        parts.append('<h3>Live synthetic scheduling-safety evidence</h3><p>The source is already serverless. These fixtures test trigger safety, not classic migration.</p>'+_table(['Source job','Clone job','Source paused','Original unchanged','Clone has triggers','Backup saved','Jobs started by demo','PASS'],[(r.get('source_job_id','?'),r.get('clone_job_id','?'),r.get('source_schedule_paused','?'),r.get('original_unchanged','?'),r.get('clone_has_triggers','?'),r.get('backup_persisted','?'),r.get('jobs_started_by_demo','?'),r.get('passed','?')) for r in safety_reports]))
    else: parts.append('<p>No live scheduling-safety evidence loaded. Run clone_safety_demo if authorized, then refresh.</p>')
    parts.append('<p>Full approvals, migration backups and detailed audit events remain in the local SQLite control plane. Synthetic fixture backups are stored separately in Delta when the live safety demo runs.</p><p><b>Cutover: NOT ESTABLISHED.</b> Synthetic comparisons cannot authorize promotion. This dashboard is read-only and never creates jobs, activates schedules or changes permissions.</p></section><footer>Refresh by rerunning this notebook. Inventory is scoped to the scanning principal’s visibility and configured cap.</footer></main></body></html>')
    return ''.join(parts)
