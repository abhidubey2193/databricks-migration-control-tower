"""Opt-in read-only notebook content export and code scanning."""
import base64
from .assessment import check_code, score

def scan_notebook(w, workload):
    if workload.get('kind')!='NOTEBOOK': raise ValueError('Expected notebook')
    path=workload.get('source_path')
    try:
        from databricks.sdk.service.workspace import ExportFormat
        response=w.workspace.export(path,format=ExportFormat.SOURCE)
        raw=getattr(response,'content',None)
        if not raw: raise ValueError('Notebook has no source content')
        code=base64.b64decode(raw).decode('utf-8')
        if len(code)>2_000_000: raise ValueError('Notebook exceeds scan cap')
        language=workload.get('attributes',{}).get('language','PYTHON')
        findings=check_code(code,language)
        readiness,eligible=score(findings)
        # Never emit notebook source into inventory.
        return {'findings':[x.__dict__ for x in findings], 'readiness':readiness,'eligible':eligible,'code_scanned':True}
    except Exception as exc:
        return {'findings':[{'limitation':'L1','severity':'high','detail':f'Code scan unavailable: {type(exc).__name__}','remediation':'Review notebook permissions or export restrictions'}], 'readiness':0,'eligible':False,'code_scanned':False}
