"""SMCT command line. Destructive actions are deliberately not provided."""
import argparse
import json
from pathlib import Path
from .discovery import inventory
from .store import Store

def main():
    p=argparse.ArgumentParser(description='Databricks serverless migration control tower (sandbox only)')
    p.add_argument('--db',default='smct.sqlite',help='Local SQLite state path')
    sub=p.add_subparsers(dest='command',required=True)
    scan=sub.add_parser('scan',help='Read-only workspace inventory')
    scan.add_argument('--output',default='inventory.json')
    scan.add_argument('--no-notebooks',action='store_true')
    scan.add_argument('--root',default='/Workspace')
    scan.add_argument('--max-notebooks',type=int,default=500)
    sub.add_parser('list',help='List persisted workload status')
    approval=sub.add_parser('approve',help='Record authorized sandbox approval')
    approval.add_argument('job_id',type=int)
    approval.add_argument('--actor',required=True)
    approval.add_argument('--reason',required=True)
    clone=sub.add_parser('clone',help='Build a no-trigger validation clone')
    clone.add_argument('job_id',type=int)
    clone.add_argument('--apply',action='store_true',help='Actually create a disabled-trigger clone; default dry-run')
    clone.add_argument('--backup-dir',default='backups')
    verify=sub.add_parser('validate',help='Compare isolated classic/serverless output tables')
    verify.add_argument('job_id',type=int)
    verify.add_argument('--classic-table',required=True)
    verify.add_argument('--serverless-table',required=True)
    verify.add_argument('--http-path',required=True,help='SQL warehouse HTTP path')
    verify.add_argument('--host',default=None)
    verify.add_argument('--max-rows',type=int,default=10000)
    gate=sub.add_parser('gate',help='Check safe cutover evidence; read-only')
    gate.add_argument('job_id',type=int)
    sub.add_parser('audit',help='Show recorded audit trail')
    args=p.parse_args()
    db=Store(args.db)
    if args.command=='scan':
        workloads,errors=inventory(include_notebooks=not args.no_notebooks,root=args.root,max_notebooks=args.max_notebooks)
        db.upsert_inventory(workloads)
        payload={'metadata':{'read_only':True,'complete':not bool(errors),'errors':errors},'workloads':[w.to_dict() for w in workloads]}
        Path(args.output).write_text(json.dumps(payload,indent=2,default=str),encoding='utf8')
        print(json.dumps({'workloads':len(workloads),'warnings':errors,'output':args.output},indent=2))
    elif args.command=='list':
        print(json.dumps([{'id':w['workload_id'],'name':w['name'],'score':w['readiness'],'status':w['status']} for w in db.workloads()],indent=2))
    elif args.command=='approve':
        db.approve(f'job:{args.job_id}',args.actor,args.reason)
        print('Approval recorded. This does not migrate or execute a workload.')
    elif args.command in ('clone','gate'):
        from databricks.sdk import WorkspaceClient
        from .orchestration import clone_job,cutover_report
        w=WorkspaceClient()
        if args.command=='clone': result=clone_job(w,db,args.job_id,backup_dir=args.backup_dir,apply=args.apply)
        else: result=cutover_report(w,db,args.job_id)
        print(json.dumps(result,indent=2,default=str))
    elif args.command=='validate':
        from .ab import connect_and_compare
        result=connect_and_compare(args.host,args.http_path,args.classic_table,args.serverless_table,args.max_rows)
        db.save_validation(f'job:{args.job_id}',result)
        print(json.dumps(result,indent=2,default=str))
    elif args.command=='audit': print(json.dumps(db.events(),indent=2))

if __name__=='__main__': main()
