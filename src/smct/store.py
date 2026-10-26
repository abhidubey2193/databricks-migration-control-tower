"""Local durable control-plane state. No secrets or source code are persisted."""
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

SCHEMA = '''
CREATE TABLE IF NOT EXISTS workloads(workload_id TEXT PRIMARY KEY, name TEXT NOT NULL, kind TEXT NOT NULL, status TEXT NOT NULL, readiness INTEGER NOT NULL, payload TEXT NOT NULL, updated_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS events(id INTEGER PRIMARY KEY AUTOINCREMENT, workload_id TEXT NOT NULL, event_type TEXT NOT NULL, details TEXT NOT NULL, created_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS approvals(workload_id TEXT PRIMARY KEY, approver TEXT NOT NULL, approved_at TEXT NOT NULL, rationale TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS migrations(workload_id TEXT PRIMARY KEY, original_job_id INTEGER NOT NULL, clone_job_id INTEGER NOT NULL, backup_path TEXT NOT NULL, source_digest TEXT NOT NULL, status TEXT NOT NULL, created_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS validations(workload_id TEXT PRIMARY KEY, passed INTEGER NOT NULL, report TEXT NOT NULL, created_at TEXT NOT NULL);
'''

def now():
    return datetime.now(timezone.utc).isoformat()

class Store:
    def __init__(self, path='smct.sqlite'):
        self.path = str(path)
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as db:
            db.executescript(SCHEMA)

    def connect(self):
        return sqlite3.connect(self.path)

    def event(self, wid, event_type, details):
        with self.connect() as db:
            db.execute('INSERT INTO events(workload_id,event_type,details,created_at) VALUES(?,?,?,?)', (wid,event_type,json.dumps(details,default=str),now()))

    def upsert_inventory(self, workloads):
        with self.connect() as db:
            for item in workloads:
                w = item.to_dict() if hasattr(item,'to_dict') else item
                prior = db.execute('SELECT status FROM workloads WHERE workload_id=?',(w['workload_id'],)).fetchone()
                status = prior[0] if prior else w.get('status','DISCOVERED')
                db.execute('INSERT INTO workloads VALUES(?,?,?,?,?,?,?) ON CONFLICT(workload_id) DO UPDATE SET name=excluded.name,kind=excluded.kind,readiness=excluded.readiness,payload=excluded.payload,updated_at=excluded.updated_at', (w['workload_id'], w['name'], w['kind'], status, w['readiness'], json.dumps(w,default=str),now()))

    def workloads(self):
        with self.connect() as db:
            rows=db.execute('SELECT payload,status FROM workloads ORDER BY kind,name').fetchall()
        return [dict(json.loads(raw),status=status) for raw,status in rows]

    def get(self,wid):
        with self.connect() as db:
            row=db.execute('SELECT payload,status FROM workloads WHERE workload_id=?',(wid,)).fetchone()
        if row is None: raise ValueError(f'Unknown workload {wid}')
        return dict(json.loads(row[0]),status=row[1])

    def set_status(self,wid,status,details=None):
        permitted={'DISCOVERED','BLOCKED','READY','APPROVED','CLONED','VALIDATED','FAILED','MIGRATED','ROLLED_BACK'}
        if status not in permitted: raise ValueError('Invalid migration status')
        with self.connect() as db:
            if not db.execute('SELECT 1 FROM workloads WHERE workload_id=?',(wid,)).fetchone(): raise ValueError('Unknown workload')
            db.execute('UPDATE workloads SET status=?,updated_at=? WHERE workload_id=?',(status,now(),wid))
        self.event(wid,'STATUS_'+status,details or {})

    def approve(self,wid,actor,rationale):
        if not actor.strip() or not rationale.strip(): raise ValueError('Approver and reason required')
        self.get(wid)
        with self.connect() as db:
            db.execute('INSERT OR REPLACE INTO approvals VALUES(?,?,?,?)',(wid,actor,now(),rationale))
        self.event(wid,'APPROVED',{'approver':actor,'rationale':rationale})
        self.set_status(wid,'APPROVED')

    def approval(self,wid):
        with self.connect() as db:
            row=db.execute('SELECT approver,approved_at,rationale FROM approvals WHERE workload_id=?',(wid,)).fetchone()
        return dict(zip(('approver','approved_at','rationale'),row)) if row else None

    def migration(self,wid):
        with self.connect() as db:
            row=db.execute('SELECT original_job_id,clone_job_id,backup_path,source_digest,status FROM migrations WHERE workload_id=?',(wid,)).fetchone()
        return dict(zip(('original_job_id','clone_job_id','backup_path','source_digest','status'),row)) if row else None

    def save_migration(self,wid,original,clone,backup,digest):
        with self.connect() as db:
            db.execute('INSERT INTO migrations VALUES(?,?,?,?,?,?,?)',(wid,original,clone,backup,digest,'CLONED',now()))
        self.event(wid,'CLONE_CREATED',{'original_job_id':original,'clone_job_id':clone,'backup_path':backup})
        self.set_status(wid,'CLONED')

    def save_validation(self,wid,report):
        with self.connect() as db:
            db.execute('INSERT OR REPLACE INTO validations VALUES(?,?,?,?)',(wid,int(bool(report['passed'])),json.dumps(report,default=str),now()))
        self.event(wid,'VALIDATION',report)
        self.set_status(wid,'VALIDATED' if report['passed'] else 'FAILED')

    def validation(self,wid):
        with self.connect() as db:
            row=db.execute('SELECT report FROM validations WHERE workload_id=?',(wid,)).fetchone()
        return json.loads(row[0]) if row else None

    def events(self,wid=None):
        with self.connect() as db:
            if wid: rows=db.execute('SELECT workload_id,event_type,details,created_at FROM events WHERE workload_id=? ORDER BY id DESC',(wid,)).fetchall()
            else: rows=db.execute('SELECT workload_id,event_type,details,created_at FROM events ORDER BY id DESC LIMIT 500').fetchall()
        return [dict(workload_id=r[0],event_type=r[1],details=json.loads(r[2]),created_at=r[3]) for r in rows]
