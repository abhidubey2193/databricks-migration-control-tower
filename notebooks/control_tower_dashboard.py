# Databricks notebook source
# MAGIC %md
# MAGIC # Serverless Migration Control Tower Dashboard
# MAGIC Reads the persisted fleet scan and validation reports. No workload mutations.
# MAGIC Set catalog to auto, then Run all. Use object_type and minimum_score to filter inventory.

# COMMAND ----------
import sys
from pathlib import Path
sys.path.insert(0,str(Path.cwd().parent/'src'))
import json
from smct.destination import resolve_destination
from smct.ab import safe_table
from smct.notebook_dashboard import render_dashboard

for key,default in [('catalog','auto'),('schema','serverless_migration')]:
    dbutils.widgets.text(key,default)
dbutils.widgets.dropdown('object_type','All',['All','JOB','NOTEBOOK','CLUSTER'])
dbutils.widgets.dropdown('minimum_score','0',['0','25','50','75','100'])
catalog,schema=resolve_destination(spark,dbutils.widgets.get('catalog'),dbutils.widgets.get('schema'))
inventory_table=safe_table(f'{catalog}.{schema}.fleet_snapshots')
validation_table=safe_table(f'{catalog}.{schema}.validation_demo_evidence')
rows=spark.sql(f'SELECT scanned_at,payload_json FROM {inventory_table} ORDER BY scanned_at DESC LIMIT 1').collect()
if not rows: raise ValueError('No saved inventory. Run fleet_scan first.')
payload=json.loads(rows[0]['payload_json'])
# Do not suppress permissions, corrupt evidence or query errors as empty results.
validation_rows=spark.sql(f'SELECT report_json FROM {validation_table} ORDER BY validated_at DESC, scenario LIMIT 20').collect()
reports=[json.loads(row['report_json']) for row in validation_rows]
safety_table=safe_table(f'{catalog}.{schema}.clone_safety_demo_evidence')
safety_reports=[]
if spark.catalog.tableExists(safety_table):
    safety_rows=spark.sql(f'SELECT report_json FROM {safety_table} ORDER BY checked_at DESC LIMIT 1').collect()
    safety_reports=[json.loads(row['report_json']) for row in safety_rows]
html=render_dashboard(payload,reports,safety_reports=safety_reports,scanned_at=rows[0]['scanned_at'],object_type=dbutils.widgets.get('object_type'),minimum_score=int(dbutils.widgets.get('minimum_score')))
displayHTML(html)
print(json.dumps({'dashboard':'rendered','inventory_objects':len(payload.get('workloads',[])),'inventory_complete':payload.get('metadata',{}).get('complete',False),'safety_reports':len(safety_reports),'validation_reports':len(reports),'pass_reports':sum(r.get('passed') is True for r in reports),'fail_reports':sum(r.get('passed') is False for r in reports)}))
