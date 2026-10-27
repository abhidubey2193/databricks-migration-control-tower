# Databricks notebook source
# MAGIC %md
# MAGIC # Live L10 trigger safety demonstration
# MAGIC Creates two synthetic serverless job definitions: paused source and no-trigger clone.
# MAGIC Starts neither job. Uses the same clone builder as the migration tool.
# MAGIC This is a scheduling safety test, NOT classic-to-serverless migration.
# MAGIC Set apply=true to create/reuse the fixtures and persist backup/evidence in Delta.

# COMMAND ----------
import sys
from pathlib import Path
sys.path.insert(0,str(Path.cwd().parent/'src'))
import json
from datetime import datetime,timezone
from databricks.sdk import WorkspaceClient
from smct.ab import safe_table
from smct.destination import resolve_destination
from smct.live_safety import run_safety_demo

for key,default in [('catalog','auto'),('schema','serverless_migration')]: dbutils.widgets.text(key,default)
dbutils.widgets.dropdown('apply','false',['false','true'])
catalog,schema=resolve_destination(spark,dbutils.widgets.get('catalog'),dbutils.widgets.get('schema'))
w=WorkspaceClient()
# Resolve the checked-in harmless notebook using supported workspace APIs.
fixture_path=None
for candidate in (str(Path.cwd()/'safety_fixture.py'),str(Path.cwd()/'safety_fixture')):
    try:
        status=w.workspace.get_status(candidate)
        typ=getattr(status.object_type,'value',str(status.object_type))
        if typ=='NOTEBOOK': fixture_path=candidate;break
    except Exception as exc:
        if type(exc).__name__ not in ('NotFound','ResourceDoesNotExist'): raise
if fixture_path is None: raise ValueError('Cannot resolve safety_fixture notebook. Pull the latest branch, then reopen this notebook from its notebooks folder.')
apply=dbutils.widgets.get('apply')=='true'
stamp=datetime.now(timezone.utc).isoformat()
def persist_backup(backup):
    spark.createDataFrame([(stamp,json.dumps(backup))],'created_at STRING, backup_json STRING').write.format('delta').mode('append').saveAsTable(safe_table(f'{catalog}.{schema}.clone_safety_demo_backups'))
if apply: spark.sql(f'CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`')
report=run_safety_demo(w,fixture_path,persist_backup,apply=apply)
report['checked_at']=stamp
if apply:
    spark.createDataFrame([(stamp,json.dumps(report))],'checked_at STRING, report_json STRING').write.format('delta').mode('append').saveAsTable(safe_table(f'{catalog}.{schema}.clone_safety_demo_evidence'))
print(json.dumps(report,indent=2))
