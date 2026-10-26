# Databricks notebook source
# MAGIC %md
# MAGIC # SMCT - Sandbox Workspace Fleet Snapshot
# MAGIC Read-only discovery, writes a governed inventory snapshot to a dedicated schema.

# COMMAND ----------
# Databricks notebook source
import json
from datetime import datetime, timezone
from databricks.sdk import WorkspaceClient
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,BooleanType

# COMMAND ----------
dbutils.widgets.text('catalog','main')
dbutils.widgets.text('schema','serverless_migration')
catalog=dbutils.widgets.get('catalog')
schema=dbutils.widgets.get('schema')
import re
assert re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*',catalog)
assert re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*',schema)
workspace=WorkspaceClient()
rows=[]
for job in workspace.jobs.list():
    try:
        full=workspace.jobs.get(job.job_id)
        settings=full.settings.as_dict() if full.settings else {}
        tasks=settings.get('tasks') or []
        classic=any(t.get('existing_cluster_id') or t.get('new_cluster') or t.get('job_cluster_key') for t in tasks)
        if not classic: continue
        blocked=len(tasks)>10 or any(t.get('libraries') or t.get('spark_jar_task') for t in tasks)
        rows.append((f'job:{job.job_id}',settings.get('name') or str(job.job_id),'JOB',len(tasks),not blocked,'BLOCKED' if blocked else 'DISCOVERED',datetime.now(timezone.utc).isoformat()))
    except Exception as exc:
        print('WARN: unable to inspect job',job.job_id,type(exc).__name__)
for cluster in workspace.clusters.list():
    rows.append((f'cluster:{cluster.cluster_id}',cluster.cluster_name or cluster.cluster_id,'CLUSTER',0,False,'DISCOVERED',datetime.now(timezone.utc).isoformat()))

# COMMAND ----------
spark.sql(f'CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`')
schema_def=StructType([StructField('workload_id',StringType(),False),StructField('name',StringType()),StructField('kind',StringType()),StructField('task_count',IntegerType()),StructField('initially_eligible',BooleanType()),StructField('status',StringType()),StructField('scanned_at',StringType())])
frame=spark.createDataFrame(rows,schema_def)
frame.write.format('delta').mode('append').saveAsTable(f'`{catalog}`.`{schema}`.`fleet_snapshots`')
print('Saved classic job and cluster snapshot:',len(rows),'items')
