# Databricks notebook source
# MAGIC %md
# MAGIC # SMCT workspace inventory
# MAGIC Uses the same assessment engine as the CLI. No jobs are changed or executed.

# COMMAND ----------
import sys
from pathlib import Path
sys.path.insert(0, str(Path.cwd().parent / 'src'))
import json
import re
from datetime import datetime, timezone
from smct.discovery import inventory
from databricks.sdk import WorkspaceClient
from pyspark.sql.types import StructType, StructField, StringType

from smct.destination import resolve_destination

for key,default in [('catalog','auto'),('schema','serverless_migration')]:
    dbutils.widgets.text(key,default)
catalog,schema=resolve_destination(spark,dbutils.widgets.get('catalog'),dbutils.widgets.get('schema'))
print(f'Evidence destination: {catalog}.{schema}')
if not all(re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*',x) for x in (catalog,schema)):
    raise ValueError('Invalid destination identifier')
workloads,errors=inventory(WorkspaceClient(),root='/',max_notebooks=500)
payload={'metadata':{'read_only':True,'complete':not errors,'errors':errors,'synthetic':False},'workloads':[w.to_dict() for w in workloads]}
scanned_at=datetime.now(timezone.utc).isoformat()
spark.sql(f'CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`')
frame=spark.createDataFrame([(scanned_at,json.dumps(payload))],StructType([StructField('scanned_at',StringType()),StructField('payload_json',StringType())]))
frame.write.format('delta').mode('append').saveAsTable(f'`{catalog}`.`{schema}`.`fleet_snapshots`')
print(json.dumps({'objects':len(workloads),'complete':not errors,'warnings':errors}))
