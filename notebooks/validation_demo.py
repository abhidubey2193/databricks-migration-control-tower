# Databricks notebook source
# MAGIC %md
# MAGIC # SMCT serverless validation integration demo
# MAGIC Baseline is imported synthetic data. Target transformation executes on this notebook's compute.
# MAGIC This does not constitute classic compute execution. Uses only generated sample orders.

# COMMAND ----------
import sys
from pathlib import Path
sys.path.insert(0,str(Path.cwd().parent/'src'))
import json
from datetime import datetime, timezone
from smct.ab import safe_table
from smct.validation import compare_rows

for key,default in [('catalog','main'),('schema','serverless_migration')]:
    dbutils.widgets.text(key,default)
catalog=dbutils.widgets.get('catalog')
schema=dbutils.widgets.get('schema')
# Validate identifiers before using them in any statement.
safe_table(f'{catalog}.{schema}.smct_demo_baseline')
spark.sql(f'CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`')
source=spark.createDataFrame([(1,10),(2,20),(2,20)],'id LONG, amount LONG')
baseline=spark.createDataFrame([(1,20),(2,40),(2,40)],'id LONG, amount LONG')
target=source.selectExpr('id','amount * 2 AS amount')
wrong=target.selectExpr('id','amount + 1 AS amount')
stamp=datetime.now(timezone.utc).isoformat()
# Append-only tables retain per-run evidence and avoid overwriting prior results.
for name,frame in [('smct_demo_baseline',baseline),('smct_demo_target',target),('smct_demo_wrong',wrong)]:
    from pyspark.sql.functions import lit
    frame.withColumn('smct_demo_run',lit(stamp)).write.format('delta').mode('append').saveAsTable(f'`{catalog}`.`{schema}`.`{name}`')
reports=[]
for scenario,frame,expected in [('PASS',target,True),('FAIL',wrong,False)]:
    result=compare_rows([r.asDict() for r in baseline.collect()],[r.asDict() for r in frame.collect()],classic_schema=baseline.schema.jsonValue(),serverless_schema=frame.schema.jsonValue(),key_columns=['id'])
    result.update(baseline_source='imported_synthetic',target_source='executed_notebook_transformation',validated_at=stamp,scenario=scenario)
    assert result['passed'] is expected, result
    reports.append((stamp,scenario,json.dumps(result)))
spark.createDataFrame(reports,'validated_at STRING, scenario STRING, report_json STRING').write.format('delta').mode('append').saveAsTable(f'`{catalog}`.`{schema}`.`validation_demo_evidence`')
print(json.dumps([json.loads(r[2]) for r in reports],indent=2))
