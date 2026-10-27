# Databricks notebook source
# MAGIC %md
# MAGIC # SMCT isolated serverless target
# MAGIC Generated sample orders only. Writes one uniquely named demonstration table.
# COMMAND ----------
import json
import re

for key, default in [
    ("catalog", ""),
    ("schema", "serverless_migration"),
    ("output_table", ""),
]:
    dbutils.widgets.text(key, default)
parts = [dbutils.widgets.get(k) for k in ("catalog", "schema", "output_table")]
if not all(re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", p) for p in parts):
    raise ValueError("Explicit isolated output identifiers required")
if not parts[2].startswith("smct_program_") or not parts[2].endswith("_target"):
    raise ValueError("Only isolated SMCT program target tables are permitted")
source = spark.createDataFrame(
    [(1, 10, "SE"), (2, 20, "SE"), (2, 20, "SE"), (3, None, "SE")],
    "id LONG, amount LONG, region STRING",
)
result = source.selectExpr("id", "amount * 2 AS amount", "region")
# A repeated accidental run cannot overwrite the immutable validation output.
# Plain CREATE TABLE rejects an existing table, including a concurrent creator.
# The DataFrame errorifexists save mode is unsupported on some serverless runtimes.
result.createOrReplaceTempView("smct_target_rows")
try:
    spark.sql(
        "CREATE TABLE " + ".".join("`" + p + "`" for p in parts)
        + " USING DELTA AS SELECT * FROM smct_target_rows"
    )
finally:
    spark.catalog.dropTempView("smct_target_rows")
print(
    json.dumps(
        {
            "output_table": ".".join(parts),
            "rows": result.count(),
            "input_source": "generated_synthetic_orders",
            "execution": "actual_job_notebook",
        }
    )
)
