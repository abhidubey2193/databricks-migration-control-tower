# Databricks notebook source
# MAGIC %md
# MAGIC # Pinned-input A/B fixture
# MAGIC Identical code runs on classic and serverless compute. Generated sandbox inputs only.
# COMMAND ----------
import re, json

for key in ("input_table", "input_version", "output_table"):
    dbutils.widgets.text(key, "")


def table_name(value):
    parts = value.split(".")
    if len(parts) != 3 or any(
        not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", p) for p in parts
    ):
        raise ValueError("Plain three-part table required")
    return ".".join("`" + p + "`" for p in parts)


source = dbutils.widgets.get("input_table")
output = dbutils.widgets.get("output_table")
version = dbutils.widgets.get("input_version")
if (
    not version.isdecimal()
    or not output.split(".")[-1].startswith("smct_")
    or source == output
):
    raise ValueError("Pinned version and isolated SMCT output required")
# CREATE TABLE fails when a previous/concurrent execution already created output.
spark.sql(
    f"CREATE TABLE {table_name(output)} USING DELTA AS SELECT id, amount * 2 AS amount, region FROM {table_name(source)} VERSION AS OF {int(version)}"
)
print(
    json.dumps(
        dict(
            output_table=output,
            input_table=source,
            input_version=int(version),
            rows=spark.table(table_name(output)).count(),
        )
    )
)
