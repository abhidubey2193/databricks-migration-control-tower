"""Compare explicit isolated Delta output tables via SQL warehouse. Never launches production workloads."""

import re
from .validation import compare_rows

IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def safe_table(name):
    parts = name.split(".")
    if len(parts) != 3 or not all(IDENT.fullmatch(x) for x in parts):
        raise ValueError("Use plain three-part catalog.schema.table names only")
    return ".".join("`" + x + "`" for x in parts)


def warehouse_compare(
    connection, classic_table, serverless_table, max_rows=10000, ignore_columns=()
):
    if max_rows < 1 or max_rows > 100000:
        raise ValueError("max_rows must be 1..100000")
    if classic_table == serverless_table:
        raise ValueError("Baseline and target must be separate tables")
    output = []
    schemas = []
    for name in (classic_table, serverless_table):
        with connection.cursor() as cur:
            cur.execute(f"SELECT * FROM {safe_table(name)} LIMIT {max_rows + 1}")
            columns = [col[0] for col in cur.description]
            schemas.append([[str(x) for x in col] for col in cur.description])
            rows = cur.fetchall()
        if len(rows) > max_rows:
            raise ValueError(f"{name} exceeds {max_rows} rows; no PASS recorded")
        output.append([dict(zip(columns, row)) for row in rows])
    result = compare_rows(
        output[0],
        output[1],
        ignore_columns,
        classic_schema=schemas[0],
        serverless_schema=schemas[1],
    )
    result.update(
        {
            "classic_table": classic_table,
            "serverless_table": serverless_table,
            "method": "bounded full-table record comparison",
            "max_rows": max_rows,
            "isolated_outputs_required": True,
        }
    )
    return result


def connect_and_compare(host, http_path, classic, serverless, max_rows=10000):
    from databricks import sql
    from databricks.sdk.core import Config

    config = Config()
    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient(config=config)
    with sql.connect(
        server_hostname=host or config.host.replace("https://", ""),
        http_path=http_path,
        credentials_provider=lambda: w.config.authenticate,
    ) as connection:
        return warehouse_compare(connection, classic, serverless, max_rows)


def spark_compare(
    spark,
    classic_table,
    serverless_table,
    *,
    classic_version,
    serverless_version,
    ignore_columns=(),
    key_columns=(),
):
    """Exact distributed EXCEPT ALL comparison of pinned Delta snapshots."""
    from pyspark.sql import functions as F

    if classic_table == serverless_table:
        raise ValueError("Independent output tables required")
    for version in (classic_version, serverless_version):
        if type(version) is not int or version < 0:
            raise ValueError("Nonnegative Delta snapshot versions required")
    left, right = [
        spark.sql(f"SELECT * FROM {safe_table(t)} VERSION AS OF {v}")
        for t, v in (
            (classic_table, classic_version),
            (serverless_table, serverless_version),
        )
    ]
    if set(key_columns) & set(ignore_columns):
        raise ValueError("Keys cannot be ignored")
    for frame in (left, right):
        if len(frame.columns) != len(set(frame.columns)):
            raise ValueError("Duplicate column names are ambiguous")
        if not set(ignore_columns).issubset(frame.columns) or not set(
            key_columns
        ).issubset(frame.columns):
            raise ValueError("Ignored/key column missing from schema")
    left, right = [
        f.drop(*ignore_columns) if ignore_columns else f for f in (left, right)
    ]
    columns_equal = set(left.columns) == set(right.columns)
    if not left.columns or not right.columns:
        raise ValueError("At least one comparable column required")
    if columns_equal:
        left, right = (
            left.select(*sorted(left.columns)),
            right.select(*sorted(right.columns)),
        )
    schema_equal = left.schema.jsonValue() == right.schema.jsonValue()
    counts = [f.count() for f in (left, right)]

    def nulls(frame):
        expressions = [
            F.sum(
                F.when(F.col("`" + c.replace("`", "``") + "`").isNull(), 1).otherwise(0)
            ).alias(c)
            for c in frame.columns
        ]
        return {
            k: int(v or 0) for k, v in frame.agg(*expressions).first().asDict().items()
        }

    missing = left.exceptAll(right).count() if schema_equal else None
    extra = right.exceptAll(left).count() if schema_equal else None
    result = dict(
        passed=schema_equal and missing == 0 and extra == 0,
        classic_rows=counts[0],
        serverless_rows=counts[1],
        columns_equal=columns_equal,
        typed_schema_compared=True,
        schema_equal=schema_equal,
        classic_schema=left.schema.jsonValue(),
        serverless_schema=right.schema.jsonValue(),
        missing_records=missing,
        extra_records=extra,
        classic_null_counts=nulls(left),
        serverless_null_counts=nulls(right),
        ignored_columns=list(ignore_columns),
        key_columns=list(key_columns),
        baseline_table=classic_table,
        baseline_table_version=classic_version,
        serverless_table=serverless_table,
        serverless_table_version=serverless_version,
        method="distributed pinned Delta EXCEPT ALL",
        comparison_version=3,
        thresholds=dict(missing_records=0, extra_records=0),
    )
    if key_columns:
        result["duplicate_key_groups"] = dict(
            zip(
                ("classic", "serverless"),
                [
                    f.groupBy(*key_columns).count().where("count > 1").count()
                    for f in (left, right)
                ],
            )
        )
        if schema_equal:
            result["changed_key_groups"] = (
                left.exceptAll(right)
                .select(*key_columns)
                .union(right.exceptAll(left).select(*key_columns))
                .distinct()
                .count()
            )
    return result
