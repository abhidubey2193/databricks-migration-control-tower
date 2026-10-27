"""Real Spark operations; pinned Delta SQL dispatch is adapted to local temp views."""

import re
import pytest

pytest.importorskip("pyspark")

from smct.ab import spark_compare


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[2]")
        .appName("smct-comparison-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.warehouse.dir", str(tmp_path_factory.mktemp("spark")))
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


class Snapshots:
    def __init__(self, snapshots):
        self.snapshots = snapshots
        self.calls = []

    def sql(self, statement):
        self.calls.append(statement)
        match = re.fullmatch(
            r"SELECT \* FROM `c`\.`s`\.`(\w+)` VERSION AS OF (\d+)", statement
        )
        assert match, statement
        return self.snapshots[(match[1], int(match[2]))]


def test_distributed_multiset_null_duplicate_and_key_checks(spark):
    left = spark.createDataFrame([(1, 10), (1, 10), (2, None)], "id LONG, amount LONG")
    right = spark.createDataFrame([(2, None), (1, 10), (1, 10)], "id LONG, amount LONG")
    adapter = Snapshots({("before", 0): left, ("after", 2): right})
    result = spark_compare(
        adapter,
        "c.s.before",
        "c.s.after",
        classic_version=0,
        serverless_version=2,
        key_columns=["id"],
    )
    assert result["passed"] and result["classic_rows"] == 3
    assert result["classic_null_counts"] == {"amount": 1, "id": 0}
    assert result["duplicate_key_groups"] == {"classic": 1, "serverless": 1}
    assert result["changed_key_groups"] == 0
    changed = spark.createDataFrame(
        [(2, None), (1, 10), (1, 11)], "id LONG, amount LONG"
    )
    adapter.snapshots[("after", 2)] = changed
    result = spark_compare(
        adapter,
        "c.s.before",
        "c.s.after",
        classic_version=0,
        serverless_version=2,
        key_columns=["id"],
    )
    assert (
        not result["passed"]
        and result["missing_records"] == result["extra_records"] == 1
    )
    assert result["changed_key_groups"] == 1


def test_empty_outputs_still_compare_typed_schemas(spark):
    left = spark.createDataFrame([], "id LONG, amount LONG")
    right = spark.createDataFrame([], "id LONG, amount STRING")
    adapter = Snapshots({("before", 0): left, ("after", 0): right})
    result = spark_compare(
        adapter, "c.s.before", "c.s.after", classic_version=0, serverless_version=0
    )
    assert (
        not result["passed"]
        and not result["schema_equal"]
        and result["typed_schema_compared"]
    )
    assert result["missing_records"] is None


def test_ignored_columns_do_not_mask_nonignored_schema_or_key_errors(spark):
    left = spark.createDataFrame([(1, "before")], "id LONG, stamp STRING")
    right = spark.createDataFrame([(1, "after")], "id LONG, stamp STRING")
    adapter = Snapshots({("before", 0): left, ("after", 0): right})
    assert spark_compare(
        adapter,
        "c.s.before",
        "c.s.after",
        classic_version=0,
        serverless_version=0,
        ignore_columns=["stamp"],
    )["passed"]
    with pytest.raises(ValueError, match="Keys cannot"):
        spark_compare(
            adapter,
            "c.s.before",
            "c.s.after",
            classic_version=0,
            serverless_version=0,
            ignore_columns=["id"],
            key_columns=["id"],
        )
