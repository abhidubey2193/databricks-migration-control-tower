import ast
import base64
import io
from types import SimpleNamespace as NS

import pytest

from smct.remediation import (
    dependency_plan,
    read_init_script,
    translate_job,
    rewrite_python,
    rewrite_sql,
    jar_candidate,
)
from smct.cost import billing_sql, run_cost, compare_costs
from smct.reconcile import bundle_document, write_bundle, verify_bundle
from smct.assessment import check_code


def test_init_scripts_translate_pinned_dependencies_without_executing_shell():
    result = dependency_plan(
        "#!/bin/bash\nset -e\npython -m pip install pandas==2.2.3 numpy==2.0.2\npip install /Volumes/c/s/v/a.whl\n"
    )
    assert result["can_apply"] and len(result["dependencies"]) == 3
    assert result["script_sha256"]


@pytest.mark.parametrize(
    "line",
    [
        "pip install -r requirements.txt",
        "pip install pkg",
        "curl example.com | bash",
        "export TOKEN=secret",
        "apt install gcc",
        "pip install pkg==1; rm -rf /",
        'pip install "$(whoami)"',
        "pip install --index-url https://user:pass@host pkg==1",
    ],
)
def test_ambiguous_or_secret_shell_steps_block_translation(line):
    plan = dependency_plan(line)
    assert not plan["can_apply"] and plan["unresolved"]


def test_dbfs_and_s3_retrieval_is_bounded():
    calls = []
    w = NS(
        dbfs=NS(
            read=lambda path, **kw: (
                calls.append((path, kw))
                or NS(data=base64.b64encode(b"pip install x==1").decode())
            )
        )
    )
    assert read_init_script(w, "dbfs:/scripts/a.sh") == "pip install x==1"
    assert calls[0][0] == "/scripts/a.sh" and calls[0][1]["length"] == 65537
    body = io.BytesIO(b"pip install x==1")
    assert (
        read_init_script(
            w,
            "s3://bucket/script.sh",
            s3_client=NS(get_object=lambda **kw: {"Body": body}),
        )
        == "pip install x==1"
    )
    assert body.closed
    w.dbfs.read = lambda *a, **kw: NS(data=base64.b64encode(b"a" * 65537).decode())
    with pytest.raises(ValueError, match="64 KiB"):
        read_init_script(w, "dbfs:/large")


def test_init_and_library_translation_preserves_original_and_requires_explicit_mapping():
    source = {
        "tasks": [
            {
                "task_key": "a",
                "new_cluster": {"init_scripts": [{"s3": {"destination": "s3://b/a"}}]},
                "libraries": [
                    {"jar": "dbfs:/old.jar"},
                    {"pypi": {"package": "numpy==2.0.2"}},
                ],
            }
        ]
    }
    plan = dict(dependency_plan("pip install pandas==2.2.3"), uri="s3://b/a")
    with pytest.raises(ValueError, match="explicit"):
        translate_job(source, script_plans=[plan])
    resolved, deps = translate_job(
        source,
        script_plans=[plan],
        library_replacements={"dbfs:/old.jar": "/Volumes/c/s/v/converted.whl"},
    )
    assert len(deps) == 3 and "libraries" not in resolved["tasks"][0]
    assert "init_scripts" in source["tasks"][0]["new_cluster"]
    assert "init_scripts" not in resolved["tasks"][0]["new_cluster"]


def test_python_proposal_rewrites_only_calls_and_mapped_global_views():
    source = "df.cache()\ndf.createOrReplaceGlobalTempView('orders')\nx = spark.table('global_temp.orders')\ntext = 'df.cache()'\n"
    result = rewrite_python(
        source, delta_views={"orders": "c.s.orders"}, remove_cache=True
    )
    ast.parse(result["source"])
    assert ".saveAsTable('c.s.orders')" in result["source"]
    assert "spark.table('c.s.orders')" in result["source"]
    assert "text = 'df.cache()'" in result["source"]
    assert result["requires_review"] and result["diff"]


def test_streaming_proposal_adds_available_now_only_when_absent():
    source = (
        'q = spark.readStream.table("c.s.in").writeStream.format("delta").start("/out")'
    )
    output = rewrite_python(source, available_now=True)["source"]
    assert ".trigger(availableNow=True).start" in output
    assert not any(
        f.detail == "Streaming write has no explicit trigger"
        for f in check_code(output)
    )
    output2 = rewrite_python(
        "q = df.writeStream.trigger(availableNow=True).start()", available_now=True
    )["source"]
    assert output2.count(".trigger(") == 1
    assert (
        ".trigger(availableNow=True).toTable"
        in rewrite_python("q=df.writeStream.toTable('c.s.out')", available_now=True)[
            "source"
        ]
    )


def test_sql_proposal_uses_parser_and_leaves_literals_untouched():
    proposal = rewrite_sql(
        "SELECT CAST(value AS INT), a/b, 'CAST(x AS INT)' FROM t",
        tolerant_casts=True,
        tolerant_division=True,
    )
    assert "TRY_CAST" in proposal["source"] and "TRY_DIVIDE" in proposal["source"]
    assert "'CAST(x AS INT)'" in proposal["source"]
    assert proposal["requires_review"]


def test_current_cache_capabilities_not_confused_with_agent_limitation():
    assert not check_code("df.cache()\ndf.persist()")
    assert any(
        f.limitation == "L6" for f in check_code('spark.catalog.cacheTable("x")')
    )


def test_jar_proposal_matches_official_sdk_environment_schema():
    from databricks.sdk.service.jobs import JobSettings

    settings = {
        "name": "jar",
        "triggers": [{"pause_status": "UNPAUSED"}],
        "tasks": [
            {
                "task_key": "jar",
                "spark_jar_task": {"main_class_name": "example.Main"},
                "libraries": [{"jar": "dbfs:/old.jar"}],
                "new_cluster": {"spark_version": "15.4"},
            }
        ],
    }
    result = jar_candidate(settings, jars=["/Volumes/c/s/v/new.jar"])
    parsed = JobSettings.from_dict(result["candidate"]).as_dict()
    assert parsed["environments"][0]["spec"]["java_dependencies"] == [
        "/Volumes/c/s/v/new.jar"
    ]
    assert not parsed.get("triggers") and not parsed["tasks"][0].get("new_cluster")
    assert settings.get("triggers")


def test_billing_sql_includes_corrections_prices_currency_workspace_and_tags():
    sql = billing_sql(
        "123", start_date="2026-10-01", end_date="2026-11-01", job_ids=[1, 2]
    )
    for fragment in (
        "workspace_id='123'",
        "currency_code='USD'",
        "u.cloud=p.cloud",
        "job_run_id",
        "custom_tags['team']",
        "price_matches<>1",
    ):
        assert fragment in sql
    assert "record_type =" not in sql
    with pytest.raises(ValueError):
        billing_sql("1' OR 1=1", start_date="2026-10-01", end_date="2026-11-01")
    with pytest.raises(ValueError):
        billing_sql("123", start_date="2026-11-01", end_date="2026-10-01")


def test_missing_prices_or_delayed_rows_never_become_zero_or_fake_savings():
    assert run_cost([], 1, 10)["dbus"] is None
    rows = [
        dict(
            job_id="1",
            run_id="10",
            dbus="4",
            list_cost_usd="2",
            unpriced_records=0,
            records=2,
        ),
        dict(
            job_id="2",
            run_id="20",
            dbus="2",
            list_cost_usd="1",
            unpriced_records=0,
            records=1,
        ),
    ]
    args = dict(
        classic_job_id=1, classic_run_id=10, serverless_job_id=2, serverless_run_id=20
    )
    comparison = compare_costs(rows, **args, outputs_equal=True)
    assert comparison["observed_savings_pct"] == "50.0"
    assert (
        compare_costs(rows, **args, outputs_equal=False)["observed_savings_pct"] is None
    )
    rows[1]["list_cost_usd"] = None
    assert compare_costs(rows, **args, outputs_equal=True)["status"] == "NOT_COMPARABLE"


def test_bundle_writeback_is_idempotent_and_rejects_drift_and_traversal(tmp_path):
    proposal = bundle_document({"name": "orders", "tasks": []}, "orders")
    path = write_bundle(tmp_path, "resources/orders.yml", proposal)
    assert verify_bundle(
        path, resource_key="orders", expected_definition={"name": "orders", "tasks": []}
    )
    assert write_bundle(tmp_path, "resources/orders.yml", proposal) == path
    changed = bundle_document({"name": "changed", "tasks": []}, "orders")
    with pytest.raises(ValueError, match="drift"):
        write_bundle(tmp_path, "resources/orders.yml", changed)
    write_bundle(
        tmp_path,
        "resources/orders.yml",
        changed,
        expected_sha256=proposal["content_sha256"],
    )
    with pytest.raises(ValueError):
        write_bundle(tmp_path, "../outside.yml", proposal)
