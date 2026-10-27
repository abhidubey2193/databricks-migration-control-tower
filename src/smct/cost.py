"""Exact job/run DBUs and USD list cost; signed billing corrections are retained."""

from datetime import date
from decimal import Decimal


def billing_sql(workspace_id, *, start_date, end_date, job_ids=()):
    workspace_id = str(workspace_id)
    if not workspace_id.isdecimal():
        raise ValueError("Numeric workspace ID required")
    start, end = date.fromisoformat(start_date), date.fromisoformat(end_date)
    if start >= end or (end - start).days > 366:
        raise ValueError(
            "Increasing date window of at most 366 days required; end exclusive"
        )
    ids = [str(x) for x in job_ids]
    if any(not x.isdecimal() for x in ids):
        raise ValueError("Numeric job IDs required")
    job_filter = (
        " AND u.usage_metadata.job_id IN (" + ",".join("'" + x + "'" for x in ids) + ")"
        if ids
        else ""
    )
    return f"""WITH records AS (
      SELECT u.record_id,u.workspace_id,u.usage_metadata.job_id AS job_id,
        u.usage_metadata.job_run_id AS run_id,u.custom_tags['team'] AS team,
        u.custom_tags['smct_program'] AS program,u.sku_name,u.usage_unit,u.usage_quantity,
        COUNT(p.sku_name) AS price_matches,MAX(p.pricing.effective_list.default) AS unit_price
      FROM system.billing.usage u LEFT JOIN system.billing.list_prices p
        ON u.sku_name=p.sku_name AND u.cloud=p.cloud AND u.usage_unit=p.usage_unit
        AND p.currency_code='USD' AND u.usage_start_time >= p.price_start_time
        AND (p.price_end_time IS NULL OR u.usage_end_time <= p.price_end_time)
      WHERE u.workspace_id='{workspace_id}' AND u.usage_unit='DBU'
        AND u.usage_date >= DATE '{start}' AND u.usage_date < DATE '{end}' {job_filter}
      GROUP BY u.record_id,u.workspace_id,u.usage_metadata.job_id,u.usage_metadata.job_run_id,
        u.custom_tags['team'],u.custom_tags['smct_program'],u.sku_name,u.usage_unit,u.usage_quantity
    ) SELECT workspace_id,job_id,run_id,team,program,sku_name,usage_unit,
      SUM(usage_quantity) AS dbus,
      CASE WHEN SUM(CASE WHEN price_matches<>1 THEN 1 ELSE 0 END)=0
        THEN SUM(usage_quantity*unit_price) ELSE NULL END AS list_cost_usd,
      SUM(CASE WHEN price_matches<>1 THEN 1 ELSE 0 END) AS unpriced_records,COUNT(*) AS records
    FROM records GROUP BY workspace_id,job_id,run_id,team,program,sku_name,usage_unit"""


def run_cost(rows, job_id, run_id):
    selected = [
        r
        for r in rows
        if str(r.get("job_id")) == str(job_id) and str(r.get("run_id")) == str(run_id)
    ]
    if not selected:
        return dict(status="PENDING_OR_UNATTRIBUTED", dbus=None, list_cost_usd=None)
    dbus = sum(Decimal(str(r["dbus"])) for r in selected)
    priced = all(
        r.get("list_cost_usd") is not None and int(r.get("unpriced_records", 0)) == 0
        for r in selected
    )
    usd = sum(Decimal(str(r["list_cost_usd"])) for r in selected) if priced else None
    return dict(
        status="PRICED" if priced else "PARTIAL_PRICE",
        dbus=str(dbus),
        list_cost_usd=str(usd) if usd is not None else None,
        records=sum(int(r.get("records", 0)) for r in selected),
    )


def compare_costs(
    rows,
    *,
    classic_job_id,
    classic_run_id,
    serverless_job_id,
    serverless_run_id,
    outputs_equal,
):
    classic = run_cost(rows, classic_job_id, classic_run_id)
    target = run_cost(rows, serverless_job_id, serverless_run_id)
    comparable = (
        outputs_equal
        and classic["status"] == target["status"] == "PRICED"
        and Decimal(classic["dbus"]) > 0
        and Decimal(target["dbus"]) > 0
    )
    result = dict(
        status="COMPARABLE" if comparable else "NOT_COMPARABLE",
        classic=classic,
        serverless=target,
        basis="USD list prices; excludes cloud infrastructure and discounts",
        attribution="exact job_id + job_run_id",
        observed_savings_pct=None,
    )
    if comparable and Decimal(classic["list_cost_usd"]) > 0:
        result["observed_savings_pct"] = str(
            (1 - Decimal(target["list_cost_usd"]) / Decimal(classic["list_cost_usd"]))
            * 100
        )
    return result


BILLING_SQL = billing_sql("0", start_date="2026-01-01", end_date="2026-02-01")
