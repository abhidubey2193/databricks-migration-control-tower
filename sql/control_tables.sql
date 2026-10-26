-- Replace ${catalog} and ${schema} with identifiers you control before execution.
CREATE SCHEMA IF NOT EXISTS ${catalog}.${schema};
CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.workloads (
  workload_id STRING NOT NULL, kind STRING, name STRING, compute_mode STRING,
  readiness INT, eligible BOOLEAN, migration_status STRING,
  discovered_at TIMESTAMP, attributes_json STRING
) USING DELTA;
CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.findings (
  workload_id STRING NOT NULL, limitation_id STRING, severity STRING,
  detail STRING, remediation STRING, detected_at TIMESTAMP
) USING DELTA;
CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.approvals (
  workload_id STRING NOT NULL, requested_by STRING, approved_by STRING,
  approval_status STRING, approved_at TIMESTAMP, justification STRING
) USING DELTA;
CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.validation_runs (
  workload_id STRING NOT NULL, classic_run_id STRING, serverless_run_id STRING,
  result STRING, details_json STRING, validated_at TIMESTAMP
) USING DELTA;
CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.migration_events (
  workload_id STRING NOT NULL, event_type STRING, actor STRING,
  details_json STRING, event_at TIMESTAMP
) USING DELTA;
