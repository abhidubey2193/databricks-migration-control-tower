#!/usr/bin/env bash
set -euo pipefail
profile="${1:-SANDBOX}"
target="${2:-dev}"
cd "$(dirname "$0")/.."
if ! command -v databricks >/dev/null; then
  echo 'Install the official Databricks CLI, then run: databricks auth login --profile SANDBOX' >&2
  exit 1
fi
databricks current-user me --profile "$profile" >/dev/null
databricks bundle validate -t "$target" --profile "$profile"
databricks bundle deploy -t "$target" --profile "$profile"
databricks bundle run smct_fleet_controller -t "$target" --profile "$profile"
databricks bundle run smct_dashboard_snapshot -t "$target" --profile "$profile"
echo 'Deployed. Run smct_fleet_controller for the manifest workflow; it defaults to scan.'
