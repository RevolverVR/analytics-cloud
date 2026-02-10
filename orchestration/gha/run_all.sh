#!/usr/bin/env bash
set -euo pipefail

# Always run from repo root
REPO_ROOT="$(pwd)"

# Ensure data dir exists (local + CI)
mkdir -p "${REPO_ROOT}/data"

echo "[run_all] repo_root=${REPO_ROOT}"

# 1) Ingest -> RAW DuckDB (real)
echo "[run_all] ingest: load_to_duckdb"
python -m analytics_cloud.ingest.load_to_duckdb

# 2) Generate dbt profiles.yml (local absolute path; file is gitignored)
echo "[run_all] dbt: generate profiles.yml"
cat > dbt/analytics_dbt/profiles.yml << PROFILES
analytics_dbt:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: "${REPO_ROOT}/data/dev.duckdb"
      threads: 4
PROFILES

# 3) dbt build
echo "[run_all] dbt: deps + build"
dbt deps  --project-dir dbt/analytics_dbt --profiles-dir dbt/analytics_dbt
dbt build --project-dir dbt/analytics_dbt --profiles-dir dbt/analytics_dbt

# 4) SLA report (stdout)
echo "[run_all] sla: compute"
python -m analytics_cloud.sla.compute

# 5) Notify (optional; placeholder-safe)
if [[ "${ENABLE_NOTIFICATIONS:-0}" == "1" ]]; then
  echo "[run_all] sla: notify (enabled)"
  python -m analytics_cloud.sla.notify
else
  echo "[run_all] sla: notify (skipped) - set ENABLE_NOTIFICATIONS=1 to enable"
fi

# 6) Export mart artifacts (CSV + Parquet)
echo "[run_all] export: mart_ticket_metrics"
python -m analytics_cloud.dashboard.export

# 7) Load mart into Postgres (for Metabase)
echo "[run_all] postgres: load mart_ticket_metrics"
python -m analytics_cloud.dashboard.load_postgres
