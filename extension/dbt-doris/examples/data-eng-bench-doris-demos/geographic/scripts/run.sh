#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
DBT_BIN="${DBT_BIN:-dbt}"
MYSQL_BIN="${MYSQL_BIN:-mysql}"
DORIS_HOST="${DORIS_HOST:-127.0.0.1}"
DORIS_PORT="${DORIS_PORT:-9030}"
"$MYSQL_BIN" -h "$DORIS_HOST" -P "$DORIS_PORT" -u "${DORIS_USER:-root}" --password="${DORIS_PASSWORD:-}" < scripts/setup.sql
"$DBT_BIN" debug --project-dir . --profiles-dir .
"$DBT_BIN" build --project-dir . --profiles-dir .
"$DBT_BIN" build --project-dir . --profiles-dir .
