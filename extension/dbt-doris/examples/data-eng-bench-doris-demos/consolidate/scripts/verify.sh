#!/usr/bin/env bash
set -euo pipefail
MYSQL_BIN="${MYSQL_BIN:-mysql}"
DORIS_HOST="${DORIS_HOST:-127.0.0.1}"
DORIS_PORT="${DORIS_PORT:-9030}"
rows=$("$MYSQL_BIN" -N -s -h "$DORIS_HOST" -P "$DORIS_PORT" -u "${DORIS_USER:-root}" --password="${DORIS_PASSWORD:-}" -e "select source, ad_date, clicks, impressions, views, conversions from dbt_demo_consolidate.int__ads_unified order by source, ad_date")
test "$(printf '%s\n' "$rows" | wc -l)" -eq 6
test "$("$MYSQL_BIN" -N -s -h "$DORIS_HOST" -P "$DORIS_PORT" -u "${DORIS_USER:-root}" --password="${DORIS_PASSWORD:-}" -e "select count(*) from dbt_demo_consolidate.int__ads_unified")" -eq 6
printf '%s\n' "$rows"
