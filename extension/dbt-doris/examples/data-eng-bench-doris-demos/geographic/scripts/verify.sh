#!/usr/bin/env bash
set -euo pipefail
MYSQL_BIN="${MYSQL_BIN:-mysql}"
DORIS_HOST="${DORIS_HOST:-127.0.0.1}"
DORIS_PORT="${DORIS_PORT:-9030}"
rows=$("$MYSQL_BIN" -N -s -h "$DORIS_HOST" -P "$DORIS_PORT" -u "${DORIS_USER:-root}" --password="${DORIS_PASSWORD:-}" -e "select state_province, customer_count, order_count, total_revenue from dbt_demo_geographic.fct_state_customers order by state_province")
expected=$'CA\t2\t2\t145.00\nNY\t1\t1\t50.00'
test "$rows" = "$expected"
printf '%s\n' "$rows"
