#!/usr/bin/env bash
set -euo pipefail

demo_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
dbt_bin=${DBT_BIN:-dbt}
mysql_bin=${MYSQL_BIN:-mysql}
doris_host=${DORIS_HOST:-127.0.0.1}
doris_port=${DORIS_PORT:-9030}
doris_user=${DORIS_USER:-root}
doris_password=${DORIS_PASSWORD:-}

mysql_args=(-h "$doris_host" -P "$doris_port" -u "$doris_user")
if [[ -n "$doris_password" ]]; then
  mysql_args+=("-p$doris_password")
fi

"$mysql_bin" "${mysql_args[@]}" < "$demo_dir/scripts/setup.sql"
"$dbt_bin" debug --project-dir "$demo_dir" --profiles-dir "$demo_dir"
"$dbt_bin" build --select daily_order_summary --project-dir "$demo_dir" --profiles-dir "$demo_dir"
"$dbt_bin" run --select monthly_order_summary_mv --project-dir "$demo_dir" --profiles-dir "$demo_dir"
"$dbt_bin" run --select monthly_order_summary_mv --project-dir "$demo_dir" --profiles-dir "$demo_dir"
"$demo_dir/scripts/verify.sh"
