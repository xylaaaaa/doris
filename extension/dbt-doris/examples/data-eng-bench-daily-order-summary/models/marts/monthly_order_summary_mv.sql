{{
  config(
    materialized='materialized_view',
    build_mode='immediate',
    refresh_trigger='manual',
    refresh_on_run=true,
    wait_for_refresh=true
  )
}}

select
    date_trunc(order_date, 'month') as order_month,
    sum(order_count) as order_count,
    round(sum(total_revenue), 2) as total_revenue
from {{ ref('daily_order_summary') }}
group by date_trunc(order_date, 'month')
