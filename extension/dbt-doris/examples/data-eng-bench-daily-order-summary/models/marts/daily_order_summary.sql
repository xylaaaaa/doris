{{
  config(
    materialized='table',
    duplicate_key=['order_date'],
    distributed_by=['order_date'],
    buckets=1,
    properties={'replication_num': '1'}
  )
}}

with valid_orders as (
    select
        cast(ordered_at as date) as order_date,
        grand_total
    from {{ source('orders', 'orders') }}
    where status not in ('CANCELLED', 'RETURNED', 'FAILED')
)

select
    order_date,
    count(*) as order_count,
    round(sum(grand_total), 2) as total_revenue
from valid_orders
group by order_date
