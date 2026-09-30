-- Illustrative Snowflake/dbt mart; wire this to Snowflake after the local PySpark pipeline is working.
select cast(order_timestamp as date) as order_date, count(distinct order_id) as order_count
from {{ ref('stg_orders') }}
group by 1
