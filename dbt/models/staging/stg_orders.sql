select order_id, customer_id, order_status, order_timestamp
from {{ source('ecommerce', 'orders') }}
