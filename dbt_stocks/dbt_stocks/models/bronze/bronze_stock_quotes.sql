{{ config(materialized='view') }}

SELECT
  id,
  symbol,
  to_timestamp(fetched_at) AS fetched_at,
  price AS current_price,
  high AS day_high,
  low AS day_low,
  open_price AS day_open,
  prev_close,
  (price - prev_close) as change_amount
FROM {{ source('raw', 'stock_quotes') }}
