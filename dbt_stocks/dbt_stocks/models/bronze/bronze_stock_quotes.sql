{{ config(materialized='view') }}

SELECT
  id,
  symbol,
  to_timestamp(fetched_at) AS fetched_at,
  price AS current_price,
  high AS day_high,
  low AS day_low,
  open_price AS day_open,
  prev_close
FROM {{ source('raw', 'stock_quotes') }}
