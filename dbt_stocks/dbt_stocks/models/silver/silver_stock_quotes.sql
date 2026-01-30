select
  id,
  symbol,
  fetched_at,

  current_price,
  day_open,
  day_high,
  day_low,
  prev_close,

  change_amount,

  case
    when prev_close = 0 then null
    else (change_amount / prev_close) * 100
  end as change_percent

from {{ ref('bronze_stock_quotes') }}
