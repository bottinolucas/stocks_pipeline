with source as (
  select
    symbol,
    current_price::double precision as current_price_dbl,
    fetched_at
  from {{ ref('silver_stock_quotes') }}
  where current_price is not null
),

latest_day as (
  select max(fetched_at)::date as max_day
  from source
),

latest_prices as (
  select
    s.symbol,
    avg(s.current_price_dbl) as avg_price
  from source s
  join latest_day ld
    on s.fetched_at::date = ld.max_day
  group by s.symbol
),

all_time_volatility as (
  select
    symbol,
    stddev_pop(current_price_dbl) as volatility,
    case
      when avg(current_price_dbl) = 0 then null
      else stddev_pop(current_price_dbl) / nullif(avg(current_price_dbl), 0)
    end as relative_volatility
  from source
  group by symbol
)

select
  lp.symbol,
  lp.avg_price,
  v.volatility,
  v.relative_volatility
from latest_prices lp
join all_time_volatility v using (symbol)
order by lp.symbol
