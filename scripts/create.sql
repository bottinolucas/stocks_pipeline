create schema if not exists bronze;
create schema if not exists silver;
create schema if not exists gold;

CREATE TABLE public.stock_quotes (
  id SERIAL PRIMARY KEY,
  symbol TEXT,
  fetched_at BIGINT,
  price NUMERIC,
  high NUMERIC,
  low NUMERIC,
  open_price NUMERIC,
  prev_close NUMERIC
);