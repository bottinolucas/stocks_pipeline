CREATE TABLE stock_quotes (
  id SERIAL PRIMARY KEY,
  symbol TEXT,
  fetched_at BIGINT,
  price NUMERIC,
  high NUMERIC,
  low NUMERIC,
  open_price NUMERIC,
  prev_close NUMERIC
);