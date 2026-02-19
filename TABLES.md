# Market Data Tables

Parquet-based market database in `db/`. Query with DuckDB (python: `import duckdb`). All parquet files use **Snappy compression** and **122,880 row group size**.

Data covers stocks and ETFs, years 2000-2026. Only regular trading hours (9:00 AM - 3:59 PM ET) are included in price data.

## Dependency Graph

```
tickers ──> prices ──> daily_aggs_enriched ──> trading_calendar
   │            │              ▲       │
   │            │     [market_cap CSVs] └──> cap_lookup
   │            │
   └──> insider_purchases
```

Brackets indicate raw source data used as intermediate input only — no standalone parquet output.

---

## tickers_v1

**File:** `tickers.parquet` (single file)

**Sort order:** `ticker` ASC

| Column | Type | Description |
|--------|------|-------------|
| ticker | VARCHAR | Uppercase ticker symbol (e.g. "AAPL") |
| asset_type | VARCHAR | `"stock"` or `"etf"` (ETF wins on overlap) |

**Query:**
```sql
SELECT * FROM read_parquet('db/tickers.parquet')
WHERE ticker = 'AAPL';
```

---

## prices_v2

**File:** `prices/` (Hive-partitioned directory)

**Partitioning:** `year=YYYY/data.parquet` — one file per year (2000-2026)

**Sort order within each partition:** `ticker` ASC, `ts` ASC

**Data:** Hourly OHLCV bars, regular hours only (9 AM - 3:59 PM ET). Deduplicated — if a ticker appears in both stock and ETF sources, the ETF record wins.

| Column | Type | Description |
|--------|------|-------------|
| ticker | VARCHAR | Uppercase ticker symbol |
| ts | TIMESTAMP | Bar timestamp (hourly) |
| open | FLOAT | Opening price |
| high | FLOAT | High price |
| low | FLOAT | Low price |
| close | FLOAT | Closing price |
| volume | INTEGER | Bar volume |
| year | BIGINT | Hive partition key (auto from path) |

**Query (use hive_partitioning + year filter for pruning):**
```sql
-- Single ticker, single year (fastest — reads one file)
SELECT * FROM read_parquet('db/prices/**/data.parquet', hive_partitioning=true)
WHERE year = 2024 AND ticker = 'AAPL'
ORDER BY ts;

-- Single ticker, date range (prunes to relevant year partitions)
SELECT * FROM read_parquet('db/prices/**/data.parquet', hive_partitioning=true)
WHERE year BETWEEN 2023 AND 2024 AND ticker = 'AAPL'
AND ts >= '2023-06-01' AND ts < '2024-07-01';
```

**Performance notes:** Always filter on `year` to get partition pruning. Within a partition, data is sorted by `(ticker, ts)` so filtering on `ticker` benefits from row-group min/max statistics.

---

## daily_aggs_enriched_v1

**File:** `daily_aggs_enriched/` (Hive-partitioned directory)

**Partitioning:** `year=YYYY/data.parquet` — one file per year (2000-2026)

**Sort order within each partition:** `ticker` ASC, `day` ASC

**Data:** Daily OHLCV (aggregated from hourly prices) enriched with market cap, a sequential trading day index per ticker, and running cumulative sums. The cumulative sums enable computing any SMA with any window size via a self-join — no hardcoded windows.

| Column | Type | Description |
|--------|------|-------------|
| ticker | VARCHAR | Uppercase ticker symbol |
| day | DATE | Trading date |
| open | FLOAT | Day open (first hourly open) |
| high | FLOAT | Day high (max of hourly highs) |
| low | FLOAT | Day low (min of hourly lows) |
| close | FLOAT | Day close (last hourly close) |
| volume | BIGINT | Total day volume |
| cap | BIGINT | Market capitalization in dollars (NULL if unavailable) |
| trading_day_num | BIGINT | Sequential trading day number for this ticker (1, 2, 3, ...) |
| cum_close | DOUBLE | Running cumulative sum of `close` for this ticker |
| cum_high | DOUBLE | Running cumulative sum of `high` |
| cum_low | DOUBLE | Running cumulative sum of `low` |
| cum_volume | DOUBLE | Running cumulative sum of `volume` |
| year | BIGINT | Hive partition key (auto from path) |

**Query:**
```sql
SELECT * FROM read_parquet('db/daily_aggs_enriched/**/data.parquet', hive_partitioning=true)
WHERE year = 2024 AND ticker = 'AAPL'
ORDER BY day;
```

**Computing SMAs via cumulative sums:**

The cumulative sums are computed globally across the full history before partitioning into year files. Any N-day SMA can be computed via a self-join on `trading_day_num`:

```sql
-- 20-day SMA of close for AAPL
SELECT
    a.day,
    a.close,
    (a.cum_close - b.cum_close) / 20.0 AS sma_20
FROM daily_aggs_enriched a
JOIN daily_aggs_enriched b
  ON b.ticker = a.ticker
  AND b.trading_day_num = a.trading_day_num - 20
WHERE a.ticker = 'AAPL';
```

When loading a subset of years, load enough extra years so the lagged `trading_day_num` falls within the loaded data. Rule of thumb: `ceil(max_sma_window / 252) + 1` extra years before the target period.

---

## cap_lookup_v1

**File:** `cap_lookup/` (Hive-partitioned directory)

**Partitioning:** `year=YYYY/data.parquet` — one file per year

**Sort order within each partition:** `day` ASC, `ticker` ASC

**Data:** Subset of `daily_aggs_enriched` where `cap IS NOT NULL`, re-sorted by `(day, ticker)`. Optimized for queries that filter by day first, then scan by market cap range. Use this table for day-level cap screening; use `daily_aggs_enriched` for ticker-level time series and SMA self-joins.

| Column | Type | Description |
|--------|------|-------------|
| day | DATE | Trading date |
| ticker | VARCHAR | Uppercase ticker symbol |
| cap | BIGINT | Market capitalization in dollars |
| close | FLOAT | Day close price |
| trading_day_num | BIGINT | Sequential trading day number for this ticker |
| cum_close | DOUBLE | Running cumulative sum of `close` for this ticker |
| year | BIGINT | Hive partition key (auto from path) |

**Query:**
```sql
-- All tickers in a cap range on a specific day
SELECT ticker, close, cap, trading_day_num, cum_close
FROM read_parquet('db/cap_lookup/**/data.parquet', hive_partitioning=true)
WHERE year = 2024 AND day = '2024-06-15'
  AND cap BETWEEN 1000000000 AND 10000000000;
```

**Performance notes:** With `(day, ticker)` sort order, all rows for a given day are contiguous. DuckDB's row-group min/max stats on `day` skip straight to the right block, then the `cap` filter runs over just that day's rows. The SMA self-joins should still use `daily_aggs_enriched` (which keeps its `(ticker, day)` sort for efficient `trading_day_num` lookups). Each table is sorted for its job.

---

## insider_purchases_v1

**File:** `insider_purchases/` (Hive-partitioned directory)

**Partitioning:** `year=YYYY/data.parquet` — one file per year, partitioned by `filing_date`

**Sort order within each partition:** `filing_date` ASC, `ticker` ASC

**Data:** SEC Form 4 open-market purchases only. Sorted by filing date (when the trade became public knowledge) rather than trade date, to avoid lookahead bias in backtesting.

| Column | Type | Description |
|--------|------|-------------|
| filing_date | DATE | SEC filing date (when the trade became public knowledge) |
| ticker | VARCHAR | Uppercase ticker symbol |
| insider_cik | VARCHAR | SEC CIK identifier for the insider |
| total_value | FLOAT | `shares * pricePerShare` (may be NULL) |
| shares | FLOAT | Number of shares purchased |
| is_director | BOOLEAN | Insider is a board director |
| is_officer | BOOLEAN | Insider is a company officer |
| is_ten_pct_owner | BOOLEAN | Insider is a 10%+ owner |
| officer_title | VARCHAR | Officer title if applicable (may be NULL) |
| insider_name | VARCHAR | Name of the reporting insider |
| year | BIGINT | Hive partition key (auto from path) |

**Query:**
```sql
SELECT * FROM read_parquet('db/insider_purchases/**/data.parquet', hive_partitioning=true)
WHERE year = 2024 AND ticker = 'AAPL'
ORDER BY filing_date;
```

**Performance note:** When scanning for insider signals, push the candidate ticker list into the query rather than aggregating all tickers:

```sql
-- Do this (push-in filter):
SELECT ticker, COUNT(DISTINCT insider_cik) AS unique_buyers
FROM read_parquet('db/insider_purchases/**/data.parquet', hive_partitioning=true)
WHERE year BETWEEN 2023 AND 2024
  AND filing_date BETWEEN '2024-01-01' AND '2024-03-31'
  AND ticker IN (SELECT ticker FROM cap_filtered_candidates)
GROUP BY ticker;
```

---

## trading_calendar_v1

**File:** `trading_calendar.parquet` (single file)

**Sort order:** `day` ASC

**Data:** Every US equity trading day. Derived from the union of SPY and AAPL trading days to avoid gaps.

| Column | Type | Description |
|--------|------|-------------|
| day | DATE | A trading day |

**Query:**
```sql
SELECT * FROM read_parquet('db/trading_calendar.parquet')
WHERE day BETWEEN '2024-01-01' AND '2024-12-31';
```

---

## Query Efficiency Tips

1. **Hive-partitioned tables (prices, daily_aggs_enriched, cap_lookup, insider_purchases):** Always include `year = ...` or `year BETWEEN ... AND ...` in WHERE clauses. This prunes entire parquet files from disk reads.
2. **Sort order matters for query patterns.** Most tables sort by `(ticker, ...)` for efficient per-ticker lookups. `cap_lookup` sorts by `(day, ticker)` for efficient per-day cap screening. `insider_purchases` sorts by `(filing_date, ticker)`. `trading_calendar` sorts by `day`. DuckDB uses row-group min/max statistics to skip row groups based on leading sort columns.
3. **Use `hive_partitioning=true`** when querying partitioned directories. The `year` column comes from the directory path, not the parquet data itself.
4. **Glob pattern for partitioned tables:** Use `db/prices/**/data.parquet` (or `db/daily_aggs_enriched/**/data.parquet`, etc.). Each year partition contains a single `data.parquet` file.
5. **SMA computation:** Use cumulative sums in `daily_aggs_enriched` with a self-join on `trading_day_num`. Any window size works without pre-computed aggregation tables.
6. **Year buffer for lookbacks:** When loading partitioned data for a date range, load extra year(s) before the target period to cover SMA lookback and insider purchase lookback windows.
