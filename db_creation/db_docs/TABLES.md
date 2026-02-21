# Market Data Tables

Parquet-based market database in `db/`. Query with DuckDB (python: `import duckdb`). All parquet files use **Snappy compression** and **122,880 row group size**.

Data covers stocks and ETFs, years 2000-2026. Only regular trading hours (9:00 AM - 3:59 PM ET) are included in price data.

## Table Summary

| Table | File | Type | Sort Order | Partitioned |
|-------|------|------|------------|-------------|
| tickers | `tickers.parquet` | Single file | `ticker` ASC | No |
| prices | `prices/` | Hive-partitioned | `ticker` ASC, `ts` ASC | `year=YYYY/data.parquet` |
| trading_calendar | `trading_calendar.parquet` | Single file | `trading_day_num` ASC | No |
| daily_aggs_enriched | `daily_aggs_enriched/` | Hive-partitioned | `ticker` ASC, `trading_day_num` ASC | `year=YYYY/data.parquet` |
| cap_lookup | `cap_lookup/` | Hive-partitioned | `trading_day_num` ASC, `ticker` ASC | `year=YYYY/data.parquet` |
| insider_purchases | `insider_purchases/` | Hive-partitioned | `trading_day_num` ASC, `ticker` ASC | `year=YYYY/data.parquet` |

## Dependency Graph

```
tickers ──> prices ──> trading_calendar
   │           │              │
   │           └──────┐       ├───────────┐
   │                  ↓       ↓           ↓
   │         daily_aggs_enriched    insider_purchases
   │          + [market_cap CSVs]         ↑
   │                  │                   │
   │             cap_lookup               │
   └──────────────────────────────────────┘
```

**Direct dependencies:**
- `prices` depends on `tickers`
- `trading_calendar` depends on `prices`
- `daily_aggs_enriched` depends on `prices`, `trading_calendar` (+ reads market_cap CSVs directly)
- `cap_lookup` depends on `daily_aggs_enriched`
- `insider_purchases` depends on `tickers`, `trading_calendar`

Brackets indicate raw source data used as intermediate input only — no standalone parquet output. `prices` computes global `trading_day_num` and writes `trading_calendar.parquet` as a side-product. The `trading_calendar` step verifies it and makes the target available for dependency tracking.

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

## prices_v3

**File:** `prices/` (Hive-partitioned directory)

**Partitioning:** `year=YYYY/data.parquet` — one file per year (2000-2026)

**Sort order within each partition:** `ticker` ASC, `ts` ASC

**Data:** Hourly OHLCV bars, regular hours only (9 AM - 3:59 PM ET). Deduplicated — if a ticker appears in both stock and ETF sources, the ETF record wins. Each row includes a global `trading_day_num` identifying the calendar trading day.

| Column | Type | Description |
|--------|------|-------------|
| ticker | VARCHAR | Uppercase ticker symbol |
| ts | TIMESTAMP | Bar timestamp (hourly) |
| open | FLOAT | Opening price |
| high | FLOAT | High price |
| low | FLOAT | Low price |
| close | FLOAT | Closing price |
| volume | INTEGER | Bar volume |
| trading_day_num | SMALLINT | Global trading day number (same for all tickers on the same day) |
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

## trading_calendar_v3

**File:** `trading_calendar.parquet` (single file)

**Sort order:** `trading_day_num` ASC

**Data:** Maps global trading day numbers to calendar dates. Every US equity trading day with a global sequential number. Derived from all distinct trading days in the prices data. Written by the `prices` step and verified by the `trading_calendar` step.

| Column | Type | Description |
|--------|------|-------------|
| trading_day_num | SMALLINT | Global sequential trading day number (1, 2, 3, ...) |
| day | DATE | Calendar date for that trading day |

**Query:**
```sql
SELECT * FROM read_parquet('db/trading_calendar.parquet')
WHERE day BETWEEN '2024-01-01' AND '2024-12-31';

-- Find trading_day_num for a specific date
SELECT trading_day_num FROM read_parquet('db/trading_calendar.parquet')
WHERE day = '2024-06-15';
```

---

## daily_aggs_enriched_v3

**File:** `daily_aggs_enriched/` (Hive-partitioned directory)

**Partitioning:** `year=YYYY/data.parquet` — one file per year (2000-2026)

**Sort order within each partition:** `ticker` ASC, `trading_day_num` ASC

**Data:** Daily OHLCV (aggregated from hourly prices) enriched with market cap, a global trading day number shared across all tickers, and running cumulative sums per ticker. The cumulative sums enable computing any SMA with any window size via a self-join — no hardcoded windows. Sorted by `(ticker, trading_day_num)` so each ticker's full time series is contiguous, enabling efficient per-ticker lookups and SMA self-joins.

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
| trading_day_num | SMALLINT | Global trading day number (same for all tickers on the same day) |
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

The cumulative sums are computed per ticker across the full history before partitioning into year files. Any N-day SMA can be computed via a self-join on `trading_day_num`:

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

Since `trading_day_num` is global (same number for all tickers on the same day), it can also be used to align data across tickers or join with other tables like `insider_purchases` and `cap_lookup`.

When loading a subset of years, load enough extra years so the lagged `trading_day_num` falls within the loaded data. Rule of thumb: `ceil(max_sma_window / 252) + 1` extra years before the target period.

---

## cap_lookup_v2

**File:** `cap_lookup/` (Hive-partitioned directory)

**Partitioning:** `year=YYYY/data.parquet` — one file per year

**Sort order within each partition:** `trading_day_num` ASC, `ticker` ASC

**Data:** Subset of `daily_aggs_enriched` where `cap IS NOT NULL`, sorted by `(trading_day_num, ticker)`. Optimized for queries that filter by day first, then scan by market cap range. Use this table for day-level cap screening; use `daily_aggs_enriched` for ticker-level time series and SMA self-joins.

| Column | Type | Description |
|--------|------|-------------|
| day | DATE | Trading date |
| ticker | VARCHAR | Uppercase ticker symbol |
| cap | BIGINT | Market capitalization in dollars |
| close | FLOAT | Day close price |
| trading_day_num | SMALLINT | Global trading day number |
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

**Performance notes:** With `(trading_day_num, ticker)` sort order, all rows for a given trading day are contiguous. DuckDB's row-group min/max stats on `trading_day_num` enable effective pruning. Filter on `trading_day_num` or `day` to skip to the right block, then apply `cap` filter over just that day's rows. The SMA self-joins should still use `daily_aggs_enriched`.

---

## insider_purchases_v2

**File:** `insider_purchases/` (Hive-partitioned directory)

**Partitioning:** `year=YYYY/data.parquet` — one file per year, partitioned by `filing_date`

**Sort order within each partition:** `trading_day_num` ASC, `ticker` ASC

**Data:** SEC Form 4 open-market purchases only. Each row includes a `trading_day_num` mapped via ASOF join — weekend/holiday filings map to the next trading day. Sorted by `(trading_day_num, ticker)` for efficient day-based scanning and alignment with other daily tables.

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
| trading_day_num | SMALLINT | Global trading day number (next trading day on or after filing_date) |
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

## Query Efficiency Tips

1. **Hive-partitioned tables (prices, daily_aggs_enriched, cap_lookup, insider_purchases):** Always include `year = ...` or `year BETWEEN ... AND ...` in WHERE clauses. This prunes entire parquet files from disk reads.
2. **Sort order matters for query patterns.** `daily_aggs_enriched` sorts by `(ticker, trading_day_num)` — each ticker's full time series is contiguous, enabling efficient per-ticker lookups and SMA self-joins. `cap_lookup` and `insider_purchases` sort by `(trading_day_num, ticker)` for day-based scanning. `prices` sorts by `(ticker, ts)` for per-ticker lookups. `trading_calendar` sorts by `trading_day_num`.
3. **Row group statistics (min/max pruning):** Each file has 122,880-row groups. Because data is sorted, the min/max stats on the sort columns don't overlap between adjacent row groups, enabling DuckDB to skip irrelevant groups entirely. For example, in `prices` (sorted by `ticker, ts`), each row group covers a contiguous range of tickers — filtering on `ticker = 'AAPL'` only reads the few row groups whose min/max ticker range includes `'AAPL'`. In `daily_aggs_enriched` (sorted by `ticker, trading_day_num`), each row group covers a contiguous range of tickers — filtering on `ticker = 'AAPL'` only reads the few row groups whose min/max ticker range includes `'AAPL'`.
4. **Bloom filters:** DuckDB automatically writes bloom filters on dictionary-encoded and integer columns. Key columns with bloom filters: `ticker`, `ts`, `day`, and `trading_day_num`. These enable fast negative lookups — DuckDB can rule out a row group for a specific value without reading the column data.
5. **Use `hive_partitioning=true`** when querying partitioned directories. The `year` column comes from the directory path, not the parquet data itself.
6. **Glob pattern for partitioned tables:** Use `db/prices/**/data.parquet` (or `db/daily_aggs_enriched/**/data.parquet`, etc.). Each year partition contains a single `data.parquet` file.
7. **SMA computation:** Use cumulative sums in `daily_aggs_enriched` with a self-join on `trading_day_num`. Any window size works without pre-computed aggregation tables. Since `trading_day_num` is global, the same number aligns across all tickers and tables.
8. **Year buffer for lookbacks:** When loading partitioned data for a date range, load extra year(s) before the target period to cover SMA lookback and insider purchase lookback windows.
9. **Cross-table joins via trading_day_num:** Since `trading_day_num` is global (shared across all tables), you can efficiently join between `daily_aggs_enriched`, `cap_lookup`, `insider_purchases`, and `trading_calendar` on this column.
