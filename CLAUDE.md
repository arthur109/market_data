# Market Data Database

Parquet-based market database in `db/`. Query with DuckDB (python: `import duckdb`). All parquet files use **ZSTD compression** and **122,880 row group size**.

Data covers stocks and ETFs, years 2000-2026. Only regular trading hours (9:00 AM - 3:59 PM ET) are included in price data.

## Dependency Graph

```
tickers ──> prices ──> daily_aggs ──> ten_day_aggs
   │                       └────────> hundred_day_aggs
   ├──> market_cap
   └──> insider_trades
```

---

## tickers_v1

**File:** `db/tickers.parquet` (single file)

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

**File:** `db/prices/` (Hive-partitioned directory)

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

## daily_aggs_v2

**File:** `db/daily_aggs/` (Hive-partitioned directory)

**Partitioning:** `year=YYYY/data.parquet` — one file per year (2000-2026)

**Sort order within each partition:** `ticker` ASC, `day` ASC

**Data:** Daily OHLCV aggregated from hourly prices. Includes component sums for building higher-level aggregations without re-reading hourly data.

| Column | Type | Description |
|--------|------|-------------|
| ticker | VARCHAR | Uppercase ticker symbol |
| day | DATE | Trading date |
| open | FLOAT | Day open (first hourly open) |
| high | FLOAT | Day high (max of hourly highs) |
| low | FLOAT | Day low (min of hourly lows) |
| close | FLOAT | Day close (last hourly close) |
| volume | BIGINT | Total day volume |
| sum_open | DOUBLE | Sum of all hourly opens |
| sum_high | DOUBLE | Sum of all hourly highs |
| sum_low | DOUBLE | Sum of all hourly lows |
| sum_close | DOUBLE | Sum of all hourly closes |
| sum_volume | BIGINT | Same as volume (sum of hourly volumes) |
| cnt | UTINYINT | Number of hourly bars that day |
| year | BIGINT | Hive partition key (auto from path) |

**Query:**
```sql
SELECT * FROM read_parquet('db/daily_aggs/**/data.parquet', hive_partitioning=true)
WHERE year = 2024 AND ticker = 'AAPL'
ORDER BY day;
```

**Component sums:** `sum_open`, `sum_high`, `sum_low`, `sum_close` are the raw sums of the hourly bar values. Combined with `cnt`, these allow computing VWAP-like averages or building multi-day aggregates without re-reading hourly data. For example, the average hourly close for a day is `sum_close / cnt`.

---

## ten_day_aggs_v1

**File:** `db/ten_day_aggs.parquet` (single file)

**Sort order:** `ticker` ASC, `block_start` ASC

**Data:** Non-overlapping 10-trading-day blocks per ticker. Days are numbered sequentially per ticker and grouped into blocks of 10. The last block for a ticker may have fewer than 10 days.

| Column | Type | Description |
|--------|------|-------------|
| ticker | VARCHAR | Uppercase ticker symbol |
| block_start | DATE | First trading day in the block |
| block_end | DATE | Last trading day in the block |
| open | FLOAT | Block open (first day's open) |
| high | FLOAT | Block high (max daily high) |
| low | FLOAT | Block low (min daily low) |
| close | FLOAT | Block close (last day's close) |
| volume | BIGINT | Total block volume |
| sum_open | DOUBLE | Sum of hourly opens across all days |
| sum_high | DOUBLE | Sum of hourly highs across all days |
| sum_low | DOUBLE | Sum of hourly lows across all days |
| sum_close | DOUBLE | Sum of hourly closes across all days |
| sum_volume | BIGINT | Sum of hourly volumes across all days |
| cnt | USMALLINT | Total hourly bar count across all days |
| day_cnt | UTINYINT | Number of trading days in this block (<=10) |

**Query:**
```sql
SELECT * FROM read_parquet('db/ten_day_aggs.parquet')
WHERE ticker = 'AAPL'
ORDER BY block_start;
```

---

## hundred_day_aggs_v1

**File:** `db/hundred_day_aggs.parquet` (single file)

**Sort order:** `ticker` ASC, `block_start` ASC

**Data:** Non-overlapping 100-trading-day blocks per ticker. Same structure as ten_day_aggs but with 100-day grouping. The last block for a ticker may have fewer than 100 days.

| Column | Type | Description |
|--------|------|-------------|
| ticker | VARCHAR | Uppercase ticker symbol |
| block_start | DATE | First trading day in the block |
| block_end | DATE | Last trading day in the block |
| open | FLOAT | Block open (first day's open) |
| high | FLOAT | Block high (max daily high) |
| low | FLOAT | Block low (min daily low) |
| close | FLOAT | Block close (last day's close) |
| volume | BIGINT | Total block volume |
| sum_open | DOUBLE | Sum of hourly opens across all days |
| sum_high | DOUBLE | Sum of hourly highs across all days |
| sum_low | DOUBLE | Sum of hourly lows across all days |
| sum_close | DOUBLE | Sum of hourly closes across all days |
| sum_volume | BIGINT | Sum of hourly volumes across all days |
| cnt | USMALLINT | Total hourly bar count across all days |
| day_cnt | UTINYINT | Number of trading days in this block (<=100) |

**Query:**
```sql
SELECT * FROM read_parquet('db/hundred_day_aggs.parquet')
WHERE ticker = 'AAPL'
ORDER BY block_start;
```

---

## market_cap_v2

**File:** `db/market_cap.parquet` (single file)

**Sort order:** `ticker` ASC, `day` ASC

**Data:** Daily market capitalization. Only includes tickers present in the tickers table. Values are filtered to `cap > 0` and `cap < 20 trillion`.

| Column | Type | Description |
|--------|------|-------------|
| ticker | VARCHAR | Uppercase ticker symbol |
| day | DATE | Date |
| cap | BIGINT | Market capitalization in dollars |

**Query:**
```sql
SELECT * FROM read_parquet('db/market_cap.parquet')
WHERE ticker = 'AAPL'
ORDER BY day;
```

---

## insider_trades_v2

**File:** `db/insider_trades.parquet` (single file)

**Sort order:** `ticker` ASC, `trade_date` ASC

**Data:** SEC Form 4 insider transactions. Only open-market purchases (`P`) and sales (`S`) are included. Only tickers present in the tickers table. Date range filtered to 2000-2026.

| Column | Type | Description |
|--------|------|-------------|
| ticker | VARCHAR | Uppercase ticker symbol |
| trade_date | DATE | Transaction date |
| tx_code | VARCHAR | `"P"` (purchase) or `"S"` (sale) |
| shares | FLOAT | Number of shares transacted |
| total_value | FLOAT | `shares * pricePerShare` (may be NULL) |
| acquired_disposed | VARCHAR | `"A"` (acquired) or `"D"` (disposed) |
| shares_after | FLOAT | Shares owned after transaction (may be NULL) |
| ownership_type | VARCHAR | `"D"` (direct) or `"I"` (indirect) |
| is_director | BOOLEAN | Insider is a board director |
| is_officer | BOOLEAN | Insider is a company officer |
| is_ten_pct_owner | BOOLEAN | Insider is a 10%+ owner |
| insider_name | VARCHAR | Name of the reporting insider |
| insider_cik | VARCHAR | SEC CIK identifier for the insider |
| officer_title | VARCHAR | Officer title if applicable (may be NULL) |

**Query:**
```sql
SELECT * FROM read_parquet('db/insider_trades.parquet')
WHERE ticker = 'AAPL' AND tx_code = 'P'
ORDER BY trade_date;
```

---

## Query Efficiency Tips

1. **Hive-partitioned tables (prices, daily_aggs):** Always include `year = ...` or `year BETWEEN ... AND ...` in WHERE clauses. This prunes entire parquet files from disk reads.
2. **All tables are sorted by `(ticker, ...)`** as the primary sort key. DuckDB uses row-group min/max statistics to skip row groups that don't contain your ticker, so `WHERE ticker = 'X'` is efficient even on single-file tables.
3. **Use `hive_partitioning=true`** when querying `prices` or `daily_aggs` directories. The `year` column comes from the directory path, not the parquet data itself.
4. **Glob pattern for partitioned tables:** Use `db/prices/**/data.parquet` (or `db/daily_aggs/**/data.parquet`). Each year partition contains a single `data.parquet` file.
5. **Multi-resolution price data:** Use `hundred_day_aggs` for coarse scans, `ten_day_aggs` for medium resolution, `daily_aggs` for daily, and `prices` for hourly. This avoids reading billions of hourly rows when daily or block-level granularity suffices.
