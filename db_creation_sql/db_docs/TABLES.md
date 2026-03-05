# Market Data Tables (SQLite)

SQLite-based market database in `db_sql/`. Query with Python's `sqlite3` module. Data is split across **per-year `.db` files** plus two staging databases for reference data.

All tables use **STRICT mode**. Primary-key-only tables use **WITHOUT ROWID** for compact clustered storage. Dates are stored as **YYYYMMDD integers**, timestamps as **epoch seconds (UTC)**.

Data covers stocks and ETFs, years 2000-2026. Only regular trading hours (9:00 AM - 3:59 PM ET) are included in price data.

## Database Files

| File | Contents |
|------|----------|
| `staging_tickers.db` | `tickers` table (all tickers, built once) |
| `staging_calendar.db` | `trading_calendar` table (all trading days, built once) |
| `YYYY.db` (e.g. `2020.db`) | Per-year data: `tickers` + `trading_calendar` (reference copies) + `prices` + `daily_aggs_enriched` + `cap_lookup` + `insider_purchases` |

Each year `.db` file is **self-contained** — it includes reference copies of `tickers` and `trading_calendar` so queries don't need to attach other databases.

## Table Summary

| Table | Location | Primary Key / Clustering | Indexes |
|-------|----------|--------------------------|---------|
| tickers | staging + year DBs | `ticker` (WITHOUT ROWID) | — |
| trading_calendar | staging + year DBs | `trading_day_num` (WITHOUT ROWID) | — |
| prices | year DBs | rowid | `(ticker, ts)`, `(trading_day_num, ticker)` |
| daily_aggs_enriched | year DBs | rowid | `(trading_day_num, ticker)`, `(ticker, day)` |
| cap_lookup | year DBs | `(trading_day_num, ticker)` (WITHOUT ROWID) | — |
| insider_purchases | year DBs | rowid | `(trading_day_num, ticker)` |

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
- `data_quality` depends on `prices`, `trading_calendar`
- `daily_aggs_enriched` depends on `prices`, `trading_calendar`, `data_quality` (+ reads market_cap CSVs directly)
- `cap_lookup` depends on `daily_aggs_enriched`
- `insider_purchases` depends on `tickers`, `trading_calendar`

---

## tickers

**Location:** `staging_tickers.db` + copied into each year `.db`

**Schema:** `WITHOUT ROWID, STRICT`

| Column | Type | Description |
|--------|------|-------------|
| ticker | TEXT | Uppercase ticker symbol (e.g. "AAPL") — **PRIMARY KEY** |
| asset_type | TEXT | `"stock"` or `"etf"` (ETF wins on overlap) |

**Query:**
```sql
SELECT * FROM tickers WHERE ticker = 'AAPL';
```

---

## trading_calendar

**Location:** `staging_calendar.db` + copied into each year `.db`

**Schema:** `WITHOUT ROWID, STRICT`

| Column | Type | Description |
|--------|------|-------------|
| trading_day_num | INTEGER | Global sequential trading day number (1, 2, 3, ...) — **PRIMARY KEY** |
| day | INTEGER | Calendar date as YYYYMMDD (e.g. 20240615) |

**Query:**
```sql
-- All trading days in 2024
SELECT * FROM trading_calendar
WHERE day BETWEEN 20240101 AND 20241231;

-- Find trading_day_num for a specific date
SELECT trading_day_num FROM trading_calendar WHERE day = 20240615;
```

---

## prices

**Location:** Per-year `.db` files (e.g. `2024.db`)

**Schema:** `STRICT`

**Indexes:** `(ticker, ts)`, `(trading_day_num, ticker)`

| Column | Type | Description |
|--------|------|-------------|
| ticker | TEXT | Uppercase ticker symbol (FK: `tickers`) |
| ts | INTEGER | Bar timestamp as epoch seconds (UTC) |
| open | REAL | Opening price |
| high | REAL | High price |
| low | REAL | Low price |
| close | REAL | Closing price |
| volume | INTEGER | Bar volume |
| trading_day_num | INTEGER | Global trading day number (FK: `trading_calendar`) |

**Data:** Hourly OHLCV bars, regular hours only (9 AM - 3:59 PM ET). Deduplicated — if a ticker appears in both stock and ETF sources, the ETF record wins.

**Query:**
```sql
-- Single ticker in a year DB (uses idx_prices_ticker_ts)
SELECT * FROM prices WHERE ticker = 'AAPL' ORDER BY ts;

-- All tickers on a specific trading day (uses idx_prices_tdn_ticker)
SELECT * FROM prices WHERE trading_day_num = 6500;
```

---

## daily_aggs_enriched

**Location:** Per-year `.db` files

**Schema:** `STRICT`

**Indexes:** `(trading_day_num, ticker)`, `(ticker, day)`

| Column | Type | Description |
|--------|------|-------------|
| ticker | TEXT | Uppercase ticker symbol (FK: `tickers`) |
| day | INTEGER | Trading date as YYYYMMDD |
| open | REAL | Day open (first hourly open) |
| high | REAL | Day high (max of hourly highs) |
| low | REAL | Day low (min of hourly lows) |
| close | REAL | Day close (last hourly close) |
| volume | INTEGER | Total day volume |
| cap | INTEGER | Market capitalization in dollars (NULL if unavailable) |
| trading_day_num | INTEGER | Global trading day number (FK: `trading_calendar`) |
| cum_close | REAL | Running cumulative sum of `close` for this ticker |
| cum_high | REAL | Running cumulative sum of `high` |
| cum_low | REAL | Running cumulative sum of `low` |
| cum_volume | REAL | Running cumulative sum of `volume` |

**Data:** Daily OHLCV aggregated from hourly prices, enriched with market cap and running cumulative sums per ticker. Cumulative sums enable computing any SMA via a self-join on `trading_day_num`.

**Data quality filtering:** If `data_quality_exclusions.csv` exists in the output directory, rows matching `(ticker, day)` pairs in that file are excluded before aggregation. This removes flagged bad price ticks (e.g., garbage prices orders of magnitude away from the rolling median). Run `python build.py data_quality` to generate the exclusion file, review/edit it, then rebuild this table.

**Query:**
```sql
-- Single ticker time series (uses idx_dae_ticker_day)
SELECT * FROM daily_aggs_enriched
WHERE ticker = 'AAPL' ORDER BY day;

-- All tickers on a specific trading day (uses idx_dae_tdn_ticker)
SELECT * FROM daily_aggs_enriched WHERE trading_day_num = 6500;
```

**Computing SMAs via cumulative sums:**

```sql
-- 20-day SMA of close for AAPL (within a single year DB)
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

Since `trading_day_num` is global (same number for all tickers on the same day), it can be used to align data across tickers or join with other tables.

**Cross-year SMA:** When computing SMAs near year boundaries, you need data from the prior year. Either ATTACH the prior year's DB or load the needed rows into a temp table.

---

## cap_lookup

**Location:** Per-year `.db` files

**Schema:** `WITHOUT ROWID, STRICT` — composite primary key `(trading_day_num, ticker)`

| Column | Type | Description |
|--------|------|-------------|
| trading_day_num | INTEGER | Global trading day number (FK: `trading_calendar`) — **PK part 1** |
| ticker | TEXT | Uppercase ticker symbol (FK: `tickers`) — **PK part 2** |
| day | INTEGER | Trading date as YYYYMMDD |
| cap | INTEGER | Market capitalization in dollars |
| close | REAL | Day close price |
| cum_close | REAL | Running cumulative sum of `close` for this ticker |

**Data:** Subset of `daily_aggs_enriched` where `cap IS NOT NULL`. Clustered by `(trading_day_num, ticker)` for efficient day-based cap screening. Use this table for day-level cap filtering; use `daily_aggs_enriched` for ticker-level time series and SMA self-joins.

**Query:**
```sql
-- All tickers in a cap range on a specific trading day
SELECT ticker, close, cap, cum_close
FROM cap_lookup
WHERE trading_day_num = 6500
  AND cap BETWEEN 1000000000 AND 10000000000;
```

**Performance notes:** The WITHOUT ROWID clustered primary key on `(trading_day_num, ticker)` means all rows for a given trading day are physically contiguous. Queries filtering on `trading_day_num` first are very fast.

---

## insider_purchases

**Location:** Per-year `.db` files (partitioned by filing_date year)

**Schema:** `STRICT`

**Indexes:** `(trading_day_num, ticker)`

| Column | Type | Description |
|--------|------|-------------|
| filing_date | INTEGER | SEC filing date as YYYYMMDD |
| ticker | TEXT | Uppercase ticker symbol (FK: `tickers`) |
| insider_cik | TEXT | SEC CIK identifier for the insider |
| total_value | REAL | `shares * pricePerShare` (summed across same-day transactions; may be NULL) |
| trading_day_num | INTEGER | Global trading day number (next trading day on or after filing_date; FK: `trading_calendar`) |

**Data:** SEC Form 4 open-market purchases only. Multiple purchases by the same insider on the same filing date for the same ticker are combined into a single row (total_value is summed, preserving the weighted average price). `trading_day_num` mapped so weekend/holiday filings point to the next trading day.

**Query:**
```sql
-- Insider purchases for a ticker
SELECT * FROM insider_purchases
WHERE ticker = 'AAPL' ORDER BY filing_date;

-- Insider activity on a specific trading day (uses idx_ip_tdn_ticker)
SELECT * FROM insider_purchases WHERE trading_day_num = 6500;
```

---

## Query Patterns

### Opening a year database for reads

These databases are pre-built, read-only artifacts. Strip all write-path overhead for maximum read performance:

```python
import sqlite3

conn = sqlite3.connect("db_sql/2024.db")
conn.execute("PRAGMA journal_mode = OFF")       # no journal (read-only, nothing to recover)
conn.execute("PRAGMA locking_mode = EXCLUSIVE")  # skip lock syscalls (single-process access)
conn.execute("PRAGMA foreign_keys = OFF")        # skip FK validation on reads
conn.execute("PRAGMA trusted_schema = ON")       # skip schema validation overhead
conn.execute("PRAGMA mmap_size = 268435456")     # 256MB mmap for zero-copy reads
conn.execute("PRAGMA cache_size = -64000")       # 64MB page cache
conn.execute("PRAGMA temp_store = MEMORY")       # temp tables in RAM
```

Or use the `open_readonly()` helper from `build_common.py` which sets all of these automatically.

**Why each PRAGMA matters:**

| PRAGMA | Default | Override | Effect |
|--------|---------|----------|--------|
| `journal_mode` | DELETE | OFF | Eliminates journal file creation and syncs. Safe because the DB is never written to. |
| `locking_mode` | NORMAL | EXCLUSIVE | Acquires lock once and holds it, avoiding per-query lock/unlock syscalls. |
| `foreign_keys` | OFF | OFF (explicit) | Ensures FK checks never run, even if enabled elsewhere. Zero cost on reads. |
| `trusted_schema` | OFF | ON | Skips security checks on views/triggers. Saves overhead since we control the schema. |
| `mmap_size` | 0 | 256MB | Memory-maps the DB file so reads bypass the SQLite page cache and use OS page cache directly. Avoids `read()` syscalls and double-buffering. |
| `cache_size` | ~2MB | 64MB | Larger page cache for queries that can't use mmap (e.g., temp tables, complex joins). |
| `temp_store` | FILE | MEMORY | Temp tables and sort spills go to RAM instead of disk. |

**Additional notes:**
- The databases use **32KB pages** (`page_size = 32768`). This is baked into the file at build time and cannot be changed at read time. Larger pages reduce B-tree depth and favor analytical (sequential scan) workloads over OLTP (random point lookups).
- **ANALYZE** was run at build time, so `sqlite_stat1` is populated and the query planner will use index statistics automatically.
- **VACUUM** was run at build time, so the file is defragmented with no wasted pages.

### Cross-year queries

Each year DB is independent. To query across years, use SQLite's ATTACH to combine databases:

```sql
ATTACH 'db_sql/2023.db' AS y2023;
ATTACH 'db_sql/2024.db' AS y2024;

SELECT * FROM y2023.daily_aggs_enriched
WHERE ticker = 'AAPL'
UNION ALL
SELECT * FROM y2024.daily_aggs_enriched
WHERE ticker = 'AAPL'
ORDER BY day;
```

### SMA computation across years

```sql
ATTACH 'db_sql/2023.db' AS prev;
-- Compute 20-day SMA in 2024 using 2023 lookback
SELECT
    a.day,
    a.close,
    (a.cum_close - b.cum_close) / 20.0 AS sma_20
FROM daily_aggs_enriched a
LEFT JOIN daily_aggs_enriched b
  ON b.ticker = a.ticker
  AND b.trading_day_num = a.trading_day_num - 20
LEFT JOIN prev.daily_aggs_enriched pb
  ON pb.ticker = a.ticker
  AND pb.trading_day_num = a.trading_day_num - 20
WHERE a.ticker = 'AAPL';
```

### Index usage tips

1. **`prices`**: Use `(ticker, ts)` index for per-ticker time series queries. Use `(trading_day_num, ticker)` for day-based cross-ticker queries.
2. **`daily_aggs_enriched`**: Use `(ticker, day)` for per-ticker lookups. Use `(trading_day_num, ticker)` for day-based scanning and cross-ticker joins.
3. **`cap_lookup`**: Clustered on `(trading_day_num, ticker)` via WITHOUT ROWID — filter on `trading_day_num` first, then cap range. Data is physically stored in PK order so all rows for a given day are contiguous on disk.
4. **`insider_purchases`**: Use `(trading_day_num, ticker)` index for day-based scanning.
5. **Cross-table joins via `trading_day_num`**: Since `trading_day_num` is global (shared across all tables), join between `daily_aggs_enriched`, `cap_lookup`, `insider_purchases`, and `trading_calendar` on this column.
6. **INTEGER dates are fast to compare**: `day BETWEEN 20240101 AND 20241231` is a simple integer range scan — no string parsing overhead. Convert with `day // 10000` for year, `(day // 100) % 100` for month, `day % 100` for day-of-month.
