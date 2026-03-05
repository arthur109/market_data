"""Step 8: Build daily_aggs_enriched — daily OHLCV with market cap and cumulative sums."""

import sqlite3

from build_common import (
    MARKET_CAP_DIR, OUTPUT_DIR,
    create_indexes, create_table, get_year_dbs, install_duckdb_sqlite,
    log, optimize_db, step, verify_table,
)


@step("daily_aggs_enriched_v5", target="daily_aggs_enriched", depends_on=("prices", "trading_calendar", "data_quality"))
def build_daily_aggs_enriched(con):
    """Daily OHLCV enriched with market cap, global trading day number, and cumulative sums."""
    csv_pattern = str(MARKET_CAP_DIR / "*.csv")
    tickers_db = str(OUTPUT_DIR / "staging_tickers.db")
    calendar_db = str(OUTPUT_DIR / "staging_calendar.db")
    year_dbs = get_year_dbs()

    install_duckdb_sqlite(con)

    # Load all prices from year dbs via sqlite_scan (ts is epoch seconds)
    log("Loading prices from year SQLite databases...")
    parts = [f"SELECT ticker, to_timestamp(ts) AS ts, open, high, low, close, volume, trading_day_num FROM sqlite_scan('{p}', 'prices')" for _, p in year_dbs]
    con.execute(f"CREATE TABLE _all_prices AS {' UNION ALL '.join(parts)}")

    # Apply data quality exclusions before aggregation
    staging_path = OUTPUT_DIR / "staging_exclusions.db"
    if staging_path.exists():
        log("Applying data quality exclusions from staging_exclusions.db...")
        before = con.execute("SELECT COUNT(*) FROM _all_prices").fetchone()[0]

        nuked_count = con.execute(
            f"SELECT COUNT(*) FROM sqlite_scan('{staging_path}', 'nuked_tickers')"
        ).fetchone()[0]
        if nuked_count:
            con.execute(f"""
                DELETE FROM _all_prices
                WHERE ticker IN (SELECT ticker FROM sqlite_scan('{staging_path}', 'nuked_tickers'))
            """)

        dropped_count = con.execute(
            f"SELECT COUNT(*) FROM sqlite_scan('{staging_path}', 'dropped_bars')"
        ).fetchone()[0]
        if dropped_count:
            # ts in _all_prices is TIMESTAMP (via to_timestamp), staging has INTEGER epoch
            con.execute(f"""
                CREATE TABLE _drop AS
                SELECT ticker, to_timestamp(ts) AS ts
                FROM sqlite_scan('{staging_path}', 'dropped_bars')
            """)
            con.execute("DELETE FROM _all_prices WHERE (ticker, ts) IN (SELECT ticker, ts FROM _drop)")
            con.execute("DROP TABLE _drop")

        after = con.execute("SELECT COUNT(*) FROM _all_prices").fetchone()[0]
        excluded = before - after
        log(f"  Nuked {nuked_count:,} tickers, dropped {dropped_count:,} individual bars, excluded {excluded:,} total rows")
    else:
        log("No staging_exclusions.db found — no filtering applied.")

    log("Aggregating hourly prices into daily OHLCV...")

    con.execute("""
        CREATE TABLE _daily AS
        SELECT
            ticker,
            CAST(ts AS DATE) AS day,
            FIRST(open ORDER BY ts) AS open,
            MAX(high) AS high,
            MIN(low) AS low,
            LAST(close ORDER BY ts) AS close,
            SUM(volume)::BIGINT AS volume
        FROM _all_prices
        GROUP BY ticker, CAST(ts AS DATE)
    """)

    con.execute("DROP TABLE _all_prices")

    daily_count = con.execute("SELECT COUNT(*) FROM _daily").fetchone()[0]
    log(f"  {daily_count:,} daily rows")

    log("Loading market cap data...")

    con.execute(f"""
        CREATE TABLE _market_cap AS
        SELECT
            replace(string_split(filename, '/')[-1], '.csv', '') AS ticker,
            CAST(date AS DATE) AS day,
            CAST(market_cap AS BIGINT) AS cap
        FROM read_csv(
            '{csv_pattern}',
            header=true,
            columns={{'date': 'DATE', 'market_cap': 'BIGINT'}},
            filename=true
        )
        WHERE ticker != ''
          AND ticker IN (SELECT ticker FROM sqlite_scan('{tickers_db}', 'tickers'))
          AND cap > 0
          AND cap < 20000000000000
    """)

    mc_count = con.execute("SELECT COUNT(*) FROM _market_cap").fetchone()[0]
    log(f"  {mc_count:,} market cap rows")

    log("Enriching daily aggs with market cap, global trading_day_num, and cumulative sums...")

    con.execute(f"""
        CREATE TABLE _enriched AS
        SELECT
            d.ticker,
            d.day,
            d.open,
            d.high,
            d.low,
            d.close,
            d.volume,
            mc.cap,
            cal.trading_day_num,
            SUM(d.close)  OVER w AS cum_close,
            SUM(d.high)   OVER w AS cum_high,
            SUM(d.low)    OVER w AS cum_low,
            SUM(d.volume::DOUBLE) OVER w AS cum_volume
        FROM _daily d
        INNER JOIN sqlite_scan('{calendar_db}', 'trading_calendar') cal
          ON cal.day = strftime(d.day, '%Y%m%d')::INTEGER
        LEFT JOIN _market_cap mc
          ON mc.ticker = d.ticker AND mc.day = d.day
        WINDOW w AS (PARTITION BY d.ticker ORDER BY d.day)
    """)

    con.execute("DROP TABLE _daily")
    con.execute("DROP TABLE _market_cap")

    enriched_count = con.execute("SELECT COUNT(*) FROM _enriched").fetchone()[0]
    log(f"  {enriched_count:,} enriched rows")

    # Get distinct years
    years = [r[0] for r in con.execute(
        "SELECT DISTINCT YEAR(day) AS yr FROM _enriched ORDER BY yr"
    ).fetchall()]

    total_years = len(years)
    total_rows = 0

    for yr_idx, year in enumerate(years):
        rows = con.execute(f"""
            SELECT
                ticker, strftime(day, '%Y%m%d')::INTEGER, open, high, low, close, volume::BIGINT, cap::BIGINT,
                trading_day_num, cum_close, cum_high, cum_low, cum_volume
            FROM _enriched
            WHERE YEAR(day) = {year}
            ORDER BY ticker, trading_day_num
        """).fetchall()

        db_path = OUTPUT_DIR / f"{year}.db"
        sconn = sqlite3.connect(str(db_path))
        sconn.execute("PRAGMA journal_mode = WAL")
        sconn.execute("PRAGMA foreign_keys = ON")
        create_table(sconn, "daily_aggs_enriched", with_indexes=False)
        sconn.executemany(
            "INSERT INTO daily_aggs_enriched VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            rows,
        )
        create_indexes(sconn, "daily_aggs_enriched")
        sconn.commit()
        sconn.close()

        optimize_db(str(db_path))
        count = verify_table(str(db_path), "daily_aggs_enriched")
        total_rows += count
        log(f"  [{yr_idx + 1}/{total_years}] {year}.db: {count:,} rows")

    con.execute("DROP TABLE _enriched")

    log(f"  Wrote {total_rows:,} total daily_aggs_enriched rows")
