"""Step 8: Build daily_aggs_enriched — daily OHLCV with market cap and cumulative sums."""

import shutil
from pathlib import Path

from build_common import (
    MARKET_CAP_DIR, OUTPUT_DIR, PARQUET_SETTINGS, log, step, verify_parquet,
)


@step("daily_aggs_enriched_v2", target="daily_aggs_enriched", depends_on=("prices", "trading_calendar"))
def build_daily_aggs_enriched(con):
    """Daily OHLCV enriched with market cap, global trading day number, and cumulative sums."""
    prices_pattern = str(OUTPUT_DIR / "prices" / "**" / "*.parquet")
    csv_pattern = str(MARKET_CAP_DIR / "*.csv")
    tickers_path = str(OUTPUT_DIR / "tickers.parquet")
    calendar_path = str(OUTPUT_DIR / "trading_calendar.parquet")
    enriched_dir = OUTPUT_DIR / "daily_aggs_enriched"
    building_dir = OUTPUT_DIR / "daily_aggs_enriched_building"

    if building_dir.exists():
        shutil.rmtree(building_dir)
    building_dir.mkdir(parents=True)

    log("Aggregating hourly prices into daily OHLCV...")

    con.execute(f"""
        CREATE TABLE _daily AS
        SELECT
            ticker,
            CAST(ts AS DATE) AS day,
            FIRST(open ORDER BY ts) AS open,
            MAX(high) AS high,
            MIN(low) AS low,
            LAST(close ORDER BY ts) AS close,
            SUM(volume)::BIGINT AS volume
        FROM read_parquet('{prices_pattern}', hive_partitioning=true)
        GROUP BY ticker, CAST(ts AS DATE)
    """)

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
          AND ticker IN (SELECT ticker FROM read_parquet('{tickers_path}'))
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
        INNER JOIN read_parquet('{calendar_path}') cal
          ON cal.day = d.day
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
        out_dir = building_dir / f"year={year}"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / "data.parquet"

        con.execute(f"""
            COPY (
                SELECT
                    ticker, day, open, high, low, close, volume, cap,
                    trading_day_num, cum_close, cum_high, cum_low, cum_volume
                FROM _enriched
                WHERE YEAR(day) = {year}
                ORDER BY trading_day_num, ticker
            ) TO '{out_path}' ({PARQUET_SETTINGS})
        """)

        count = verify_parquet(str(out_path))
        total_rows += count
        log(f"  [{yr_idx + 1}/{total_years}] year={year}: {count:,} rows")

    con.execute("DROP TABLE _enriched")

    # Atomic swap
    if enriched_dir.exists():
        stale = Path(str(enriched_dir) + "_old")
        enriched_dir.rename(stale)
        building_dir.rename(enriched_dir)
        shutil.rmtree(stale)
    else:
        building_dir.rename(enriched_dir)

    log(f"  Wrote {total_rows:,} total daily_aggs_enriched rows")
