"""Step 3: Aggregate hourly prices into daily OHLCV + component sums (Hive-partitioned by year)."""

import shutil
from pathlib import Path

from build_common import OUTPUT_DIR, PARQUET_SETTINGS, log, step, verify_parquet


@step("daily_aggs_v2", target="daily_aggs", depends_on=("prices",))
def build_daily_aggs(con):
    """Aggregate hourly prices into daily OHLCV + component sums, partitioned by year."""
    prices_pattern = str(OUTPUT_DIR / "prices" / "**" / "*.parquet")
    daily_dir = OUTPUT_DIR / "daily_aggs"
    building_dir = OUTPUT_DIR / "daily_aggs_building"

    # Clean prior building state
    if building_dir.exists():
        shutil.rmtree(building_dir)
    building_dir.mkdir(parents=True)

    log("Building daily aggregates from all price data...")

    # Get distinct years from prices
    years = [r[0] for r in con.execute(f"""
        SELECT DISTINCT year
        FROM read_parquet('{prices_pattern}', hive_partitioning=true)
        ORDER BY year
    """).fetchall()]

    total_years = len(years)
    total_rows = 0

    for yr_idx, year in enumerate(years):
        year_prices = str(OUTPUT_DIR / "prices" / f"year={year}" / "*.parquet")
        out_dir = building_dir / f"year={year}"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / "data.parquet"

        con.execute(f"""
            COPY (
                SELECT
                    ticker,
                    CAST(ts AS DATE) AS day,

                    -- Day's OHLCV (first/last by ts, max/min/sum)
                    FIRST(open ORDER BY ts) AS open,
                    MAX(high) AS high,
                    MIN(low) AS low,
                    LAST(close ORDER BY ts) AS close,
                    SUM(volume)::BIGINT AS volume,

                    -- Component sums
                    SUM(open) AS sum_open,
                    SUM(high) AS sum_high,
                    SUM(low) AS sum_low,
                    SUM(close) AS sum_close,
                    SUM(volume)::BIGINT AS sum_volume,
                    COUNT(*)::UTINYINT AS cnt

                FROM read_parquet('{year_prices}')
                GROUP BY ticker, CAST(ts AS DATE)
                ORDER BY ticker, day
            ) TO '{out_path}' ({PARQUET_SETTINGS})
        """)

        count = verify_parquet(str(out_path))
        total_rows += count
        log(f"  [{yr_idx + 1}/{total_years}] year={year}: {count:,} rows")

    # Swap in the final directory
    # Remove stale single-file artifact if it exists from prior version
    old_file = OUTPUT_DIR / "daily_aggs.parquet"
    if old_file.exists():
        old_file.unlink()

    if daily_dir.exists():
        stale = Path(str(daily_dir) + "_old")
        daily_dir.rename(stale)
        building_dir.rename(daily_dir)
        shutil.rmtree(stale)
    else:
        building_dir.rename(daily_dir)

    log(f"  Wrote {total_rows:,} total daily agg rows")
