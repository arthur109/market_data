"""Step 5: Aggregate daily data into 100-trading-day blocks."""

from pathlib import Path

from build_common import OUTPUT_DIR, PARQUET_SETTINGS, log, step, verify_parquet


@step("hundred_day_aggs_v1", target="hundred_day_aggs", depends_on=("daily_aggs",))
def build_hundred_day_aggs(con):
    """Aggregate daily data into 100-trading-day blocks."""
    daily_path = str(OUTPUT_DIR / "daily_aggs" / "**" / "*.parquet")
    dest = OUTPUT_DIR / "hundred_day_aggs.parquet"
    tmp = Path(str(dest) + ".tmp")

    log("Building 100-day aggregates...")

    con.execute(f"""
        COPY (
            WITH numbered AS (
                SELECT *,
                    (ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY day) - 1) // 100 AS block_id
                FROM read_parquet('{daily_path}', hive_partitioning=true)
            )
            SELECT
                ticker,
                MIN(day) AS block_start,
                MAX(day) AS block_end,

                -- Block OHLCV
                FIRST(open ORDER BY day) AS open,
                MAX(high) AS high,
                MIN(low) AS low,
                LAST(close ORDER BY day) AS close,
                SUM(volume)::BIGINT AS volume,

                -- Component sums
                SUM(sum_open) AS sum_open,
                SUM(sum_high) AS sum_high,
                SUM(sum_low) AS sum_low,
                SUM(sum_close) AS sum_close,
                SUM(sum_volume)::BIGINT AS sum_volume,
                SUM(cnt)::USMALLINT AS cnt,
                COUNT(*)::UTINYINT AS day_cnt

            FROM numbered
            GROUP BY ticker, block_id
            ORDER BY ticker, block_start
        ) TO '{tmp}' ({PARQUET_SETTINGS})
    """)
    tmp.rename(dest)

    count = verify_parquet(str(dest))
    log(f"  Wrote {count:,} rows to hundred_day_aggs.parquet")
