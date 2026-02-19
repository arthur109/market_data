"""Step 10: Build trading_calendar — every US equity trading day."""

from pathlib import Path

from build_common import OUTPUT_DIR, PARQUET_SETTINGS, log, step, verify_parquet


@step("trading_calendar_v1", target="trading_calendar", depends_on=("daily_aggs_enriched",))
def build_trading_calendar(con):
    """Every US equity trading day, derived from SPY + AAPL trading days."""
    enriched_pattern = str(OUTPUT_DIR / "daily_aggs_enriched" / "**" / "*.parquet")
    dest = OUTPUT_DIR / "trading_calendar.parquet"
    tmp = Path(str(dest) + ".tmp")

    log("Building trading calendar from SPY + AAPL trading days...")

    con.execute(f"""
        COPY (
            SELECT DISTINCT day
            FROM read_parquet('{enriched_pattern}', hive_partitioning=true)
            WHERE ticker IN ('SPY', 'AAPL')
            ORDER BY day
        ) TO '{tmp}' ({PARQUET_SETTINGS})
    """)
    tmp.rename(dest)

    count = verify_parquet(str(dest))
    log(f"  Wrote {count:,} trading days to trading_calendar.parquet")
