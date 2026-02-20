"""Step 11: Verify trading_calendar — global trading day lookup produced by prices step."""

from build_common import OUTPUT_DIR, log, step, verify_parquet


@step("trading_calendar_v3", target="trading_calendar", depends_on=("prices",))
def verify_trading_calendar(con):
    """Verify trading_calendar.parquet written by the prices step."""
    dest = OUTPUT_DIR / "trading_calendar.parquet"

    if not dest.exists():
        raise RuntimeError(
            "trading_calendar.parquet not found — prices step should have created it"
        )

    count = verify_parquet(str(dest))

    result = con.execute(f"""
        SELECT MIN(day), MAX(day), MIN(trading_day_num), MAX(trading_day_num)
        FROM read_parquet('{dest}')
    """).fetchone()

    log(f"  {count:,} trading days ({result[0]} to {result[1]})")
    log(f"  trading_day_num range: {result[2]} to {result[3]}")
