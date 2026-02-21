"""Step 11: Verify trading_calendar — staging_calendar.db produced by prices step."""

from build_common import OUTPUT_DIR, log, step, verify_table

import sqlite3


@step("trading_calendar_v3", target="trading_calendar", depends_on=("prices",))
def verify_trading_calendar(con):
    """Verify staging_calendar.db written by the prices step."""
    dest = OUTPUT_DIR / "staging_calendar.db"

    if not dest.exists():
        raise RuntimeError(
            "staging_calendar.db not found — prices step should have created it"
        )

    count = verify_table(str(dest), "trading_calendar")

    sconn = sqlite3.connect(str(dest))
    result = sconn.execute("""
        SELECT MIN(day), MAX(day), MIN(trading_day_num), MAX(trading_day_num)
        FROM trading_calendar
    """).fetchone()
    sconn.close()

    log(f"  {count:,} trading days ({result[0]} to {result[1]})")
    log(f"  trading_day_num range: {result[2]} to {result[3]}")
