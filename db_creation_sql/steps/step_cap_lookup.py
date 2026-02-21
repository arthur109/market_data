"""Step 12: Build cap_lookup — daily_aggs_enriched rows with cap, sorted by (trading_day_num, ticker)."""

import sqlite3

from build_common import (
    OUTPUT_DIR, create_table, get_year_dbs,
    install_duckdb_sqlite, log, optimize_db, step, verify_table,
)


@step("cap_lookup_v2", target="cap_lookup", depends_on=("daily_aggs_enriched",))
def build_cap_lookup(con):
    """Side table with rows that have market cap, sorted by (trading_day_num, ticker) for fast day+cap queries."""
    year_dbs = get_year_dbs()

    install_duckdb_sqlite(con)

    log("Building cap_lookup from daily_aggs_enriched (cap IS NOT NULL)...")

    # Load all daily_aggs_enriched from year dbs (day is already INTEGER)
    parts = [
        f"SELECT trading_day_num, ticker, day, cap, close, cum_close FROM sqlite_scan('{p}', 'daily_aggs_enriched')"
        for _, p in year_dbs
    ]
    con.execute(f"""
        CREATE TABLE _cap_lookup AS
        SELECT * FROM ({' UNION ALL '.join(parts)})
        WHERE cap IS NOT NULL
    """)

    total = con.execute("SELECT COUNT(*) FROM _cap_lookup").fetchone()[0]
    log(f"  {total:,} rows with market cap")

    years = [r[0] for r in con.execute(
        "SELECT DISTINCT (day // 10000)::INTEGER AS yr FROM _cap_lookup ORDER BY yr"
    ).fetchall()]

    total_years = len(years)
    total_rows = 0

    for yr_idx, year in enumerate(years):
        rows = con.execute(f"""
            SELECT trading_day_num, ticker, day, cap, close, cum_close
            FROM _cap_lookup
            WHERE day // 10000 = {year}
            ORDER BY trading_day_num, ticker
        """).fetchall()

        db_path = OUTPUT_DIR / f"{year}.db"
        sconn = sqlite3.connect(str(db_path))
        sconn.execute("PRAGMA journal_mode = WAL")
        sconn.execute("PRAGMA foreign_keys = ON")
        # WITHOUT ROWID table with PK (trading_day_num, ticker) — no separate indexes needed
        create_table(sconn, "cap_lookup")
        sconn.executemany("INSERT INTO cap_lookup VALUES (?, ?, ?, ?, ?, ?)", rows)
        sconn.commit()
        sconn.close()

        optimize_db(str(db_path))
        count = verify_table(str(db_path), "cap_lookup")
        total_rows += count
        log(f"  [{yr_idx + 1}/{total_years}] {year}.db: {count:,} rows")

    con.execute("DROP TABLE _cap_lookup")

    log(f"  Wrote {total_rows:,} total cap_lookup rows")
