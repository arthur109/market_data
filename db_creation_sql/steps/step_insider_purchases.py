"""Step 9: Build insider_purchases — SEC Form 4 open-market purchases by filing date."""

import sqlite3

from build_common import (
    INSIDER_TRADES_DIR, OUTPUT_DIR,
    create_indexes, create_table, install_duckdb_sqlite,
    log, optimize_db, step, verify_table,
)


@step("insider_purchases_v2", target="insider_purchases", depends_on=("tickers", "trading_calendar"))
def build_insider_purchases(con):
    """SEC Form 4 open-market purchases, written to year dbs by filing date year."""
    tickers_db = str(OUTPUT_DIR / "staging_tickers.db")
    calendar_db = str(OUTPUT_DIR / "staging_calendar.db")
    jsonl_pattern = str(INSIDER_TRADES_DIR / "**" / "*.jsonl.gz")

    install_duckdb_sqlite(con)

    log("Reading insider trades from JSONL.GZ files (purchases only)...")

    con.execute(f"""
        CREATE TABLE _purchases AS
        SELECT
            CAST(filedAt AS DATE) AS filing_date,
            upper(trim(issuer.tradingSymbol)) AS ticker,
            reportingOwner.cik AS insider_cik,
            CAST(tx.amounts.shares * tx.amounts.pricePerShare AS FLOAT) AS total_value,
            CAST(tx.amounts.shares AS FLOAT) AS shares,
            COALESCE(reportingOwner.relationship.isDirector, false)::INTEGER AS is_director,
            COALESCE(reportingOwner.relationship.isOfficer, false)::INTEGER AS is_officer,
            COALESCE(reportingOwner.relationship.isTenPercentOwner, false)::INTEGER AS is_ten_pct_owner,
            reportingOwner.relationship.officerTitle AS officer_title,
            reportingOwner.name AS insider_name
        FROM read_json(
            '{jsonl_pattern}',
            format='newline_delimited',
            ignore_errors=true
        )
        , LATERAL UNNEST(nonDerivativeTable.transactions) AS t(tx)
        WHERE tx.coding.code = 'P'
          AND tx.amounts.shares IS NOT NULL
          AND upper(trim(issuer.tradingSymbol)) != ''
          AND upper(trim(issuer.tradingSymbol)) IN (
              SELECT ticker FROM sqlite_scan('{tickers_db}', 'tickers')
          )
          AND filedAt IS NOT NULL
          AND EXTRACT(YEAR FROM CAST(filedAt AS DATE)) BETWEEN 2000 AND 2026
    """)

    total = con.execute("SELECT COUNT(*) FROM _purchases").fetchone()[0]
    log(f"  {total:,} purchase rows")

    log("Adding trading_day_num via ASOF join with trading calendar...")

    con.execute(f"""
        CREATE TABLE _purchases_tdn AS
        SELECT
            p.filing_date,
            p.ticker,
            p.insider_cik,
            p.total_value,
            p.shares,
            p.is_director,
            p.is_officer,
            p.is_ten_pct_owner,
            p.officer_title,
            p.insider_name,
            cal.trading_day_num
        FROM _purchases p
        ASOF JOIN (
            SELECT trading_day_num, strptime(CAST(day AS VARCHAR), '%Y%m%d')::DATE AS day
            FROM sqlite_scan('{calendar_db}', 'trading_calendar')
        ) cal
          ON p.filing_date <= cal.day
    """)

    con.execute("DROP TABLE _purchases")

    matched = con.execute("SELECT COUNT(*) FROM _purchases_tdn WHERE trading_day_num IS NOT NULL").fetchone()[0]
    log(f"  {matched:,} / {total:,} rows matched to trading days")

    # Get distinct years from filing_date
    years = [r[0] for r in con.execute(
        "SELECT DISTINCT EXTRACT(YEAR FROM filing_date)::INTEGER AS yr FROM _purchases_tdn ORDER BY yr"
    ).fetchall()]

    total_years = len(years)
    total_rows = 0

    for yr_idx, year in enumerate(years):
        rows = con.execute(f"""
            SELECT
                strftime(filing_date, '%Y%m%d')::INTEGER, ticker, insider_cik, total_value, shares,
                is_director, is_officer, is_ten_pct_owner,
                officer_title, insider_name, trading_day_num
            FROM _purchases_tdn
            WHERE EXTRACT(YEAR FROM filing_date) = {year}
            ORDER BY trading_day_num, ticker
        """).fetchall()

        db_path = OUTPUT_DIR / f"{year}.db"
        sconn = sqlite3.connect(str(db_path))
        sconn.execute("PRAGMA journal_mode = WAL")
        sconn.execute("PRAGMA foreign_keys = ON")
        create_table(sconn, "insider_purchases", with_indexes=False)
        sconn.executemany(
            "INSERT INTO insider_purchases VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            rows,
        )
        create_indexes(sconn, "insider_purchases")
        sconn.commit()
        sconn.close()

        optimize_db(str(db_path))
        count = verify_table(str(db_path), "insider_purchases")
        total_rows += count
        log(f"  [{yr_idx + 1}/{total_years}] {year}.db: {count:,} rows")

    con.execute("DROP TABLE _purchases_tdn")

    log(f"  Wrote {total_rows:,} total insider_purchases rows")
