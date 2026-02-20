"""Step 9: Build insider_purchases — SEC Form 4 open-market purchases by filing date."""

import shutil
from pathlib import Path

from build_common import (
    INSIDER_TRADES_DIR, OUTPUT_DIR, PARQUET_SETTINGS, log, step, verify_parquet,
)


@step("insider_purchases_v2", target="insider_purchases", depends_on=("tickers", "trading_calendar"))
def build_insider_purchases(con):
    """SEC Form 4 open-market purchases, partitioned by filing date year."""
    tickers_path = str(OUTPUT_DIR / "tickers.parquet")
    calendar_path = str(OUTPUT_DIR / "trading_calendar.parquet")
    jsonl_pattern = str(INSIDER_TRADES_DIR / "**" / "*.jsonl.gz")
    purchases_dir = OUTPUT_DIR / "insider_purchases"
    building_dir = OUTPUT_DIR / "insider_purchases_building"

    if building_dir.exists():
        shutil.rmtree(building_dir)
    building_dir.mkdir(parents=True)

    log("Reading insider trades from JSONL.GZ files (purchases only)...")

    con.execute(f"""
        CREATE TABLE _purchases AS
        SELECT
            CAST(filedAt AS DATE) AS filing_date,
            upper(trim(issuer.tradingSymbol)) AS ticker,
            reportingOwner.cik AS insider_cik,
            CAST(tx.amounts.shares * tx.amounts.pricePerShare AS FLOAT) AS total_value,
            CAST(tx.amounts.shares AS FLOAT) AS shares,
            COALESCE(reportingOwner.relationship.isDirector, false) AS is_director,
            COALESCE(reportingOwner.relationship.isOfficer, false) AS is_officer,
            COALESCE(reportingOwner.relationship.isTenPercentOwner, false) AS is_ten_pct_owner,
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
              SELECT ticker FROM read_parquet('{tickers_path}')
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
            p.*,
            cal.trading_day_num
        FROM _purchases p
        ASOF JOIN read_parquet('{calendar_path}') cal
          ON p.filing_date <= cal.day
    """)

    con.execute("DROP TABLE _purchases")

    matched = con.execute("SELECT COUNT(*) FROM _purchases_tdn WHERE trading_day_num IS NOT NULL").fetchone()[0]
    log(f"  {matched:,} / {total:,} rows matched to trading days")

    # Get distinct years from filing_date
    years = [r[0] for r in con.execute(
        "SELECT DISTINCT YEAR(filing_date) AS yr FROM _purchases_tdn ORDER BY yr"
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
                    filing_date, ticker, insider_cik, total_value, shares,
                    is_director, is_officer, is_ten_pct_owner,
                    officer_title, insider_name, trading_day_num
                FROM _purchases_tdn
                WHERE YEAR(filing_date) = {year}
                ORDER BY trading_day_num, ticker
            ) TO '{out_path}' ({PARQUET_SETTINGS})
        """)

        count = verify_parquet(str(out_path))
        total_rows += count
        log(f"  [{yr_idx + 1}/{total_years}] year={year}: {count:,} rows")

    con.execute("DROP TABLE _purchases_tdn")

    # Atomic swap
    if purchases_dir.exists():
        stale = Path(str(purchases_dir) + "_old")
        purchases_dir.rename(stale)
        building_dir.rename(purchases_dir)
        shutil.rmtree(stale)
    else:
        building_dir.rename(purchases_dir)

    log(f"  Wrote {total_rows:,} total insider_purchases rows")
