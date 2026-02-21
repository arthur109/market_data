"""Step 1: Scan ZIP filenames to discover all tickers."""

import sqlite3
from pathlib import Path

from build_common import (
    ETFS_ZIP_DIR, OUTPUT_DIR, STOCKS_ZIP_DIR,
    discover_tickers_from_zips, log, optimize_db, step, verify_table,
)


@step("tickers_v1", target="tickers")
def build_tickers(con):
    """Scan ZIP filenames to discover all tickers. ETF wins on overlap."""
    log("Discovering tickers from stock ZIPs...")
    stock_tickers = discover_tickers_from_zips(STOCKS_ZIP_DIR)
    log(f"  Found {len(stock_tickers)} stock tickers")

    log("Discovering tickers from ETF ZIPs...")
    etf_tickers = discover_tickers_from_zips(ETFS_ZIP_DIR)
    log(f"  Found {len(etf_tickers)} ETF tickers")

    # Build rows: ETF wins on overlap
    rows = []
    all_tickers = sorted(set(stock_tickers) | set(etf_tickers))
    overlap_count = 0
    for ticker in all_tickers:
        if ticker in etf_tickers:
            asset_type = "etf"
            if ticker in stock_tickers:
                overlap_count += 1
        else:
            asset_type = "stock"
        rows.append((ticker, asset_type))

    log(f"  Total: {len(rows)} tickers ({overlap_count} overlap, classified as ETF)")

    # Write to staging_tickers.db
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    dest = OUTPUT_DIR / "staging_tickers.db"
    tmp = Path(str(dest) + ".tmp")
    if tmp.exists():
        tmp.unlink()

    sconn = sqlite3.connect(str(tmp))
    sconn.execute("PRAGMA journal_mode = WAL")
    sconn.execute("""
        CREATE TABLE tickers (
            ticker TEXT PRIMARY KEY,
            asset_type TEXT NOT NULL
        ) WITHOUT ROWID, STRICT
    """)
    sconn.executemany("INSERT INTO tickers VALUES (?, ?)", rows)
    sconn.commit()
    sconn.close()

    optimize_db(str(tmp))
    tmp.rename(dest)

    count = verify_table(str(dest), "tickers")
    log(f"  Wrote {count} rows to staging_tickers.db")
