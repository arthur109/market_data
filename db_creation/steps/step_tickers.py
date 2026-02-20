"""Step 1: Scan ZIP filenames to discover all tickers."""

from pathlib import Path

from build_common import (
    ETFS_ZIP_DIR, OUTPUT_DIR, PARQUET_SETTINGS, STOCKS_ZIP_DIR,
    discover_tickers_from_zips, log, step, verify_parquet,
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

    # Write via DuckDB
    dest = OUTPUT_DIR / "tickers.parquet"
    tmp = Path(str(dest) + ".tmp")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    con.execute("CREATE TABLE _tickers (ticker VARCHAR, asset_type VARCHAR)")
    con.executemany("INSERT INTO _tickers VALUES (?, ?)", rows)
    con.execute(f"""
        COPY (SELECT * FROM _tickers ORDER BY ticker)
        TO '{tmp}' ({PARQUET_SETTINGS})
    """)
    con.execute("DROP TABLE _tickers")
    tmp.rename(dest)

    count = verify_parquet(str(dest))
    log(f"  Wrote {count} rows to tickers.parquet")
