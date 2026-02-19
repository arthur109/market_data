"""Step 6: Read market cap CSVs into a single Parquet file."""

from pathlib import Path

from build_common import MARKET_CAP_DIR, OUTPUT_DIR, PARQUET_SETTINGS, log, step, verify_parquet


@step("market_cap_v2", target="market_cap", depends_on=("tickers",))
def build_market_cap(con):
    """Read all market cap CSVs via DuckDB glob."""
    csv_pattern = str(MARKET_CAP_DIR / "*.csv")
    dest = OUTPUT_DIR / "market_cap.parquet"
    tmp = Path(str(dest) + ".tmp")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    log("Building market_cap from CSV files...")

    con.execute(f"""
        COPY (
            SELECT
                replace(string_split(filename, '/')[-1], '.csv', '') AS ticker,
                CAST(date AS DATE) AS day,
                CAST(market_cap AS BIGINT) AS cap
            FROM read_csv(
                '{csv_pattern}',
                header=true,
                columns={{'date': 'DATE', 'market_cap': 'BIGINT'}},
                filename=true
            )
            WHERE ticker != ''
              AND ticker IN (SELECT ticker FROM read_parquet('{OUTPUT_DIR / "tickers.parquet"}'))
              AND cap > 0
              AND cap < 20000000000000
            ORDER BY ticker, day
        ) TO '{tmp}' ({PARQUET_SETTINGS})
    """)
    tmp.rename(dest)

    count = verify_parquet(str(dest))
    log(f"  Wrote {count:,} rows to market_cap.parquet")
