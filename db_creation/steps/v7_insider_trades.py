"""Step 7: Read JSONL.GZ insider trade filings via DuckDB JSON reader."""

from pathlib import Path

from build_common import INSIDER_TRADES_DIR, OUTPUT_DIR, PARQUET_SETTINGS, log, step, verify_parquet


@step("insider_trades_v2", target="insider_trades", depends_on=("tickers",))
def build_insider_trades(con):
    """Read JSONL.GZ insider trade files via DuckDB's native JSON reader + UNNEST."""
    dest = OUTPUT_DIR / "insider_trades.parquet"
    tmp = Path(str(dest) + ".tmp")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    jsonl_pattern = str(INSIDER_TRADES_DIR / "**" / "*.jsonl.gz")

    log("Building insider_trades from JSONL.GZ files...")

    con.execute(f"""
        COPY (
            SELECT
                upper(trim(issuer.tradingSymbol)) AS ticker,
                COALESCE(tx.transactionDate, periodOfReport) AS trade_date,
                tx.coding.code AS tx_code,
                CAST(tx.amounts.shares AS FLOAT) AS shares,
                CAST(tx.amounts.shares * tx.amounts.pricePerShare AS FLOAT) AS total_value,
                CASE
                    WHEN tx.amounts.acquiredDisposedCode IN ('A', 'D')
                        THEN tx.amounts.acquiredDisposedCode
                    WHEN tx.coding.code = 'P' THEN 'A'
                    ELSE 'D'
                END AS acquired_disposed,
                CAST(tx.postTransactionAmounts.sharesOwnedFollowingTransaction AS FLOAT) AS shares_after,
                CASE
                    WHEN tx.ownershipNature.directOrIndirectOwnership IN ('D', 'I')
                        THEN tx.ownershipNature.directOrIndirectOwnership
                    ELSE 'D'
                END AS ownership_type,
                COALESCE(reportingOwner.relationship.isDirector, false) AS is_director,
                COALESCE(reportingOwner.relationship.isOfficer, false) AS is_officer,
                COALESCE(reportingOwner.relationship.isTenPercentOwner, false) AS is_ten_pct_owner,
                reportingOwner.name AS insider_name,
                reportingOwner.cik AS insider_cik,
                reportingOwner.relationship.officerTitle AS officer_title
            FROM read_json(
                '{jsonl_pattern}',
                format='newline_delimited',
                ignore_errors=true
            )
            , LATERAL UNNEST(nonDerivativeTable.transactions) AS t(tx)
            WHERE tx.coding.code IN ('P', 'S')
              AND tx.amounts.shares IS NOT NULL
              AND upper(trim(issuer.tradingSymbol)) != ''
              AND upper(trim(issuer.tradingSymbol)) IN (SELECT ticker FROM read_parquet('{OUTPUT_DIR / "tickers.parquet"}'))
              AND COALESCE(tx.transactionDate, periodOfReport) IS NOT NULL
              AND EXTRACT(YEAR FROM COALESCE(tx.transactionDate, periodOfReport)) BETWEEN 2000 AND 2026
            ORDER BY ticker, trade_date
        ) TO '{tmp}' ({PARQUET_SETTINGS})
    """)
    tmp.rename(dest)

    count = verify_parquet(str(dest))
    log(f"  Wrote {count:,} rows to insider_trades.parquet")
