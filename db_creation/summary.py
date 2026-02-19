#!/usr/bin/env python3
"""
Data summary / preview for market data parquet files.

Prints a human-readable overview of each table so you can eyeball whether the
data looks reasonable or if something went wrong.

Usage:
    python summary.py                   # summarize all tables
    python summary.py prices            # summarize one table
    python summary.py tickers prices    # summarize specific tables
"""

import argparse
import os
import sys
from pathlib import Path

import duckdb

DB_DIR = Path(__file__).resolve().parent.parent / "db"

con = duckdb.connect(":memory:")


def q(sql):
    return con.execute(sql).fetchall()


def q1(sql):
    rows = q(sql)
    return rows[0][0] if rows else None


def fmt(n):
    if n is None:
        return "N/A"
    if isinstance(n, float):
        return f"{n:,.2f}"
    return f"{n:,}"


def file_size(path):
    if path.is_dir():
        total = sum(f.stat().st_size for f in path.rglob("*") if f.is_file())
    elif path.exists():
        total = path.stat().st_size
    else:
        return "N/A"
    for unit in ("B", "KB", "MB", "GB"):
        if total < 1024:
            return f"{total:.0f}{unit}" if unit == "B" else f"{total:.1f}{unit}"
        total /= 1024
    return f"{total:.1f}TB"


def section(title):
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print(f"{'=' * 60}")


def print_schema(parquet_expr):
    """Print the schema of a parquet source using DESCRIBE."""
    cols = q(f"DESCRIBE SELECT * FROM {parquet_expr}")
    parts = [f"{name} ({dtype})" for name, dtype, *_ in cols]
    print(f"  Schema: {', '.join(parts)}")


# ---------------------------------------------------------------------------
# Tickers
# ---------------------------------------------------------------------------

def summarize_tickers():
    p = DB_DIR / "tickers.parquet"
    if not p.exists():
        section("TICKERS — not found"); return
    section("TICKERS")
    print_schema(f"read_parquet('{p}')")

    total = q1(f"SELECT COUNT(*) FROM read_parquet('{p}')")
    breakdown = q(f"""
        SELECT asset_type, COUNT(*) FROM read_parquet('{p}')
        GROUP BY asset_type ORDER BY asset_type
    """)
    parts = ", ".join(f"{fmt(c)} {t}" for t, c in breakdown)
    print(f"  Rows: {fmt(total)}  ({parts})")
    print(f"  File: {file_size(p)}")

    sample = q(f"SELECT ticker, asset_type FROM read_parquet('{p}') USING SAMPLE 10")
    print(f"  Sample: {', '.join(f'{t}({a})' for t, a in sample)}")


# ---------------------------------------------------------------------------
# Prices
# ---------------------------------------------------------------------------

def summarize_prices():
    d = DB_DIR / "prices"
    if not d.exists():
        section("PRICES — not found"); return
    section("PRICES")
    pp = str(d / "**" / "*.parquet")
    print_schema(f"read_parquet('{pp}', hive_partitioning=true)")

    total = q1(f"SELECT COUNT(*) FROM read_parquet('{pp}', hive_partitioning=true)")
    tickers = q1(f"SELECT COUNT(DISTINCT ticker) FROM read_parquet('{pp}', hive_partitioning=true)")
    yr_range = q(f"""
        SELECT MIN(year), MAX(year) FROM read_parquet('{pp}', hive_partitioning=true)
    """)[0]
    print(f"  Rows: {fmt(total)} | Tickers: {fmt(tickers)} | Years: {yr_range[0]}-{yr_range[1]}")
    print(f"  Total size: {file_size(d)}")

    print(f"\n  {'Year':>6} {'Rows':>14} {'Tickers':>9} {'Size':>8} {'Rows/Ticker':>12}")
    print(f"  {'-'*53}")
    per_year = q(f"""
        SELECT year, COUNT(*) as rows, COUNT(DISTINCT ticker) as tickers
        FROM read_parquet('{pp}', hive_partitioning=true)
        GROUP BY year ORDER BY year
    """)
    for yr, rows, tkrs in per_year:
        yr_dir = d / f"year={yr}"
        sz = file_size(yr_dir)
        rpt = rows // tkrs if tkrs else 0
        print(f"  {yr:>6} {fmt(rows):>14} {fmt(tkrs):>9} {sz:>8} {fmt(rpt):>12}")

    sample = q(f"""
        SELECT ticker, ts, open, high, low, close, volume
        FROM read_parquet('{pp}', hive_partitioning=true) USING SAMPLE 5
    """)
    print(f"\n  Sample rows:")
    for t, ts, o, h, l, c, v in sample:
        print(f"    {t:>6} | {ts} | O:{o:.2f} H:{h:.2f} L:{l:.2f} C:{c:.2f} | V:{fmt(v)}")


# ---------------------------------------------------------------------------
# Daily Aggs
# ---------------------------------------------------------------------------

def summarize_daily_aggs():
    d = DB_DIR / "daily_aggs"
    if not d.exists():
        section("DAILY AGGS — not found"); return
    section("DAILY AGGS")
    pp = str(d / "**" / "*.parquet")
    print_schema(f"read_parquet('{pp}', hive_partitioning=true)")

    total = q1(f"SELECT COUNT(*) FROM read_parquet('{pp}', hive_partitioning=true)")
    tickers = q1(f"SELECT COUNT(DISTINCT ticker) FROM read_parquet('{pp}', hive_partitioning=true)")
    date_range = q(f"SELECT MIN(day), MAX(day) FROM read_parquet('{pp}', hive_partitioning=true)")[0]
    print(f"  Rows: {fmt(total)} | Tickers: {fmt(tickers)} | Dates: {date_range[0]} to {date_range[1]}")
    print(f"  Total size: {file_size(d)}")

    cnt_stats = q(f"""
        SELECT MIN(cnt), MEDIAN(cnt)::INT, MAX(cnt),
               ROUND(AVG(cnt), 1)
        FROM read_parquet('{pp}', hive_partitioning=true)
    """)[0]
    print(f"  Bars per day — min: {cnt_stats[0]}, median: {cnt_stats[1]}, max: {cnt_stats[2]}, avg: {cnt_stats[3]}")

    days_per_ticker = q(f"""
        SELECT MIN(days), MEDIAN(days)::INT, MAX(days)
        FROM (SELECT COUNT(*) as days FROM read_parquet('{pp}', hive_partitioning=true) GROUP BY ticker)
    """)[0]
    print(f"  Days per ticker — min: {fmt(days_per_ticker[0])}, median: {fmt(days_per_ticker[1])}, max: {fmt(days_per_ticker[2])}")

    print(f"\n  {'Year':>6} {'Rows':>14} {'Tickers':>9} {'Size':>8}")
    print(f"  {'-'*41}")
    per_year = q(f"""
        SELECT year, COUNT(*) as rows, COUNT(DISTINCT ticker) as tickers
        FROM read_parquet('{pp}', hive_partitioning=true)
        GROUP BY year ORDER BY year
    """)
    for yr, rows, tkrs in per_year:
        yr_dir = d / f"year={yr}"
        sz = file_size(yr_dir)
        print(f"  {yr:>6} {fmt(rows):>14} {fmt(tkrs):>9} {sz:>8}")

    sample = q(f"""
        SELECT ticker, day, open, high, low, close, volume, cnt
        FROM read_parquet('{pp}', hive_partitioning=true) USING SAMPLE 5
    """)
    print(f"\n  Sample rows:")
    for t, d_, o, h, l, c, v, cnt in sample:
        print(f"    {t:>6} | {d_} | O:{o:.2f} H:{h:.2f} L:{l:.2f} C:{c:.2f} | V:{fmt(v)} | {cnt} bars")


# ---------------------------------------------------------------------------
# N-day Agg helper
# ---------------------------------------------------------------------------

def summarize_nday_agg(name, filename, block_size):
    p = DB_DIR / filename
    if not p.exists():
        section(f"{name} — not found"); return
    section(name)
    print_schema(f"read_parquet('{p}')")

    total = q1(f"SELECT COUNT(*) FROM read_parquet('{p}')")
    tickers = q1(f"SELECT COUNT(DISTINCT ticker) FROM read_parquet('{p}')")
    date_range = q(f"SELECT MIN(block_start), MAX(block_end) FROM read_parquet('{p}')")[0]
    print(f"  Rows: {fmt(total)} | Tickers: {fmt(tickers)} | Range: {date_range[0]} to {date_range[1]}")
    print(f"  File: {file_size(p)}")

    dc_stats = q(f"""
        SELECT MIN(day_cnt), MEDIAN(day_cnt)::INT, MAX(day_cnt), ROUND(AVG(day_cnt), 1)
        FROM read_parquet('{p}')
    """)[0]
    print(f"  Days per block — min: {dc_stats[0]}, median: {dc_stats[1]}, max: {dc_stats[2]}, avg: {dc_stats[3]} (expect <={block_size})")

    sample = q(f"""
        SELECT ticker, block_start, block_end, open, close, volume, day_cnt
        FROM read_parquet('{p}') USING SAMPLE 5
    """)
    print(f"\n  Sample rows:")
    for t, bs, be, o, c, v, dc in sample:
        print(f"    {t:>6} | {bs} to {be} | O:{o:.2f} C:{c:.2f} | V:{fmt(v)} | {dc} days")


# ---------------------------------------------------------------------------
# Market Cap
# ---------------------------------------------------------------------------

def summarize_market_cap():
    p = DB_DIR / "market_cap.parquet"
    if not p.exists():
        section("MARKET CAP — not found"); return
    section("MARKET CAP")
    print_schema(f"read_parquet('{p}')")

    total = q1(f"SELECT COUNT(*) FROM read_parquet('{p}')")
    tickers = q1(f"SELECT COUNT(DISTINCT ticker) FROM read_parquet('{p}')")
    date_range = q(f"SELECT MIN(day), MAX(day) FROM read_parquet('{p}')")[0]
    print(f"  Rows: {fmt(total)} | Tickers: {fmt(tickers)} | Dates: {date_range[0]} to {date_range[1]}")
    print(f"  File: {file_size(p)}")

    cap_stats = q(f"""
        SELECT MIN(cap), MEDIAN(cap)::BIGINT, MAX(cap)
        FROM read_parquet('{p}')
    """)[0]
    print(f"  Cap range — min: ${fmt(cap_stats[0])}  median: ${fmt(cap_stats[1])}  max: ${fmt(cap_stats[2])}")

    top = q(f"""
        WITH latest AS (
            SELECT ticker, cap, ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY day DESC) as rn
            FROM read_parquet('{p}')
        )
        SELECT ticker, cap FROM latest WHERE rn = 1
        ORDER BY cap DESC LIMIT 10
    """)
    print(f"\n  Top 10 by latest market cap:")
    for t, c in top:
        print(f"    {t:>6}  ${fmt(c)}")


# ---------------------------------------------------------------------------
# Insider Trades
# ---------------------------------------------------------------------------

def summarize_insider_trades():
    p = DB_DIR / "insider_trades.parquet"
    if not p.exists():
        section("INSIDER TRADES — not found"); return
    section("INSIDER TRADES")
    print_schema(f"read_parquet('{p}')")

    total = q1(f"SELECT COUNT(*) FROM read_parquet('{p}')")
    tickers = q1(f"SELECT COUNT(DISTINCT ticker) FROM read_parquet('{p}')")
    date_range = q(f"SELECT MIN(trade_date), MAX(trade_date) FROM read_parquet('{p}')")[0]
    print(f"  Rows: {fmt(total)} | Tickers: {fmt(tickers)} | Dates: {date_range[0]} to {date_range[1]}")
    print(f"  File: {file_size(p)}")

    tx_codes = q(f"""
        SELECT tx_code, COUNT(*) FROM read_parquet('{p}')
        GROUP BY tx_code ORDER BY tx_code
    """)
    print(f"  Transaction types: {', '.join(f'{c}={fmt(n)}' for c, n in tx_codes)}")

    ad = q(f"""
        SELECT acquired_disposed, COUNT(*) FROM read_parquet('{p}')
        GROUP BY acquired_disposed ORDER BY acquired_disposed
    """)
    print(f"  Acquired/Disposed: {', '.join(f'{c}={fmt(n)}' for c, n in ad)}")

    own = q(f"""
        SELECT ownership_type, COUNT(*) FROM read_parquet('{p}')
        GROUP BY ownership_type ORDER BY ownership_type
    """)
    print(f"  Ownership type: {', '.join(f'{c}={fmt(n)}' for c, n in own)}")

    top = q(f"""
        SELECT ticker, COUNT(*) as trades FROM read_parquet('{p}')
        GROUP BY ticker ORDER BY trades DESC LIMIT 10
    """)
    print(f"\n  Top 10 most-traded tickers:")
    for t, n in top:
        print(f"    {t:>6}  {fmt(n)} trades")

    sample = q(f"""
        SELECT ticker, trade_date, tx_code, shares, total_value, insider_name
        FROM read_parquet('{p}') USING SAMPLE 5
    """)
    print(f"\n  Sample rows:")
    for t, d, tc, s, tv, name in sample:
        tv_str = f"${fmt(tv)}" if tv else "N/A"
        name_str = (name[:30] + "...") if name and len(name) > 30 else (name or "N/A")
        print(f"    {t:>6} | {d} | {tc} | {fmt(s)} shares | {tv_str} | {name_str}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

ALL_TABLES = {
    "tickers": summarize_tickers,
    "prices": summarize_prices,
    "daily_aggs": summarize_daily_aggs,
    "ten_day_aggs": lambda: summarize_nday_agg("10-DAY AGGS", "ten_day_aggs.parquet", 10),
    "hundred_day_aggs": lambda: summarize_nday_agg("100-DAY AGGS", "hundred_day_aggs.parquet", 100),
    "market_cap": summarize_market_cap,
    "insider_trades": summarize_insider_trades,
}


def run_summary(tables=None):
    """Run summary for given tables (or all). Callable from other modules."""
    tables = tables or list(ALL_TABLES.keys())
    print(f"Database directory: {DB_DIR}")
    print(f"Total DB size: {file_size(DB_DIR)}")
    for t in tables:
        ALL_TABLES[t]()
    print()


def main():
    parser = argparse.ArgumentParser(
        description="Summarize market data parquet files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "tables", nargs="*",
        help=f"Tables to summarize (default: all). Choices: {', '.join(ALL_TABLES)}",
    )
    args = parser.parse_args()

    tables = args.tables or list(ALL_TABLES.keys())
    for t in tables:
        if t not in ALL_TABLES:
            sys.exit(f"Unknown table: '{t}'. Choices: {', '.join(ALL_TABLES)}")

    run_summary(tables)
    con.close()


if __name__ == "__main__":
    main()
