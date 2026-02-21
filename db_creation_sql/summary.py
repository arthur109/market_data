#!/usr/bin/env python3
"""
Data summary / preview for market data SQLite databases.

Prints a human-readable overview of each table so you can eyeball whether the
data looks reasonable or if something went wrong.

Usage:
    python summary.py                   # summarize all tables
    python summary.py prices            # summarize one table
    python summary.py tickers prices    # summarize specific tables
"""

import argparse
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

DB_DIR = Path(__file__).resolve().parent.parent / "db_sql"


def get_year_dbs():
    """Return sorted list of (year, path) for all year .db files."""
    dbs = []
    for p in sorted(DB_DIR.glob("[0-9][0-9][0-9][0-9].db")):
        year = int(p.stem)
        dbs.append((year, str(p)))
    return dbs


def q(db_path, sql, params=()):
    conn = sqlite3.connect(db_path)
    rows = conn.execute(sql, params).fetchall()
    conn.close()
    return rows


def q1(db_path, sql, params=()):
    rows = q(db_path, sql, params)
    return rows[0][0] if rows else None


def fmt(n):
    if n is None:
        return "N/A"
    if isinstance(n, float):
        return f"{n:,.2f}"
    return f"{n:,}"


def fmt_date(d):
    """Format YYYYMMDD integer (or ISO text) as YYYY-MM-DD string."""
    if d is None:
        return "N/A"
    if isinstance(d, str):
        return d[:10]  # already ISO text
    d = int(d)
    return f"{d // 10000}-{(d // 100) % 100:02d}-{d % 100:02d}"


def fmt_ts(ts):
    """Format epoch-seconds integer (or ISO text) as YYYY-MM-DD HH:MM:SS string."""
    if ts is None:
        return "N/A"
    if isinstance(ts, str):
        return ts[:19]  # already ISO text
    return datetime.utcfromtimestamp(int(ts)).strftime("%Y-%m-%d %H:%M:%S")


def file_size(path):
    p = Path(path)
    if p.exists():
        total = p.stat().st_size
    else:
        return "N/A"
    for unit in ("B", "KB", "MB", "GB"):
        if total < 1024:
            return f"{total:.0f}{unit}" if unit == "B" else f"{total:.1f}{unit}"
        total /= 1024
    return f"{total:.1f}TB"


def total_db_size():
    total = 0
    for p in DB_DIR.glob("*.db"):
        total += p.stat().st_size
    for unit in ("B", "KB", "MB", "GB"):
        if total < 1024:
            return f"{total:.0f}{unit}" if unit == "B" else f"{total:.1f}{unit}"
        total /= 1024
    return f"{total:.1f}TB"


def section(title):
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print(f"{'=' * 60}")


def print_schema_sqlite(db_path, table):
    """Print the schema of a SQLite table."""
    conn = sqlite3.connect(db_path)
    cols = conn.execute(f"PRAGMA table_info({table})").fetchall()
    conn.close()
    parts = [f"{name} ({dtype})" for _, name, dtype, *_ in cols]
    print(f"  Schema: {', '.join(parts)}")


# ---------------------------------------------------------------------------
# Tickers
# ---------------------------------------------------------------------------

def summarize_tickers():
    p = DB_DIR / "staging_tickers.db"
    if not p.exists():
        section("TICKERS — not found"); return
    section("TICKERS")
    print_schema_sqlite(str(p), "tickers")

    total = q1(str(p), "SELECT COUNT(*) FROM tickers")
    breakdown = q(str(p), "SELECT asset_type, COUNT(*) FROM tickers GROUP BY asset_type ORDER BY asset_type")
    parts = ", ".join(f"{fmt(c)} {t}" for t, c in breakdown)
    print(f"  Rows: {fmt(total)}  ({parts})")
    print(f"  File: {file_size(p)}")

    sample = q(str(p), "SELECT ticker, asset_type FROM tickers ORDER BY RANDOM() LIMIT 10")
    print(f"  Sample: {', '.join(f'{t}({a})' for t, a in sample)}")


# ---------------------------------------------------------------------------
# Prices
# ---------------------------------------------------------------------------

def summarize_prices():
    year_dbs = get_year_dbs()
    if not year_dbs:
        section("PRICES — not found"); return
    section("PRICES")
    print_schema_sqlite(year_dbs[0][1], "prices")

    total = 0
    all_tickers = set()
    per_year = []
    for year, path in year_dbs:
        conn = sqlite3.connect(path)
        try:
            row_count = conn.execute("SELECT COUNT(*) FROM prices").fetchone()[0]
            tkr_count = conn.execute("SELECT COUNT(DISTINCT ticker) FROM prices").fetchone()[0]
        except sqlite3.OperationalError:
            continue
        finally:
            conn.close()
        if row_count == 0:
            continue
        total += row_count
        per_year.append((year, row_count, tkr_count))
        all_tickers.update(t[0] for t in q(path, "SELECT DISTINCT ticker FROM prices"))

    if not per_year:
        print("  No price data found"); return

    print(f"  Rows: {fmt(total)} | Tickers: {fmt(len(all_tickers))} | Years: {per_year[0][0]}-{per_year[-1][0]}")
    total_size = sum(Path(p).stat().st_size for _, p in year_dbs)
    print(f"  Total size: {fmt(total_size // 1024)}KB")

    print(f"\n  {'Year':>6} {'Rows':>14} {'Tickers':>9} {'Size':>8} {'Rows/Ticker':>12}")
    print(f"  {'-'*53}")
    for yr, rows, tkrs in per_year:
        sz = file_size(DB_DIR / f"{yr}.db")
        rpt = rows // tkrs if tkrs else 0
        print(f"  {yr:>6} {fmt(rows):>14} {fmt(tkrs):>9} {sz:>8} {fmt(rpt):>12}")

    # Sample from a middle year db (ts is epoch seconds)
    mid_year, mid_path = year_dbs[len(year_dbs) // 2]
    sample = q(mid_path, """
        SELECT ticker, ts, open, high, low, close, volume, trading_day_num
        FROM prices ORDER BY RANDOM() LIMIT 5
    """)
    print(f"\n  Sample rows (from {mid_year}.db):")
    for t, ts, o, h, l, c, v, tdn in sample:
        print(f"    {t:>6} | {fmt_ts(ts)} | O:{o:.2f} H:{h:.2f} L:{l:.2f} C:{c:.2f} | V:{fmt(v)} | tdn:{tdn}")


# ---------------------------------------------------------------------------
# Daily Aggs Enriched
# ---------------------------------------------------------------------------

def summarize_daily_aggs_enriched():
    year_dbs = get_year_dbs()
    if not year_dbs:
        section("DAILY AGGS ENRICHED — not found"); return
    section("DAILY AGGS ENRICHED")

    total = 0
    per_year = []
    with_cap = 0
    first_schema_printed = False

    for year, path in year_dbs:
        conn = sqlite3.connect(path)
        try:
            row_count = conn.execute("SELECT COUNT(*) FROM daily_aggs_enriched").fetchone()[0]
            tkr_count = conn.execute("SELECT COUNT(DISTINCT ticker) FROM daily_aggs_enriched").fetchone()[0]
            cap_count = conn.execute("SELECT COUNT(*) FROM daily_aggs_enriched WHERE cap IS NOT NULL").fetchone()[0]
        except sqlite3.OperationalError:
            conn.close()
            continue
        conn.close()
        if not first_schema_printed:
            print_schema_sqlite(path, "daily_aggs_enriched")
            first_schema_printed = True
        if row_count == 0:
            continue
        total += row_count
        with_cap += cap_count
        per_year.append((year, row_count, tkr_count))

    if not per_year:
        print("  No daily_aggs_enriched data found"); return

    pct = (with_cap / total * 100) if total else 0
    print(f"  Rows: {fmt(total)} | Years: {per_year[0][0]}-{per_year[-1][0]}")
    print(f"  Market cap coverage: {fmt(with_cap)} / {fmt(total)} rows ({pct:.1f}%)")

    print(f"\n  {'Year':>6} {'Rows':>14} {'Tickers':>9}")
    print(f"  {'-'*33}")
    for yr, rows, tkrs in per_year:
        print(f"  {yr:>6} {fmt(rows):>14} {fmt(tkrs):>9}")

    # day is YYYYMMDD integer
    mid_year, mid_path = year_dbs[len(year_dbs) // 2]
    sample = q(mid_path, """
        SELECT ticker, day, open, close, volume, cap, trading_day_num, cum_close, cum_volume
        FROM daily_aggs_enriched ORDER BY RANDOM() LIMIT 5
    """)
    print(f"\n  Sample rows (from {mid_year}.db):")
    for t, d_, o, c, v, cap, tdn, cc, cv in sample:
        cap_str = f"${fmt(cap)}" if cap else "N/A"
        print(f"    {t:>6} | {fmt_date(d_)} | O:{o:.2f} C:{c:.2f} | V:{fmt(v)} | cap:{cap_str} | day#{tdn} | cum_c:{fmt(cc)}")


# ---------------------------------------------------------------------------
# Cap Lookup
# ---------------------------------------------------------------------------

def summarize_cap_lookup():
    year_dbs = get_year_dbs()
    if not year_dbs:
        section("CAP LOOKUP — not found"); return
    section("CAP LOOKUP")

    total = 0
    per_year = []
    first_schema_printed = False

    for year, path in year_dbs:
        conn = sqlite3.connect(path)
        try:
            row_count = conn.execute("SELECT COUNT(*) FROM cap_lookup").fetchone()[0]
        except sqlite3.OperationalError:
            conn.close()
            continue
        conn.close()
        if not first_schema_printed:
            print_schema_sqlite(path, "cap_lookup")
            first_schema_printed = True
        if row_count == 0:
            continue
        total += row_count
        per_year.append((year, row_count))

    if not per_year:
        print("  No cap_lookup data found"); return

    print(f"  Rows: {fmt(total)} | Years: {per_year[0][0]}-{per_year[-1][0]}")

    print(f"\n  {'Year':>6} {'Rows':>14}")
    print(f"  {'-'*22}")
    for yr, rows in per_year:
        print(f"  {yr:>6} {fmt(rows):>14}")

    # Column order is now (trading_day_num, ticker, day, cap, close, cum_close)
    mid_year, mid_path = year_dbs[len(year_dbs) // 2]
    sample = q(mid_path, """
        SELECT trading_day_num, ticker, day, cap, close, cum_close
        FROM cap_lookup ORDER BY RANDOM() LIMIT 5
    """)
    print(f"\n  Sample rows (from {mid_year}.db):")
    for tdn, t, d_, cap, c, cc in sample:
        print(f"    {fmt_date(d_)} | {t:>6} | cap:${fmt(cap)} | C:{c:.2f} | day#{tdn} | cum_c:{fmt(cc)}")


# ---------------------------------------------------------------------------
# Insider Purchases
# ---------------------------------------------------------------------------

def summarize_insider_purchases():
    year_dbs = get_year_dbs()
    if not year_dbs:
        section("INSIDER PURCHASES — not found"); return
    section("INSIDER PURCHASES")

    total = 0
    per_year = []
    first_schema_printed = False

    for year, path in year_dbs:
        conn = sqlite3.connect(path)
        try:
            row_count = conn.execute("SELECT COUNT(*) FROM insider_purchases").fetchone()[0]
        except sqlite3.OperationalError:
            conn.close()
            continue
        conn.close()
        if not first_schema_printed:
            print_schema_sqlite(path, "insider_purchases")
            first_schema_printed = True
        if row_count == 0:
            continue
        total += row_count
        per_year.append((year, row_count))

    if not per_year:
        print("  No insider_purchases data found"); return

    print(f"  Rows: {fmt(total)} | Years: {per_year[0][0]}-{per_year[-1][0]}")

    print(f"\n  {'Year':>6} {'Rows':>14}")
    print(f"  {'-'*22}")
    for yr, rows in per_year:
        print(f"  {yr:>6} {fmt(rows):>14}")

    # filing_date is YYYYMMDD integer
    mid_year, mid_path = year_dbs[len(year_dbs) // 2]
    sample = q(mid_path, """
        SELECT filing_date, ticker, shares, total_value, insider_name, trading_day_num
        FROM insider_purchases ORDER BY RANDOM() LIMIT 5
    """)
    print(f"\n  Sample rows (from {mid_year}.db):")
    for fd, t, s, tv, name, tdn in sample:
        tv_str = f"${fmt(tv)}" if tv else "N/A"
        name_str = (name[:30] + "...") if name and len(name) > 30 else (name or "N/A")
        tdn_str = fmt(tdn) if tdn is not None else "N/A"
        print(f"    {t:>6} | {fmt_date(fd)} | {fmt(s)} shares | {tv_str} | tdn:{tdn_str} | {name_str}")


# ---------------------------------------------------------------------------
# Trading Calendar
# ---------------------------------------------------------------------------

def summarize_trading_calendar():
    p = DB_DIR / "staging_calendar.db"
    if not p.exists():
        section("TRADING CALENDAR — not found"); return
    section("TRADING CALENDAR")
    print_schema_sqlite(str(p), "trading_calendar")

    total = q1(str(p), "SELECT COUNT(*) FROM trading_calendar")
    date_range = q(str(p), "SELECT MIN(day), MAX(day) FROM trading_calendar")[0]
    tdn_range = q(str(p), "SELECT MIN(trading_day_num), MAX(trading_day_num) FROM trading_calendar")[0]
    print(f"  Trading days: {fmt(total)} | Range: {fmt_date(date_range[0])} to {fmt_date(date_range[1])}")
    print(f"  trading_day_num range: {tdn_range[0]} to {tdn_range[1]}")
    print(f"  File: {file_size(p)}")

    # Per-year breakdown (day is YYYYMMDD integer)
    per_year = q(str(p), """
        SELECT day / 10000 AS yr, COUNT(*) AS days,
               MIN(trading_day_num) AS min_tdn, MAX(trading_day_num) AS max_tdn
        FROM trading_calendar
        GROUP BY yr ORDER BY yr
    """)
    print(f"\n  {'Year':>6} {'Days':>6} {'TDN range':>14}")
    print(f"  {'-'*30}")
    for yr, days, min_tdn, max_tdn in per_year:
        print(f"  {yr:>6} {days:>6} {min_tdn:>6}-{max_tdn:<6}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

ALL_TABLES = {
    "tickers": summarize_tickers,
    "prices": summarize_prices,
    "daily_aggs_enriched": summarize_daily_aggs_enriched,
    "cap_lookup": summarize_cap_lookup,
    "insider_purchases": summarize_insider_purchases,
    "trading_calendar": summarize_trading_calendar,
}


def run_summary(tables=None):
    """Run summary for given tables (or all). Callable from other modules."""
    tables = tables or list(ALL_TABLES.keys())
    print(f"Database directory: {DB_DIR}")
    print(f"Total DB size: {total_db_size()}")
    for t in tables:
        ALL_TABLES[t]()
    print()


def main():
    parser = argparse.ArgumentParser(
        description="Summarize market data SQLite databases",
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


if __name__ == "__main__":
    main()
