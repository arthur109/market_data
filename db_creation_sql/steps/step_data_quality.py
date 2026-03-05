"""Data quality: detect bad hourly price ticks, apply nuke/drop rules, write staging exclusions DB."""

import re
import sqlite3
from collections import defaultdict
from datetime import datetime, timezone

from build_common import (
    OUTPUT_DIR, get_year_dbs, install_duckdb_sqlite, log, step,
)

# ANSI colors for stderr report
RED = "\033[31m"
RESET = "\033[0m"

SUFFIX_RE = re.compile(r'^[A-Z]{2,5}(WS|W|R|P|U)$')


def _apply_nuke_rules(by_ticker, all_tickers):
    """Apply nuke rules sequentially. Return {ticker: reason} for nuked tickers.

    Each row is (ticker, ts, close, median_before, median_after, prev_close, next_close, flag_reason).
    all_tickers is the full set of tickers in the dataset (for suffix validation).
    """
    nuked = {}

    for ticker, rows in by_ticker.items():
        closes = [r[2] for r in rows]

        # Rule 1: Absurd absolute price — any flagged close > $1M
        if any(c > 1_000_000 for c in closes):
            nuked[ticker] = "Rule 1: close > $1M"
            continue

        # Rule 2: Absurd relative price — close > 100x either median on 2+ days
        absurd_days = set()
        for r in rows:
            if ((r[3] is not None and r[3] > 0 and r[2] / r[3] > 100)
                    or (r[4] is not None and r[4] > 0 and r[2] / r[4] > 100)):
                absurd_days.add(datetime.fromtimestamp(r[1], tz=timezone.utc).date())
        if len(absurd_days) >= 2:
            nuked[ticker] = f"Rule 2: close > 100x median on {len(absurd_days)} days"
            continue

        # Rule 3: High anomaly count — 5+ anomalous trading days
        # Count distinct days, not bars (one bad day = ~7 hourly bars)
        anomaly_days = len(set(
            datetime.fromtimestamp(r[1], tz=timezone.utc).date() for r in rows
        ))
        if anomaly_days >= 5:
            nuked[ticker] = f"Rule 3: {anomaly_days} anomalous days ({len(rows)} bars)"
            continue

        # Rule 4: Derivative suffix — only if base ticker exists in dataset
        # e.g. ACACW is a warrant if ACAC exists, but LOW is not a derivative of LO
        m = SUFFIX_RE.match(ticker)
        if m:
            base = ticker[:m.start(1)]
            if base in all_tickers:
                nuked[ticker] = f"Rule 4: derivative suffix ({base} + {m.group(1)})"
                continue

        # Rule 5: Stock died — after-median = $0.00
        if any(r[4] is not None and r[4] == 0.0 for r in rows):
            nuked[ticker] = "Rule 5: after-median = $0"
            continue

        # Rule 6: Contaminated medians — median > 50x close (only with 2+ anomalous days)
        # A single bad day where close is tiny doesn't mean the median is polluted —
        # the median is correct and the close is garbage. Only nuke when multiple
        # days suggest systematic median contamination (e.g. split-adjustment).
        if anomaly_days >= 2 and any(
            (r[3] is not None and r[2] > 0 and r[3] / r[2] > 50)
            or (r[4] is not None and r[2] > 0 and r[4] / r[2] > 50)
            for r in rows
        ):
            nuked[ticker] = "Rule 6: median > 50x close"
            continue

    return nuked


def _fmt_ts(epoch_secs):
    """Format epoch seconds as YYYY-MM-DD HH:MM."""
    return datetime.fromtimestamp(epoch_secs, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")


def _fmt_price(p):
    """Format a price value for the report."""
    if p is None:
        return "N/A       "
    return f"${p:<10.2f}"


def _print_report(by_ticker, all_rows, nuked):
    """Print the color-coded data quality report to stderr."""
    surviving = {t: rows for t, rows in by_ticker.items() if t not in nuked}
    total_flags = sum(len(rows) for rows in by_ticker.values())
    nuked_count = len(nuked)
    surviving_count = len(surviving)
    surviving_bars = sum(len(rows) for rows in surviving.values())

    log("")
    log("=== DATA QUALITY REPORT ===")
    log("")
    log(f"Flagged {len(by_ticker):,} tickers with {total_flags:,} anomalous bars.")
    log(f"  {RED}Nuked: {nuked_count:,} tickers (all data removed){RESET}")
    log(f"  Dropped: {surviving_count:,} tickers, {surviving_bars:,} individual bars")

    issue_types = [
        ("ZERO/NEGATIVE CLOSE", "zero_or_negative"),
        ("PRICE TOO LOW vs both medians (close/median < 0.15)", "price_too_low"),
        ("PRICE TOO HIGH vs both medians (close/median > 7)", "price_too_high"),
    ]

    for label, reason in issue_types:
        reason_rows = [r for r in all_rows if r[7] == reason]
        if not reason_rows:
            continue

        reason_tickers = defaultdict(list)
        for r in reason_rows:
            reason_tickers[r[0]].append(r)

        log("")
        log(f"--- {label} ---")
        log(f"{len(reason_rows):,} occurrences across {len(reason_tickers):,} tickers")
        log("")

        for ticker in sorted(reason_tickers):
            rows = reason_tickers[ticker]
            total = len(rows)
            is_nuked = ticker in nuked

            if is_nuked:
                nuke_reason = nuked[ticker]
                log(f"  {RED}{ticker} ({total} occurrences) — NUKED: {nuke_reason}{RESET}")
            else:
                log(f"  {ticker} ({total} occurrences) — drop bars:")

            max_lines = 5
            for i, (_, ts, c, mb, ma, pc, nc, _) in enumerate(rows):
                if i >= max_lines:
                    remaining = total - max_lines
                    if remaining > 0:
                        if is_nuked:
                            log(f"{RED}    ... and {remaining} more{RESET}")
                        else:
                            log(f"    ... and {remaining} more")
                    break

                ts_str = _fmt_ts(ts)
                prev_str = _fmt_price(pc)
                close_str = _fmt_price(c)
                next_str = _fmt_price(nc)
                mb_str = f"${mb:.2f}" if mb is not None else "N/A"
                ma_str = f"${ma:.2f}" if ma is not None else "N/A"
                line = f"    {ts_str}  {prev_str} \u2192 {close_str} \u2192 {next_str}  before={mb_str}  after={ma_str}"
                if is_nuked:
                    log(f"{RED}{line}{RESET}")
                else:
                    log(line)

    log("")
    log(f"Summary: {RED}{nuked_count} nuked tickers{RESET} + {surviving_bars} dropped bars")


def _write_staging_db(nuked, surviving):
    """Write staging_exclusions.db with nuked_tickers and dropped_bars tables."""
    staging_path = OUTPUT_DIR / "staging_exclusions.db"
    staging_tmp = OUTPUT_DIR / "staging_exclusions.db.tmp"
    if staging_tmp.exists():
        staging_tmp.unlink()

    sconn = sqlite3.connect(str(staging_tmp))
    sconn.execute("""CREATE TABLE nuked_tickers (
        ticker TEXT PRIMARY KEY,
        nuke_reason TEXT NOT NULL
    ) WITHOUT ROWID, STRICT""")
    sconn.execute("""CREATE TABLE dropped_bars (
        ticker TEXT NOT NULL,
        ts INTEGER NOT NULL,
        flag_reason TEXT NOT NULL,
        PRIMARY KEY (ticker, ts)
    ) WITHOUT ROWID, STRICT""")

    sconn.executemany(
        "INSERT INTO nuked_tickers VALUES (?, ?)",
        [(t, reason) for t, reason in nuked.items()],
    )
    for rows in surviving.values():
        sconn.executemany(
            "INSERT INTO dropped_bars VALUES (?, ?, ?)",
            [(r[0], r[1], r[7]) for r in rows],
        )

    sconn.commit()
    sconn.close()
    staging_tmp.rename(staging_path)
    return staging_path


@step("data_quality_v11", target="data_quality", depends_on=("prices", "trading_calendar"))
def build_data_quality(con):
    """Scan hourly prices for anomalous ticks and produce a report + staging exclusions DB."""
    year_dbs = get_year_dbs()

    install_duckdb_sqlite(con)

    # Load all hourly bars from year dbs (ts kept as epoch-second integer)
    log("Loading hourly prices from year SQLite databases...")
    parts = [
        f"SELECT ticker, ts, ROUND(close, 2) AS close FROM sqlite_scan('{p}', 'prices')"
        for _, p in year_dbs
    ]
    con.execute(f"CREATE TABLE _all_prices AS {' UNION ALL '.join(parts)}")

    total_bars = con.execute("SELECT COUNT(*) FROM _all_prices").fetchone()[0]
    all_tickers = set(r[0] for r in con.execute("SELECT DISTINCT ticker FROM _all_prices").fetchall())
    log(f"  {total_bars:,} hourly bars loaded ({len(all_tickers):,} tickers)")

    # Compute per-bar window functions and flag anomalies
    log("Computing 50-bar before/after medians and detecting anomalies...")
    con.execute("""
        CREATE TABLE _flagged AS
        SELECT
            ticker, ts, close, median_before, median_after, prev_close, next_close,
            CASE
                -- Zero/negative: only flag if at least one median is >= $0.03
                -- (penny stocks near zero are not anomalies)
                WHEN close <= 0
                 AND (COALESCE(median_before, 0) >= 0.03 OR COALESCE(median_after, 0) >= 0.03)
                    THEN 'zero_or_negative'
                -- Price too low: ratio check + median must be >= $0.03 above close
                WHEN (median_before IS NULL OR median_before <= 0 OR close / median_before < 0.15)
                 AND (median_after IS NULL OR median_after <= 0 OR close / median_after < 0.15)
                 AND (median_before IS NOT NULL OR median_after IS NOT NULL)
                 AND (COALESCE(median_before, 0) - close >= 0.03 OR COALESCE(median_after, 0) - close >= 0.03)
                    THEN 'price_too_low'
                -- Price too high: ratio check + close must be >= $0.03 above median
                WHEN (median_before IS NULL OR median_before <= 0 OR close / median_before > 7)
                 AND (median_after IS NULL OR median_after <= 0 OR close / median_after > 7)
                 AND (median_before IS NOT NULL OR median_after IS NOT NULL)
                 AND (close - COALESCE(median_before, 0) >= 0.03 OR close - COALESCE(median_after, 0) >= 0.03)
                    THEN 'price_too_high'
            END AS flag_reason
        FROM (
            SELECT
                ticker, ts, close,
                LAG(close) OVER w AS prev_close,
                LEAD(close) OVER w AS next_close,
                MEDIAN(close) OVER (
                    PARTITION BY ticker ORDER BY ts
                    ROWS BETWEEN 50 PRECEDING AND 1 PRECEDING
                ) AS median_before,
                MEDIAN(close) OVER (
                    PARTITION BY ticker ORDER BY ts
                    ROWS BETWEEN 1 FOLLOWING AND 50 FOLLOWING
                ) AS median_after
            FROM _all_prices
            WINDOW w AS (PARTITION BY ticker ORDER BY ts)
        ) sub
        WHERE (
               close <= 0
               AND (COALESCE(median_before, 0) >= 0.03 OR COALESCE(median_after, 0) >= 0.03)
           )
           OR (
               (median_before IS NULL OR median_before <= 0 OR close / median_before < 0.15)
               AND (median_after IS NULL OR median_after <= 0 OR close / median_after < 0.15)
               AND (median_before IS NOT NULL OR median_after IS NOT NULL)
               AND (COALESCE(median_before, 0) - close >= 0.03 OR COALESCE(median_after, 0) - close >= 0.03)
           )
           OR (
               (median_before IS NULL OR median_before <= 0 OR close / median_before > 7)
               AND (median_after IS NULL OR median_after <= 0 OR close / median_after > 7)
               AND (median_before IS NOT NULL OR median_after IS NOT NULL)
               AND (close - COALESCE(median_before, 0) >= 0.03 OR close - COALESCE(median_after, 0) >= 0.03)
           )
    """)
    con.execute("DROP TABLE _all_prices")
    con.execute("DELETE FROM _flagged WHERE flag_reason IS NULL")

    total_flags = con.execute("SELECT COUNT(*) FROM _flagged").fetchone()[0]
    if total_flags == 0:
        log("No data quality issues found.")
        con.execute("DROP TABLE _flagged")
        # Write empty staging DB so downstream steps see a clean state
        _write_staging_db({}, {})
        return

    # Fetch all flagged rows: (ticker, ts, close, median_before, median_after, prev_close, next_close, flag_reason)
    all_rows = con.execute("""
        SELECT ticker, ts, close, median_before, median_after, prev_close, next_close, flag_reason
        FROM _flagged ORDER BY ticker, ts
    """).fetchall()
    con.execute("DROP TABLE _flagged")

    by_ticker = defaultdict(list)
    for row in all_rows:
        by_ticker[row[0]].append(row)

    # Apply nuke rules
    nuked = _apply_nuke_rules(by_ticker, all_tickers)
    surviving = {t: rows for t, rows in by_ticker.items() if t not in nuked}

    # Write staging DB
    staging_path = _write_staging_db(nuked, surviving)
    nuked_count = len(nuked)
    surviving_bars = sum(len(rows) for rows in surviving.values())
    log(f"Wrote {staging_path.name}: {nuked_count:,} nuked tickers, {surviving_bars:,} dropped bars")

    # Print report
    _print_report(by_ticker, all_rows, nuked)
