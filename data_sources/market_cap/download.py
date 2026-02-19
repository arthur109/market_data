#!/usr/bin/env python3
"""
Download historical daily market cap from the FMP API (financialmodelingprep.com).

Files are saved to:
    data/{SYMBOL}.csv

Ticker source (in order of preference):
  1. --tickers FILE  — one ticker per line
  2. Auto-discover   — reads ../stocks/data/*.zip and extracts ticker names from
                       the filenames inside each zip (no extraction needed).
                       Populate the stocks data source first, or use --tickers.

Set FMP_API_TOKEN in .env before running.

Usage:
    python download.py
    python download.py --from 1999-01-01 --to 2024-12-31
    python download.py --tickers my_tickers.txt
    python download.py --retry-failed
    python download.py --force
"""

import argparse
import csv
import json
import os
import sys
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from pathlib import Path

import requests

SCRIPT_DIR = Path(__file__).parent
DATA_DIR = SCRIPT_DIR / "data"
TEMP_DIR = SCRIPT_DIR / "temp"
ENV_FILE = SCRIPT_DIR / ".env"
STOCKS_DATA_DIR = SCRIPT_DIR / ".." / "stocks" / "data"
FAILED_FILE = TEMP_DIR / "failed.json"

FMP_BASE = "https://financialmodelingprep.com/stable/historical-market-capitalization"
FMP_MAX_DAYS = 5000   # above this we paginate
FMP_WINDOW = 5000     # calendar days per paginated request
WORKERS = 20          # concurrent requests in flight at once


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

def load_token():
    """Read FMP_API_TOKEN from .env or environment."""
    if ENV_FILE.exists():
        with open(ENV_FILE) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, _, val = line.partition("=")
                    if key.strip() == "FMP_API_TOKEN" and val.strip():
                        return val.strip()
    token = os.environ.get("FMP_API_TOKEN", "")
    if not token:
        sys.exit("FMP_API_TOKEN not set. Copy .env.template → .env and add your key.")
    return token


# ---------------------------------------------------------------------------
# Ticker discovery
# ---------------------------------------------------------------------------

def tickers_from_file(path):
    """Read tickers from a plain-text file, one per line."""
    tickers = []
    with open(path) as f:
        for line in f:
            t = line.strip().upper()
            if t and not t.startswith("#"):
                tickers.append(t)
    return sorted(set(tickers))


def tickers_from_stocks_zips():
    """
    Discover tickers by listing member filenames inside each stocks zip.
    Each member is named like: AAPL_full_1hour_adjsplitdiv.txt
    No extraction is needed — zipfile.ZipFile.namelist() reads the index only.
    """
    zips = sorted(STOCKS_DATA_DIR.glob("*.zip"))
    if not zips:
        return []

    tickers = []
    suffix = "_full_1hour_adjsplitdiv.txt"
    for zpath in zips:
        try:
            with zipfile.ZipFile(zpath) as zf:
                for name in zf.namelist():
                    basename = Path(name).name
                    if basename.endswith(suffix):
                        ticker = basename[: -len(suffix)]
                        if ticker:
                            tickers.append(ticker)
        except zipfile.BadZipFile:
            print(f"  Warning: skipping bad zip: {zpath.name}")

    return sorted(set(tickers))


# ---------------------------------------------------------------------------
# Failed-ticker tracking
# ---------------------------------------------------------------------------

def load_failed():
    if FAILED_FILE.exists():
        with open(FAILED_FILE) as f:
            return json.load(f)
    return {}


def save_failed(failed):
    TEMP_DIR.mkdir(exist_ok=True)
    with open(FAILED_FILE, "w") as f:
        json.dump(failed, f, indent=2)


# ---------------------------------------------------------------------------
# FMP API
# ---------------------------------------------------------------------------

def fetch_page(symbol, token, from_date, to_date):
    """
    Fetch one page of market cap data for a single symbol.
    Returns [(date_str, market_cap), ...] or None on unrecoverable error.
    """
    url = (f"{FMP_BASE}?symbol={symbol}"
           f"&from={from_date}&to={to_date}"
           f"&limit=5000"
           f"&apikey={token}")

    for attempt in range(3):
        try:
            with requests.get(url, timeout=60) as resp:
                status = resp.status_code
                if status == 404:
                    return None
                if status != 429:
                    resp.raise_for_status()
                    data = resp.json()

        except requests.exceptions.RequestException as e:
            if attempt < 2:
                time.sleep(2 ** (attempt + 1))
                continue
            return None

        if status == 429:
            time.sleep(2 ** (attempt + 1))
            continue

        if isinstance(data, dict):
            err = data.get("Error Message") or data.get("error") or data.get("message")
            if err:
                return None

        if not isinstance(data, list):
            return None

        return [
            (entry["date"], entry["marketCap"])
            for entry in data
            if isinstance(entry, dict) and "date" in entry and "marketCap" in entry
        ]

    return None


def fetch_market_cap(symbol, token, from_date, to_date):
    """
    Fetch full market cap history for a symbol, paginating if the date range
    exceeds FMP's 5000-record limit.
    Returns [(date_str, market_cap), ...] sorted ascending, or None on error.
    """
    total_days = (to_date - from_date).days

    if total_days <= FMP_MAX_DAYS:
        return fetch_page(symbol, token, from_date.isoformat(), to_date.isoformat())

    all_rows = []
    window_start = from_date
    while window_start <= to_date:
        window_end = min(window_start + timedelta(days=FMP_WINDOW), to_date)
        rows = fetch_page(symbol, token, window_start.isoformat(), window_end.isoformat())
        if rows is None:
            return None
        all_rows.extend(rows)
        window_start = window_end + timedelta(days=1)

    seen = {}
    for d, c in all_rows:
        seen[d] = c
    return sorted(seen.items())


# ---------------------------------------------------------------------------
# CSV write
# ---------------------------------------------------------------------------

def save_csv(symbol, rows):
    """Write rows to data/{SYMBOL}.csv atomically via a .tmp file."""
    DATA_DIR.mkdir(exist_ok=True)
    dest = DATA_DIR / f"{symbol}.csv"
    tmp = Path(str(dest) + ".tmp")
    with open(tmp, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["date", "market_cap"])
        for date_str, cap in rows:
            writer.writerow([date_str, cap])
    tmp.rename(dest)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Download historical daily market cap from FMP",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--tickers", metavar="FILE",
                        help="File with one ticker per line (default: read from ../stocks/data/*.zip)")
    parser.add_argument("--from", dest="from_date", metavar="YYYY-MM-DD",
                        default="1999-01-01",
                        help="Start date for all tickers (default: 1999-01-01)")
    parser.add_argument("--to", dest="to_date", metavar="YYYY-MM-DD",
                        default=date.today().isoformat(),
                        help="End date for all tickers (default: today)")
    parser.add_argument("--force", action="store_true",
                        help="Re-download even if CSV already exists")
    parser.add_argument("--retry-failed", action="store_true",
                        help="Only process tickers recorded in temp/failed.json")
    args = parser.parse_args()

    token = load_token()

    from_date = date.fromisoformat(args.from_date)
    to_date = date.fromisoformat(args.to_date)

    # Resolve ticker list
    failed_map = load_failed()

    if args.retry_failed:
        tickers = sorted(failed_map.keys())
        if not tickers:
            print("No failed tickers to retry.")
            return
        print(f"Retrying {len(tickers)} previously failed tickers.")
    elif args.tickers:
        tickers = tickers_from_file(args.tickers)
        print(f"Loaded {len(tickers)} tickers from {args.tickers}.")
    else:
        print("Auto-discovering tickers from ../stocks/data/*.zip ...")
        tickers = tickers_from_stocks_zips()
        if not tickers:
            sys.exit(
                "No tickers found. Either:\n"
                "  1. Populate ../stocks/data/ with stock zip files, or\n"
                "  2. Pass --tickers FILE with one ticker per line."
            )
        print(f"  Found {len(tickers)} tickers.")

    print(f"Date range: {from_date} to {to_date}")
    print(f"Workers:    {WORKERS} concurrent requests")
    print(f"Output:     {DATA_DIR}/")
    print()

    # Split into tickers that need downloading vs already done
    to_download = []
    skipped = 0
    for symbol in tickers:
        dest = DATA_DIR / f"{symbol}.csv"
        if not args.force and not args.retry_failed and dest.exists():
            skipped += 1
        else:
            to_download.append(symbol)

    if skipped:
        print(f"Skipping {skipped} tickers with existing CSVs.")
    print(f"Downloading {len(to_download)} tickers with {WORKERS} workers...\n")

    downloaded = failed = 0
    total = len(to_download)

    def do_fetch(symbol):
        return symbol, fetch_market_cap(symbol, token, from_date, to_date)

    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        futures = {executor.submit(do_fetch, sym): sym for sym in to_download}
        completed = 0
        for future in as_completed(futures):
            completed += 1
            symbol, rows = future.result()

            if not rows:
                status = "NO DATA" if rows is not None else "FAILED"
                print(f"  [{completed}/{total}] {symbol}: {status}", flush=True)
                failed_map[symbol] = time.strftime("%Y-%m-%dT%H:%M:%S")
                failed += 1
            else:
                save_csv(symbol, rows)
                print(f"  [{completed}/{total}] {symbol}: {len(rows)} days", flush=True)
                failed_map.pop(symbol, None)
                downloaded += 1

            # Persist progress periodically so interruptions don't lose much
            if completed % 50 == 0 or completed == total:
                save_failed(failed_map)

    save_failed(failed_map)
    print(f"\nDone: {downloaded} downloaded, {skipped} skipped, {failed} failed")
    if failed_map:
        print(f"  {len(failed_map)} total failed tickers — re-run with --retry-failed")
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
