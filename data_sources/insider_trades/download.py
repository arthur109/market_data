#!/usr/bin/env python3
"""
Download SEC Form 4 insider trade data from sec-api.io.

Files are saved to:
    data/{year}/{YYYY-MM}.jsonl.gz

A fresh index is fetched each run and saved to temp/index.json.
Set SEC_API_TOKEN in .env before running.

Usage:
    python download.py
    python download.py --from 2024-01 --to 2024-12
    python download.py --force
"""

import argparse
import json
import os
import re
import sys
import time
from pathlib import Path

import requests

SCRIPT_DIR = Path(__file__).parent
DATA_DIR = SCRIPT_DIR / "data"
TEMP_DIR = SCRIPT_DIR / "temp"
ENV_FILE = SCRIPT_DIR / ".env"

INDEX_URL = "https://api.sec-api.io/bulk/form-4/index.json"
BASE_URL = "https://api.sec-api.io/bulk/form-4"


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

def load_token():
    """Read SEC_API_TOKEN from .env or environment."""
    if ENV_FILE.exists():
        with open(ENV_FILE) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, _, val = line.partition("=")
                    if key.strip() == "SEC_API_TOKEN" and val.strip():
                        return val.strip()
    token = os.environ.get("SEC_API_TOKEN", "")
    if not token:
        sys.exit("SEC_API_TOKEN not set. Copy .env.template → .env and add your key.")
    return token


# ---------------------------------------------------------------------------
# Index
# ---------------------------------------------------------------------------

def fetch_index(token):
    """Download the file listing from sec-api.io and save to temp/index.json."""
    TEMP_DIR.mkdir(exist_ok=True)
    print("Fetching index from sec-api.io...")

    resp = requests.get(INDEX_URL, headers={"Authorization": token}, timeout=30)
    if resp.status_code == 403:
        resp = requests.get(f"{INDEX_URL}?token={token}", timeout=30)
    resp.raise_for_status()

    raw = resp.json()
    with open(TEMP_DIR / "index.json", "w") as f:
        json.dump(raw, f, indent=2)

    # Normalise to a flat list of {"name": ..., "url": ..., "size": ...}
    if isinstance(raw, list):
        entries = raw
    elif isinstance(raw, dict) and "files" in raw:
        entries = raw["files"]
    else:
        sys.exit("Unexpected index format from sec-api.io.")

    files = []
    for entry in entries:
        if isinstance(entry, str):
            files.append({"name": entry, "url": f"{BASE_URL}/{entry}", "size": None})
        elif isinstance(entry, dict):
            key = entry.get("key") or entry.get("name") or entry.get("filename", "")
            url = entry.get("url") or entry.get("link") or f"{BASE_URL}/{key}"
            files.append({"name": key, "url": url, "size": entry.get("size")})

    print(f"  Index has {len(files)} files.")
    return files


# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------

def extract_year_month(name):
    """Return YYYY-MM from a filename like '2024/2024-01.jsonl.gz'."""
    m = re.search(r"(\d{4}-\d{2})", name)
    return m.group(1) if m else None


def needs_download(local_path, expected_size, force):
    if force:
        return True
    if not local_path.exists():
        return True
    if expected_size is not None and local_path.stat().st_size != expected_size:
        return True
    return False


def download_file(url, dest, token, max_retries=5):
    """Stream-download url → dest, via a .tmp file. Cleans up on failure."""
    tmp = Path(str(dest) + ".tmp")
    dest.parent.mkdir(parents=True, exist_ok=True)

    for attempt in range(max_retries):
        try:
            with requests.get(url, headers={"Authorization": token},
                              timeout=120, stream=True) as resp:
                if resp.status_code == 429:
                    wait = (2 ** attempt) * 2
                    print(f"    Rate limited, retrying in {wait}s...")
                    time.sleep(wait)
                    continue
                resp.raise_for_status()

                with open(tmp, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=65536):
                        f.write(chunk)

            tmp.rename(dest)
            return True

        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                wait = (2 ** attempt) * 2
                print(f"    Error: {e}, retrying in {wait}s...")
                time.sleep(wait)
            else:
                print(f"    FAILED after {max_retries} attempts: {e}")
                if tmp.exists():
                    tmp.unlink()
                return False

    return False


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Download SEC Form 4 insider trade data")
    parser.add_argument("--from", dest="from_month", metavar="YYYY-MM",
                        help="Start month, inclusive (e.g. 2024-01)")
    parser.add_argument("--to", dest="to_month", metavar="YYYY-MM",
                        help="End month, inclusive (e.g. 2024-12)")
    parser.add_argument("--force", action="store_true",
                        help="Re-download files even if they already exist")
    args = parser.parse_args()

    token = load_token()
    files = fetch_index(token)

    downloaded = skipped = failed = 0

    for i, entry in enumerate(files, 1):
        ym = extract_year_month(entry["name"])
        if ym is None:
            print(f"  [{i}/{len(files)}] Skipping unrecognised entry: {entry['name']}")
            continue

        if args.from_month and ym < args.from_month:
            continue
        if args.to_month and ym > args.to_month:
            continue

        year = ym[:4]
        dest = DATA_DIR / year / f"{ym}.jsonl.gz"

        if not needs_download(dest, entry["size"], args.force):
            skipped += 1
            print(f"  [{i}/{len(files)}] {ym} — already downloaded, skipping")
            continue

        size_str = f"{entry['size'] / 1024 / 1024:.1f} MB" if entry["size"] else "? MB"
        print(f"  [{i}/{len(files)}] Downloading {ym} ({size_str})...")

        if download_file(entry["url"], dest, token):
            saved_mb = dest.stat().st_size / 1024 / 1024
            print(f"    Saved ({saved_mb:.1f} MB)")
            downloaded += 1
        else:
            failed += 1

        if i < len(files):
            time.sleep(0.5)

    print(f"\nDone: {downloaded} downloaded, {skipped} skipped, {failed} failed")
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
