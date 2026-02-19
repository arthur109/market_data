"""
Shared infrastructure for the market data build system.

Constants, logging, step registry, manifest, DuckDB helpers, ticker discovery.
"""

import json
import os
import shutil
import sys
import zipfile
from datetime import datetime
from pathlib import Path

import duckdb

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parent
DATA_SOURCES = PROJECT_DIR / "data_sources"
OUTPUT_DIR = PROJECT_DIR / "db"
MANIFEST_FILE = OUTPUT_DIR / ".build_manifest.json"

STOCKS_ZIP_DIR = DATA_SOURCES / "stocks" / "data"
ETFS_ZIP_DIR = DATA_SOURCES / "etfs" / "data"
MARKET_CAP_DIR = DATA_SOURCES / "market_cap" / "data"
INSIDER_TRADES_DIR = DATA_SOURCES / "insider_trades" / "data"

PARQUET_SETTINGS = "FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 122880"

DUCKDB_MEMORY_LIMIT = "12GB"
DUCKDB_THREADS = os.cpu_count() or 4

TICKER_SUFFIX = "_full_1hour_adjsplitdiv.txt"

REGULAR_HOURS_START = 9   # 9:00 AM ET inclusive
REGULAR_HOURS_END = 15    # 3:00 PM ET inclusive (covers 3:00-3:59, last regular bar)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------


def log(msg):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", file=sys.stderr, flush=True)


def log_progress(current, total, msg):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] [{current}/{total}] {msg}", file=sys.stderr, flush=True)


# ---------------------------------------------------------------------------
# Step Registry
# ---------------------------------------------------------------------------

_steps = []  # ordered list of (step_id, target, depends_on, func)
_target_to_steps = {}  # target -> [step_id, ...]


def step(step_id, target, depends_on=()):
    """Decorator to register a build step."""
    def decorator(func):
        _steps.append((step_id, target, depends_on, func))
        _target_to_steps.setdefault(target, []).append(step_id)
        return func
    return decorator


def get_dependency_graph():
    """Build target -> set of targets that depend on it."""
    dependents = {}
    for _, target, deps, _ in _steps:
        for dep in deps:
            dependents.setdefault(dep, set()).add(target)
    return dependents


def get_downstream_targets(target):
    """Get all targets downstream of a given target (transitive)."""
    graph = get_dependency_graph()
    visited = set()
    queue = [target]
    while queue:
        t = queue.pop(0)
        if t in visited:
            continue
        visited.add(t)
        for dep in graph.get(t, []):
            queue.append(dep)
    visited.discard(target)  # don't include the target itself
    return visited


# ---------------------------------------------------------------------------
# Manifest
# ---------------------------------------------------------------------------


def load_manifest():
    if MANIFEST_FILE.exists():
        with open(MANIFEST_FILE) as f:
            return json.load(f)
    return {}


def save_manifest(manifest):
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    tmp = Path(str(MANIFEST_FILE) + ".tmp")
    with open(tmp, "w") as f:
        json.dump(manifest, f, indent=2)
    tmp.rename(MANIFEST_FILE)


# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------


def cleanup_stale_artifacts():
    """Remove leftover temp/building artifacts from interrupted runs."""
    if not OUTPUT_DIR.exists():
        return
    for item in OUTPUT_DIR.iterdir():
        if item.name.endswith(".tmp"):
            log(f"Cleaning stale artifact: {item.name}")
            if item.is_dir():
                shutil.rmtree(item)
            else:
                item.unlink()
        elif item.name.endswith("_old") or item.name.endswith("_building") or item.name.startswith("_"):
            log(f"Cleaning stale artifact: {item.name}")
            if item.is_dir():
                shutil.rmtree(item)
            else:
                item.unlink()


# ---------------------------------------------------------------------------
# DuckDB helpers
# ---------------------------------------------------------------------------


def make_connection():
    con = duckdb.connect(":memory:")
    con.execute(f"SET memory_limit = '{DUCKDB_MEMORY_LIMIT}'")
    con.execute(f"SET threads = {DUCKDB_THREADS}")
    return con


def verify_parquet(path, min_rows=1):
    """Verify a parquet file/directory has at least min_rows rows."""
    con = duckdb.connect(":memory:")
    result = con.execute(f"SELECT COUNT(*) FROM read_parquet('{path}')").fetchone()
    con.close()
    count = result[0]
    if count < min_rows:
        raise RuntimeError(f"Verification failed: {path} has {count} rows (expected >= {min_rows})")
    return count


# ---------------------------------------------------------------------------
# Ticker discovery
# ---------------------------------------------------------------------------


def discover_tickers_from_zips(zip_dir):
    """Discover tickers by reading ZIP file indexes (no extraction)."""
    tickers = {}
    zips = sorted(zip_dir.glob("*.zip"))
    for zpath in zips:
        try:
            with zipfile.ZipFile(zpath) as zf:
                for name in zf.namelist():
                    basename = Path(name).name
                    if basename.endswith(TICKER_SUFFIX):
                        ticker = basename[:-len(TICKER_SUFFIX)]
                        if ticker:
                            tickers[ticker] = zpath
        except zipfile.BadZipFile:
            log(f"Warning: skipping bad zip: {zpath.name}")
    return tickers
