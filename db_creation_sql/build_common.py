"""
Shared infrastructure for the market data SQLite build system.

Constants, logging, step registry, manifest, DuckDB helpers, SQLite helpers, ticker discovery.
"""

import json
import os
import shutil
import sqlite3
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
OUTPUT_DIR = PROJECT_DIR / "db_sql"
MANIFEST_FILE = OUTPUT_DIR / ".build_manifest.json"

STOCKS_ZIP_DIR = DATA_SOURCES / "stocks" / "data"
ETFS_ZIP_DIR = DATA_SOURCES / "etfs" / "data"
MARKET_CAP_DIR = DATA_SOURCES / "market_cap" / "data"
INSIDER_TRADES_DIR = DATA_SOURCES / "insider_trades" / "data"

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
_disabled_steps = set()  # step_ids that are registered but skipped


def step(step_id, target, depends_on=(), disabled=False):
    """Decorator to register a build step.

    Set disabled=True to keep the step registered (visible in --list)
    but skip it during builds unless --include-disabled is passed.
    """
    def decorator(func):
        _steps.append((step_id, target, depends_on, func))
        _target_to_steps.setdefault(target, []).append(step_id)
        if disabled:
            _disabled_steps.add(step_id)
        return func
    return decorator


def finalize_step_order():
    """Topologically sort _steps by depends_on (Kahn's algorithm).

    Stable: preserves file-import order among independent steps.
    Raises RuntimeError on dependency cycles.
    """
    # Build target -> index of first step that produces it
    target_indices = {}
    for i, (_, target, _, _) in enumerate(_steps):
        target_indices.setdefault(target, i)

    # Compute in-degree per step index based on depends_on targets
    n = len(_steps)
    in_degree = [0] * n
    # adj[i] = list of step indices that depend on target produced by step i
    adj = [[] for _ in range(n)]

    for i, (_, _, deps, _) in enumerate(_steps):
        for dep_target in deps:
            provider = target_indices.get(dep_target)
            if provider is not None:
                adj[provider].append(i)
                in_degree[i] += 1

    # Kahn's: seed with zero in-degree steps, preserving original order (stable)
    from collections import deque
    queue = deque(i for i in range(n) if in_degree[i] == 0)
    ordered = []

    while queue:
        idx = queue.popleft()
        ordered.append(idx)
        for dep_idx in sorted(adj[idx]):  # sorted for stability
            in_degree[dep_idx] -= 1
            if in_degree[dep_idx] == 0:
                queue.append(dep_idx)

    if len(ordered) != n:
        # Find cycle participants
        remaining = [_steps[i][1] for i in range(n) if i not in set(ordered)]
        raise RuntimeError(
            f"Dependency cycle detected among targets: {', '.join(dict.fromkeys(remaining))}"
        )

    reordered = [_steps[i] for i in ordered]
    _steps[:] = reordered


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
    """Remove all .tmp files/dirs anywhere under OUTPUT_DIR, plus top-level _old/_building/_ artifacts."""
    if not OUTPUT_DIR.exists():
        return
    # Recursively remove any .tmp file or directory anywhere in db_sql/
    for item in OUTPUT_DIR.rglob("*.tmp"):
        log(f"Cleaning stale artifact: {item.relative_to(OUTPUT_DIR)}")
        if item.is_dir():
            shutil.rmtree(item)
        else:
            item.unlink()
    # Top-level _old, _building, and _ prefixed artifacts
    for item in OUTPUT_DIR.iterdir():
        if item.name.endswith("_old") or item.name.endswith("_building") or item.name.startswith("_"):
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


def install_duckdb_sqlite(con):
    """Install and load the DuckDB sqlite extension."""
    con.execute("INSTALL sqlite")
    con.execute("LOAD sqlite")


# ---------------------------------------------------------------------------
# SQLite Table Schemas
# ---------------------------------------------------------------------------

TABLE_SCHEMAS = {
    "tickers": {
        "create": """
            CREATE TABLE tickers (
                ticker TEXT PRIMARY KEY,
                asset_type TEXT NOT NULL
            ) WITHOUT ROWID, STRICT
        """,
        "indexes": [],
    },
    "trading_calendar": {
        "create": """
            CREATE TABLE trading_calendar (
                trading_day_num INTEGER PRIMARY KEY,
                day INTEGER NOT NULL
            ) WITHOUT ROWID, STRICT
        """,
        "indexes": [],
    },
    "prices": {
        "create": """
            CREATE TABLE prices (
                ticker TEXT NOT NULL REFERENCES tickers(ticker),
                ts INTEGER NOT NULL,
                open REAL NOT NULL,
                high REAL NOT NULL,
                low REAL NOT NULL,
                close REAL NOT NULL,
                volume INTEGER NOT NULL,
                trading_day_num INTEGER NOT NULL REFERENCES trading_calendar(trading_day_num)
            ) STRICT
        """,
        "indexes": [
            "CREATE INDEX idx_prices_ticker_ts ON prices (ticker, ts)",
            "CREATE INDEX idx_prices_tdn_ticker ON prices (trading_day_num, ticker)",
        ],
    },
    "daily_aggs_enriched": {
        "create": """
            CREATE TABLE daily_aggs_enriched (
                ticker TEXT NOT NULL REFERENCES tickers(ticker),
                day INTEGER NOT NULL,
                open REAL NOT NULL,
                high REAL NOT NULL,
                low REAL NOT NULL,
                close REAL NOT NULL,
                volume INTEGER NOT NULL,
                cap INTEGER,
                trading_day_num INTEGER NOT NULL REFERENCES trading_calendar(trading_day_num),
                cum_close REAL NOT NULL,
                cum_high REAL NOT NULL,
                cum_low REAL NOT NULL,
                cum_volume REAL NOT NULL
            ) STRICT
        """,
        "indexes": [
            "CREATE INDEX idx_dae_tdn_ticker ON daily_aggs_enriched (trading_day_num, ticker)",
            "CREATE INDEX idx_dae_ticker_day ON daily_aggs_enriched (ticker, day)",
        ],
    },
    "cap_lookup": {
        "create": """
            CREATE TABLE cap_lookup (
                trading_day_num INTEGER NOT NULL REFERENCES trading_calendar(trading_day_num),
                ticker TEXT NOT NULL REFERENCES tickers(ticker),
                day INTEGER NOT NULL,
                cap INTEGER NOT NULL,
                close REAL NOT NULL,
                cum_close REAL NOT NULL,
                PRIMARY KEY (trading_day_num, ticker)
            ) WITHOUT ROWID, STRICT
        """,
        "indexes": [],
    },
    "insider_purchases": {
        "create": """
            CREATE TABLE insider_purchases (
                filing_date INTEGER NOT NULL,
                ticker TEXT NOT NULL REFERENCES tickers(ticker),
                insider_cik TEXT,
                total_value REAL,
                trading_day_num INTEGER REFERENCES trading_calendar(trading_day_num)
            ) STRICT
        """,
        "indexes": [
            "CREATE INDEX idx_ip_tdn_ticker ON insider_purchases (trading_day_num, ticker)",
        ],
    },
}


# ---------------------------------------------------------------------------
# SQLite helpers
# ---------------------------------------------------------------------------


def _set_pragmas(conn):
    """Set performance and correctness PRAGMAs on a SQLite connection."""
    conn.execute("PRAGMA page_size = 32768")      # 32KB pages (fewer I/O ops for analytical scans)
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute("PRAGMA cache_size = -64000")     # 64MB cache during build
    conn.execute("PRAGMA temp_store = MEMORY")


def open_year_db(year):
    """Open an existing year database, set PRAGMAs, return connection."""
    db_path = OUTPUT_DIR / f"{year}.db"
    conn = sqlite3.connect(str(db_path))
    _set_pragmas(conn)
    return conn


def open_year_db_tmp(year):
    """Open a temp year database (.db.tmp) for atomic creation."""
    db_path = OUTPUT_DIR / f"{year}.db.tmp"
    conn = sqlite3.connect(str(db_path))
    _set_pragmas(conn)
    return conn


def create_table(conn, name, with_indexes=True):
    """DROP IF EXISTS then CREATE a table, optionally create indexes."""
    schema = TABLE_SCHEMAS[name]
    conn.execute(f"DROP TABLE IF EXISTS {name}")
    conn.execute(schema["create"])
    if with_indexes:
        for idx_sql in schema["indexes"]:
            conn.execute(idx_sql)


def create_indexes(conn, name):
    """Create indexes for a table (call after bulk insert)."""
    schema = TABLE_SCHEMAS[name]
    for idx_sql in schema["indexes"]:
        conn.execute(idx_sql)


def write_reference_tables(conn, tickers_rows, calendar_rows):
    """Write tickers and trading_calendar reference tables to a year db."""
    create_table(conn, "tickers")
    conn.executemany("INSERT INTO tickers VALUES (?, ?)", tickers_rows)
    create_table(conn, "trading_calendar")
    conn.executemany("INSERT INTO trading_calendar VALUES (?, ?)", calendar_rows)
    conn.commit()


def optimize_db(db_path):
    """Run ANALYZE + VACUUM on a SQLite database to optimize for reads.

    ANALYZE populates sqlite_stat1 so the query planner picks good indexes.
    VACUUM rebuilds the file to reclaim space and defragment (also converts
    from WAL back to DELETE journal mode for a single compact file).
    """
    conn = sqlite3.connect(str(db_path))
    conn.execute("ANALYZE")
    conn.execute("VACUUM")
    conn.close()


def verify_table(db_path, table, min_rows=1):
    """Verify a table in a SQLite db has at least min_rows rows."""
    conn = sqlite3.connect(str(db_path))
    count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    conn.close()
    if count < min_rows:
        raise RuntimeError(f"Verification failed: {db_path}:{table} has {count} rows (expected >= {min_rows})")
    return count


def get_year_dbs():
    """Return sorted list of (year, path) for all year .db files in OUTPUT_DIR."""
    dbs = []
    for p in sorted(OUTPUT_DIR.glob("[0-9][0-9][0-9][0-9].db")):
        year = int(p.stem)
        dbs.append((year, str(p)))
    return dbs


def open_readonly(db_path):
    """Open a SQLite database optimized for read-only in-process access.

    Skips journaling, locking, FK checks, and schema validation since the
    database is a pre-built artifact consumed by a single process.
    """
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode = OFF")
    conn.execute("PRAGMA locking_mode = EXCLUSIVE")
    conn.execute("PRAGMA foreign_keys = OFF")
    conn.execute("PRAGMA trusted_schema = ON")
    conn.execute("PRAGMA mmap_size = 268435456")   # 256MB mmap for zero-copy reads
    conn.execute("PRAGMA cache_size = -64000")      # 64MB cache
    conn.execute("PRAGMA temp_store = MEMORY")
    return conn


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
