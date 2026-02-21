# Market Data Database (SQLite)

This directory contains the built SQLite database (per-year `.db` files). Query with `sqlite3` or DuckDB's `sqlite_scan()`.

For table schemas, query patterns, and usage documentation, see [TABLES.md](TABLES.md).

## Quick Start — Read-Optimized Connection

```python
import sqlite3

conn = sqlite3.connect("db_sql/2024.db")
conn.execute("PRAGMA journal_mode = OFF")
conn.execute("PRAGMA locking_mode = EXCLUSIVE")
conn.execute("PRAGMA foreign_keys = OFF")
conn.execute("PRAGMA trusted_schema = ON")
conn.execute("PRAGMA mmap_size = 268435456")
conn.execute("PRAGMA cache_size = -64000")
conn.execute("PRAGMA temp_store = MEMORY")
```

Or use `open_readonly()` from `db_creation_sql/build_common.py`.
