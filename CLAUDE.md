# Market Data

Two database variants built from the same raw sources in `data_sources/`:

| Variant | Build system | Output | Docs |
|---------|-------------|--------|------|
| **Parquet/DuckDB** | `db_creation/` | `db/` | `db_creation/db_docs/` |
| **SQLite** | `db_creation_sql/` | `db_sql/` | `db_creation_sql/db_docs/` |

Each `db_docs/` folder contains `TABLES.md` (schemas, query patterns) and `CLAUDE.md` for the output database. These are copied into `db/` and `db_sql/` on each build.

**Whenever you edit the resulting tables by modifying the steps, update the corresponding `db_docs/TABLES.md` so it always remains accurate.**

---

## Build System Reference

Both build systems share the same architecture. All build code lives in `db_creation/` (Parquet) or `db_creation_sql/` (SQLite). Steps produce the output files.

### File layout

```
db_creation/                    # (same structure for db_creation_sql/)
├── build.py              # CLI entry point
├── build_common.py       # Constants, @step decorator, helpers
├── summary.py            # Post-build data preview (ALL_TABLES dict must match active tables)
├── db_docs/              # TABLES.md + CLAUDE.md → copied to output dir on build
│   ├── CLAUDE.md
│   └── TABLES.md
└── steps/
    ├── __init__.py       # Auto-imports step_*.py alphabetically
    ├── step_tickers.py
    ├── step_prices.py
    └── ...               # step_{name}.py — execution order is by depends_on
```

### The `@step` decorator

Every step is a function decorated with `@step()` in a `step_*.py` file under `steps/`. The decorator registers the step; `__init__.py` auto-imports all `step_*.py` files alphabetically. After import, `finalize_step_order()` topologically sorts steps by `depends_on`, so **execution order is determined by the dependency graph, not by filename**.

```python
from build_common import OUTPUT_DIR, PARQUET_SETTINGS, log, step, verify_parquet

@step("my_table_v1", target="my_table", depends_on=("prices",))
def build_my_table(con):
    ...
```

Parameters:
- **`step_id`**: Unique string, conventionally `"{target}_v{version}"`. Tracked in the manifest. Bump the version when the logic changes to trigger a rebuild.
- **`target`**: Logical name for what this step produces. Used for dependency resolution and CLI targeting (`python build.py my_table`).
- **`depends_on`**: Tuple of target names this step requires. Controls cascade — rebuilding a dependency triggers downstream rebuilds.
- **`disabled`**: Set `disabled=True` to keep the code but skip execution. Shows as `DISABLED` in `--list`. Override with `--include-disabled`.

The function receives a DuckDB in-memory connection (`con`) with 12GB memory limit and all CPU threads.

### Naming convention

Files: `step_{target_name}.py` — one file per step. Execution order is determined by `depends_on` (topological sort), not by filename.

Step IDs: `{target}_v{version}` — bump version when logic changes so the manifest detects it as new.

### Output patterns (Parquet variant)

**Single file** (small tables):
```python
dest = OUTPUT_DIR / "my_table.parquet"
tmp = Path(str(dest) + ".tmp")
con.execute(f"COPY (...) TO '{tmp}' ({PARQUET_SETTINGS})")
tmp.rename(dest)
count = verify_parquet(str(dest))
```

**Hive-partitioned by year** (large tables):
```python
out_dir = OUTPUT_DIR / "my_table"
building_dir = OUTPUT_DIR / "my_table_building"
if building_dir.exists():
    shutil.rmtree(building_dir)
building_dir.mkdir(parents=True)

# ... compute data, iterate per year ...
for year in years:
    year_dir = building_dir / f"year={year}"
    year_dir.mkdir(parents=True, exist_ok=True)
    out_path = year_dir / "data.parquet"
    con.execute(f"COPY (...) TO '{out_path}' ({PARQUET_SETTINGS})")
    verify_parquet(str(out_path))

# Atomic swap
if out_dir.exists():
    stale = Path(str(out_dir) + "_old")
    out_dir.rename(stale)
    building_dir.rename(out_dir)
    shutil.rmtree(stale)
else:
    building_dir.rename(out_dir)
```

Partitioned files must be named `data.parquet` (one per `year=YYYY/` directory). Query pattern: `read_parquet('db/my_table/**/data.parquet', hive_partitioning=true)`.

### Temp file cleanup

Any file or directory under the output dir ending in `.tmp` is automatically deleted by `cleanup_stale_artifacts()` at the start of each build. Use the `.tmp` suffix for intermediate files that should not survive a failed run (e.g. `data.parquet.tmp`, `my_table.parquet.tmp`). You don't need to clean them up manually — just name them `*.tmp` and the build system handles it.

### Using intermediate data

Steps that need data from raw sources (not from another step's output) should compute it in-memory:

```python
# Load into temp table, use it, drop it
con.execute("CREATE TABLE _tmp AS SELECT ... FROM read_csv(...)")
con.execute("CREATE TABLE _result AS SELECT ... FROM _tmp JOIN ...")
con.execute("DROP TABLE _tmp")
# ... COPY _result to parquet per year ...
con.execute("DROP TABLE _result")
```

Prefix temp tables with `_` to distinguish from output.

### Key imports from `build_common`

| Symbol | What |
|--------|------|
| `OUTPUT_DIR` | `Path` to output dir (`db/` or `db_sql/`) |
| `PARQUET_SETTINGS` | `"FORMAT PARQUET, COMPRESSION SNAPPY, ROW_GROUP_SIZE 122880"` (Parquet variant only) |
| `step` | The `@step` decorator |
| `log(msg)` | Timestamped stderr logging |
| `verify_parquet(path)` | Asserts file has >= 1 row, returns count (Parquet variant only) |
| `STOCKS_ZIP_DIR` | `data_sources/stocks/data/` |
| `ETFS_ZIP_DIR` | `data_sources/etfs/data/` |
| `MARKET_CAP_DIR` | `data_sources/market_cap/data/` (CSVs with `date`, `market_cap` columns, one file per ticker, ticker in filename) |
| `INSIDER_TRADES_DIR` | `data_sources/insider_trades/data/` (JSONL.GZ files, `YYYY/YYYY-MM.jsonl.gz`, top-level fields include `filedAt`, `issuer.tradingSymbol`, `reportingOwner`, `nonDerivativeTable.transactions[]`) |

### CLI

```bash
python build.py                    # run only new/pending steps
python build.py my_table           # rebuild my_table + all downstream
python build.py --full             # wipe manifest, rebuild everything
python build.py --list             # show all steps and status
python build.py --dry-run          # show what would run
python build.py --include-disabled # also run disabled steps
```

### Manifest

`.build_manifest.json` in the output dir tracks `{step_id: {completed_at, elapsed_seconds}}`. A step runs if its `step_id` is not in the manifest (new or version-bumped), if its target was explicitly requested, or if an upstream dependency was rebuilt. Delete the manifest (or use `--full`) to force a full rebuild.

### Checklist for adding a new step

1. Create `steps/step_{name}.py` in the appropriate build system
2. Decorate with `@step("{name}_v1", target="{name}", depends_on=(...))`
3. Write the build function following the output pattern for that variant
4. Add a summary function in `summary.py` and add it to `ALL_TABLES`
5. Document the table schema in `db_docs/TABLES.md`

### Checklist for disabling a step

1. Add `disabled=True` to the `@step()` decorator
2. Remove from `ALL_TABLES` in `summary.py` (or leave — it handles "not found" gracefully)
3. Remove or mark the table as disabled in `db_docs/TABLES.md`

---

## Data Quality Pipeline

Raw hourly price data contains garbage prices (stocks flipping between ~$14 and $0.016, zero closes, split-adjustment artifacts). The data quality pipeline detects anomalies at the **hourly bar level**, decides what to nuke vs drop, and applies cleanup before building daily aggregates.

### Architecture

```
step_data_quality  →  step_daily_aggs_enriched
   (detect + decide)       (apply exclusions when building daily aggs)
```

### Detection (`step_data_quality`)

Scans all hourly bars from year SQLite databases. For each bar, computes:
- `prev_close` / `next_close` via LAG/LEAD
- `median_before`: median of the **50 bars before** (~7 trading days)
- `median_after`: median of the **50 bars after**

A bar is flagged if **both** medians agree it's anomalous:
- `zero_or_negative`: close ≤ 0, or both medians ≤ 0
- `price_too_low`: close < 15% of both medians
- `price_too_high`: close > 7× both medians
- Edge: if one median is NULL or ≤ 0, that side auto-qualifies

**Why both medians?** A real price change has an after-median matching the new price, so it won't trigger. Garbage data differs from both windows.

### Nuke Rules

A flagged ticker gets **nuked** (all data removed from all tables) if it hits any of:

| # | Rule | Condition |
|---|------|-----------|
| 1 | Absurd absolute price | Any flagged close > $1,000,000 |
| 2 | Absurd relative price | Any flagged close > 100× either median |
| 3 | High anomaly count | 5+ anomalous trading days |
| 4 | Derivative suffix | Ticker matches `^[A-Z]{2,5}(WS\|W\|R\|P\|U)$` |
| 5 | Stock died | Any after-median = $0.00 |
| 6 | Contaminated medians | Either median > 50× close, 2+ anomalous days |

Surviving tickers (1–2 minor anomalies, no rule match) → only those specific bars are dropped.

### Output

Writes **`db_sql/staging_exclusions.db`** with two tables:

```sql
CREATE TABLE nuked_tickers (
    ticker TEXT PRIMARY KEY,
    nuke_reason TEXT NOT NULL
) WITHOUT ROWID, STRICT;

CREATE TABLE dropped_bars (
    ticker TEXT NOT NULL,
    ts INTEGER NOT NULL,
    flag_reason TEXT NOT NULL,
    PRIMARY KEY (ticker, ts)
) WITHOUT ROWID, STRICT;
```

### Applying Exclusions (`step_daily_aggs_enriched`)

Before aggregating hourly bars to daily OHLCV:
1. Reads `staging_exclusions.db` (if it exists)
2. Deletes all hourly bars for nuked tickers
3. Deletes specific flagged hourly bars for surviving tickers
4. Then aggregates remaining clean bars to daily

A day with 1 bad bar out of 7 keeps its other 6 bars — the daily OHLCV is computed from the surviving bars.

### Usage

```bash
cd db_creation_sql

# Run detection — prints color-coded report, writes staging_exclusions.db
python build.py data_quality

# Review the staging DB
sqlite3 ../db_sql/staging_exclusions.db "SELECT COUNT(*) FROM nuked_tickers; SELECT COUNT(*) FROM dropped_bars;"

# Rebuild daily_aggs_enriched to apply exclusions
python build.py daily_aggs_enriched
```

On a full build (`--full`), both steps run in sequence automatically.

### Tuning the thresholds

Detection thresholds are hardcoded in `step_data_quality.py`: `< 0.15` (too low) and `> 7` (too high). Nuke rules are in `_apply_nuke_rules()`. To change them, edit the code and bump the step_id version.
