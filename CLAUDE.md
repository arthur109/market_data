# Market Data

Parquet-based market database in `db/`. Built by `db_creation/`.

**For table schemas, query patterns, and database documentation, see [`TABLES.md`](TABLES.md).** A copy is also placed in `db/TABLES.md` on each build. **Whenever you edit the resulting tables by modifying the steps, please update TABLES.md so it always remains accurate.**

---

## Build System Reference

All build code lives in `db_creation/`. Steps produce the parquet files in `db/`.

### File layout

```
db_creation/
├── build.py              # CLI entry point
├── build_common.py       # Constants, @step decorator, helpers
├── summary.py            # Post-build data preview (ALL_TABLES dict must match active tables)
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

### Output patterns

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

Any file or directory under `db/` ending in `.tmp` is automatically deleted by `cleanup_stale_artifacts()` at the start of each build. Use the `.tmp` suffix for intermediate files that should not survive a failed run (e.g. `data.parquet.tmp`, `my_table.parquet.tmp`). You don't need to clean them up manually — just name them `*.tmp` and the build system handles it.

### Using intermediate data

Steps that need data from raw sources (not from another step's parquet output) should compute it in-memory:

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
| `OUTPUT_DIR` | `Path` to `db/` |
| `PARQUET_SETTINGS` | `"FORMAT PARQUET, COMPRESSION SNAPPY, ROW_GROUP_SIZE 122880"` |
| `step` | The `@step` decorator |
| `log(msg)` | Timestamped stderr logging |
| `verify_parquet(path)` | Asserts file has >= 1 row, returns count |
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

`db/.build_manifest.json` tracks `{step_id: {completed_at, elapsed_seconds}}`. A step runs if its `step_id` is not in the manifest (new or version-bumped), if its target was explicitly requested, or if an upstream dependency was rebuilt. Delete the manifest (or use `--full`) to force a full rebuild.

### Checklist for adding a new step

1. Create `db_creation/steps/step_{name}.py`
2. Decorate with `@step("{name}_v1", target="{name}", depends_on=(...))`
3. Write the build function following the single-file or partitioned pattern above
4. Add a summary function in `summary.py` and add it to `ALL_TABLES`
5. Document the table schema in `TABLES.md`

### Checklist for disabling a step

1. Add `disabled=True` to the `@step()` decorator
2. Remove from `ALL_TABLES` in `summary.py` (or leave — it handles "not found" gracefully)
3. Remove or mark the table as disabled in `TABLES.md`