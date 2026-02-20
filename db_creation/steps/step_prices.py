"""Step 2: Extract ZIP archives and build Hive-partitioned hourly price data."""

import shutil
import tempfile
import zipfile
from pathlib import Path

import duckdb

from build_common import (
    DUCKDB_MEMORY_LIMIT, DUCKDB_THREADS, ETFS_ZIP_DIR, OUTPUT_DIR,
    PARQUET_SETTINGS, REGULAR_HOURS_END, REGULAR_HOURS_START,
    STOCKS_ZIP_DIR, TICKER_SUFFIX,
    log, log_progress, step, verify_parquet,
)


@step("prices_v4", target="prices", depends_on=("tickers",))
def build_prices(con):
    """
    Three-pass approach:
    Pass 1: Extract each ZIP, read CSVs, write temp parquet partitioned by year
    Pass 2: For each year, merge fragments, deduplicate, sort, write final
    Pass 3: Compute global trading_day_num, write trading_calendar.parquet,
            enrich each year partition with trading_day_num
    """
    prices_dir = OUTPUT_DIR / "prices"
    building_dir = OUTPUT_DIR / "prices_building"
    temp_fragments_dir = OUTPUT_DIR / "_prices_temp_fragments"

    # Clean any prior state
    for d in [building_dir, temp_fragments_dir]:
        if d.exists():
            shutil.rmtree(d)

    temp_fragments_dir.mkdir(parents=True)
    building_dir.mkdir(parents=True)

    # Collect all ZIPs
    stock_zips = sorted(STOCKS_ZIP_DIR.glob("*.zip"))
    etf_zips = sorted(ETFS_ZIP_DIR.glob("*.zip"))
    all_zips = [(z, "stock") for z in stock_zips] + [(z, "etf") for z in etf_zips]
    total_zips = len(all_zips)

    log(f"Pass 1: Processing {total_zips} ZIP files into temp fragments...")

    # Pass 1: Extract each ZIP, read all CSVs, partition by year into temp fragments
    for zip_idx, (zip_path, asset_type) in enumerate(all_zips):
        log_progress(zip_idx + 1, total_zips, f"Processing {zip_path.name}")

        with tempfile.TemporaryDirectory() as tmpdir:
            # Extract ZIP
            with zipfile.ZipFile(zip_path) as zf:
                zf.extractall(tmpdir)

            # Find all txt files
            txt_files = list(Path(tmpdir).rglob("*.txt"))
            if not txt_files:
                log(f"  Warning: no .txt files in {zip_path.name}")
                continue

            # Use DuckDB to read all CSVs at once with filename-based ticker extraction
            zcon = duckdb.connect(":memory:")
            zcon.execute(f"SET memory_limit = '{DUCKDB_MEMORY_LIMIT}'")
            zcon.execute(f"SET threads = {DUCKDB_THREADS}")

            # Read all txt files - they have no header
            glob_pattern = str(Path(tmpdir) / "**" / "*.txt")
            suffix_sql = TICKER_SUFFIX.replace("'", "''")
            try:
                zcon.execute(f"""
                    CREATE TABLE _raw AS
                    SELECT
                        replace(string_split(filename, '/')[-1], '{suffix_sql}', '') AS ticker,
                        column0 AS ts,
                        column1 AS open,
                        column2 AS high,
                        column3 AS low,
                        column4 AS close,
                        column5 AS volume,
                        '{asset_type}' AS _asset_type
                    FROM read_csv(
                        '{glob_pattern}',
                        header=false,
                        columns={{
                            'column0': 'TIMESTAMP',
                            'column1': 'FLOAT',
                            'column2': 'FLOAT',
                            'column3': 'FLOAT',
                            'column4': 'FLOAT',
                            'column5': 'INTEGER'
                        }},
                        filename=true,
                        ignore_errors=true
                    )
                    WHERE replace(string_split(filename, '/')[-1], '{suffix_sql}', '')
                        != string_split(filename, '/')[-1]
                      AND EXTRACT(HOUR FROM column0) BETWEEN {REGULAR_HOURS_START} AND {REGULAR_HOURS_END}
                """)

                row_count = zcon.execute("SELECT COUNT(*) FROM _raw").fetchone()[0]
                if row_count == 0:
                    log(f"  Warning: no valid rows in {zip_path.name}")
                    continue

                # Get distinct years
                years = [r[0] for r in zcon.execute(
                    "SELECT DISTINCT EXTRACT(YEAR FROM ts)::INTEGER AS yr FROM _raw ORDER BY yr"
                ).fetchall()]

                # Write per-year fragments
                frag_id = f"{asset_type}_{zip_path.stem}"
                for year in years:
                    frag_dir = temp_fragments_dir / f"year={year}"
                    frag_dir.mkdir(exist_ok=True)
                    frag_path = frag_dir / f"{frag_id}.parquet"
                    zcon.execute(f"""
                        COPY (
                            SELECT ticker, ts, open, high, low, close, volume, _asset_type
                            FROM _raw
                            WHERE EXTRACT(YEAR FROM ts) = {year}
                            ORDER BY ticker, ts
                        ) TO '{frag_path}' ({PARQUET_SETTINGS})
                    """)

                zcon.execute("DROP TABLE _raw")
            except Exception as e:
                log(f"  Error processing {zip_path.name}: {e}")
            finally:
                zcon.close()

    # Pass 2: For each year, merge fragments, deduplicate, sort, write final
    log("Pass 2: Merging fragments per year...")
    year_dirs = sorted(temp_fragments_dir.glob("year=*"))
    total_years = len(year_dirs)

    for yr_idx, year_dir in enumerate(year_dirs):
        year = year_dir.name.split("=")[1]
        log_progress(yr_idx + 1, total_years, f"Merging year={year}")

        out_dir = building_dir / f"year={year}"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / "data.parquet"

        mcon = duckdb.connect(":memory:")
        mcon.execute(f"SET memory_limit = '{DUCKDB_MEMORY_LIMIT}'")
        mcon.execute(f"SET threads = {DUCKDB_THREADS}")

        frag_pattern = str(year_dir / "*.parquet")

        # Read all fragments for this year, deduplicate: ETF wins over stock
        # Use ROW_NUMBER to pick ETF over stock for overlapping (ticker, ts) pairs
        mcon.execute(f"""
            COPY (
                SELECT ticker, ts, open, high, low, close, volume
                FROM (
                    SELECT *,
                        ROW_NUMBER() OVER (
                            PARTITION BY ticker, ts
                            ORDER BY CASE WHEN _asset_type = 'etf' THEN 0 ELSE 1 END
                        ) AS _rn
                    FROM read_parquet('{frag_pattern}')
                )
                WHERE _rn = 1
                ORDER BY ticker, ts
            ) TO '{out_path}' ({PARQUET_SETTINGS})
        """)
        mcon.close()

        count = verify_parquet(str(out_path))
        log(f"  year={year}: {count:,} rows")

    # Swap in the final directory
    if prices_dir.exists():
        stale = Path(str(prices_dir) + "_old")
        prices_dir.rename(stale)
        building_dir.rename(prices_dir)
        shutil.rmtree(stale)
    else:
        building_dir.rename(prices_dir)

    # Clean up temp fragments
    shutil.rmtree(temp_fragments_dir)

    # Pass 3: Compute global trading_day_num and write trading_calendar.parquet
    log("Pass 3: Computing global trading_day_num...")

    prices_pattern = str(prices_dir / "**" / "*.parquet")
    calendar_dest = OUTPUT_DIR / "trading_calendar.parquet"
    calendar_tmp = Path(str(calendar_dest) + ".tmp")

    ccon = duckdb.connect(":memory:")
    ccon.execute(f"SET memory_limit = '{DUCKDB_MEMORY_LIMIT}'")
    ccon.execute(f"SET threads = {DUCKDB_THREADS}")

    ccon.execute(f"""
        CREATE TABLE _calendar AS
        SELECT
            day,
            ROW_NUMBER() OVER (ORDER BY day)::SMALLINT AS trading_day_num
        FROM (
            SELECT DISTINCT CAST(ts AS DATE) AS day
            FROM read_parquet('{prices_pattern}', hive_partitioning=true)
        )
    """)

    cal_count = ccon.execute("SELECT COUNT(*) FROM _calendar").fetchone()[0]
    cal_range = ccon.execute("SELECT MIN(day), MAX(day) FROM _calendar").fetchone()
    log(f"  {cal_count:,} trading days ({cal_range[0]} to {cal_range[1]})")

    ccon.execute(f"""
        COPY (SELECT trading_day_num, day FROM _calendar ORDER BY trading_day_num)
        TO '{calendar_tmp}' ({PARQUET_SETTINGS})
    """)
    calendar_tmp.rename(calendar_dest)
    verify_parquet(str(calendar_dest))
    log(f"  Wrote trading_calendar.parquet")

    # Enrich each year partition with trading_day_num
    log("  Enriching price partitions with trading_day_num...")
    year_dirs = sorted(prices_dir.glob("year=*"))
    for year_dir in year_dirs:
        year = year_dir.name.split("=")[1]
        data_path = year_dir / "data.parquet"
        tmp_path = year_dir / "data.parquet.tmp"

        ccon.execute(f"""
            COPY (
                SELECT p.ticker, p.ts, p.open, p.high, p.low, p.close, p.volume,
                       c.trading_day_num
                FROM read_parquet('{data_path}') p
                JOIN _calendar c ON CAST(p.ts AS DATE) = c.day
                ORDER BY p.ticker, p.ts
            ) TO '{tmp_path}' ({PARQUET_SETTINGS})
        """)
        tmp_path.rename(data_path)
        log(f"    year={year}: enriched")

    ccon.execute("DROP TABLE _calendar")
    ccon.close()

    total = verify_parquet(str(prices_dir / "**" / "*.parquet"))
    log(f"  Wrote {total:,} total price rows")
