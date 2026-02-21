"""Step 2: Extract ZIP archives and build per-year SQLite databases with hourly price data."""

import shutil
import sqlite3
import tempfile
import zipfile
from pathlib import Path

import duckdb

from build_common import (
    DUCKDB_MEMORY_LIMIT, DUCKDB_THREADS, ETFS_ZIP_DIR, OUTPUT_DIR,
    REGULAR_HOURS_END, REGULAR_HOURS_START,
    STOCKS_ZIP_DIR, TICKER_SUFFIX,
    create_indexes, create_table, log, log_progress, open_year_db_tmp,
    optimize_db, step, verify_table, write_reference_tables,
)

# Parquet settings for temp intermediate files (DuckDB computation only)
PARQUET_SETTINGS = "FORMAT PARQUET, COMPRESSION SNAPPY, ROW_GROUP_SIZE 122880"


@step("prices_v4", target="prices", depends_on=("tickers",))
def build_prices(con):
    """
    Four-pass approach:
    Pass 1: Extract each ZIP, read CSVs, write temp parquet partitioned by year
    Pass 2: For each year, merge fragments, deduplicate, sort, write final temp parquet
    Pass 3: Compute global trading_day_num, write staging_calendar.db,
            enrich each year temp parquet with trading_day_num
    Pass 4: For each year, create {year}.db with reference tables + prices from temp parquet
    """
    temp_fragments_dir = OUTPUT_DIR / "_prices_temp_fragments"
    temp_merged_dir = OUTPUT_DIR / "_prices_temp_merged"

    # Clean any prior state
    for d in [temp_fragments_dir, temp_merged_dir]:
        if d.exists():
            shutil.rmtree(d)

    temp_fragments_dir.mkdir(parents=True)
    temp_merged_dir.mkdir(parents=True)

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

    # Pass 2: For each year, merge fragments, deduplicate, sort, write temp merged parquet
    log("Pass 2: Merging fragments per year...")
    year_dirs = sorted(temp_fragments_dir.glob("year=*"))
    total_years = len(year_dirs)

    for yr_idx, year_dir in enumerate(year_dirs):
        year = year_dir.name.split("=")[1]
        log_progress(yr_idx + 1, total_years, f"Merging year={year}")

        out_path = temp_merged_dir / f"{year}.parquet"

        mcon = duckdb.connect(":memory:")
        mcon.execute(f"SET memory_limit = '{DUCKDB_MEMORY_LIMIT}'")
        mcon.execute(f"SET threads = {DUCKDB_THREADS}")

        frag_pattern = str(year_dir / "*.parquet")

        # Read all fragments for this year, deduplicate: ETF wins over stock
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

        log(f"  year={year}: merged")

    # Clean up temp fragments
    shutil.rmtree(temp_fragments_dir)

    # Pass 3: Compute global trading_day_num and write staging_calendar.db
    log("Pass 3: Computing global trading_day_num...")

    merged_pattern = str(temp_merged_dir / "*.parquet")

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
            FROM read_parquet('{merged_pattern}')
        )
    """)

    cal_count = ccon.execute("SELECT COUNT(*) FROM _calendar").fetchone()[0]
    cal_range = ccon.execute("SELECT MIN(day), MAX(day) FROM _calendar").fetchone()
    log(f"  {cal_count:,} trading days ({cal_range[0]} to {cal_range[1]})")

    # Write staging_calendar.db
    calendar_dest = OUTPUT_DIR / "staging_calendar.db"
    calendar_tmp = Path(str(calendar_dest) + ".tmp")
    if calendar_tmp.exists():
        calendar_tmp.unlink()

    cal_rows = ccon.execute(
        "SELECT trading_day_num, strftime(day, '%Y%m%d')::INTEGER FROM _calendar ORDER BY trading_day_num"
    ).fetchall()

    scalconn = sqlite3.connect(str(calendar_tmp))
    scalconn.execute("PRAGMA journal_mode = WAL")
    scalconn.execute("""
        CREATE TABLE trading_calendar (
            trading_day_num INTEGER PRIMARY KEY,
            day INTEGER NOT NULL
        ) WITHOUT ROWID, STRICT
    """)
    scalconn.executemany("INSERT INTO trading_calendar VALUES (?, ?)", cal_rows)
    scalconn.commit()
    scalconn.close()
    optimize_db(str(calendar_tmp))
    calendar_tmp.rename(calendar_dest)
    log("  Wrote staging_calendar.db")

    # Enrich each year's temp parquet with trading_day_num
    log("  Enriching temp parquet partitions with trading_day_num...")
    merged_files = sorted(temp_merged_dir.glob("*.parquet"))
    for mf in merged_files:
        year = mf.stem
        enriched_path = temp_merged_dir / f"{year}_enriched.parquet"

        ccon.execute(f"""
            COPY (
                SELECT p.ticker, epoch(p.ts)::BIGINT AS ts, p.open, p.high, p.low, p.close, p.volume,
                       c.trading_day_num
                FROM read_parquet('{mf}') p
                JOIN _calendar c ON CAST(p.ts AS DATE) = c.day
                ORDER BY p.ticker, p.ts
            ) TO '{enriched_path}' ({PARQUET_SETTINGS})
        """)
        # Replace original with enriched
        mf.unlink()
        enriched_path.rename(mf)
        log(f"    year={year}: enriched")

    ccon.execute("DROP TABLE _calendar")
    ccon.close()

    # Pass 4: Create year .db files from enriched temp parquet
    log("Pass 4: Writing year SQLite databases...")

    # Load reference data
    tickers_db = OUTPUT_DIR / "staging_tickers.db"
    tconn = sqlite3.connect(str(tickers_db))
    tickers_rows = tconn.execute("SELECT ticker, asset_type FROM tickers ORDER BY ticker").fetchall()
    tconn.close()

    calendar_conn = sqlite3.connect(str(calendar_dest))
    calendar_rows = calendar_conn.execute(
        "SELECT trading_day_num, day FROM trading_calendar ORDER BY trading_day_num"
    ).fetchall()
    calendar_conn.close()

    merged_files = sorted(temp_merged_dir.glob("*.parquet"))
    total_rows = 0

    for mf in merged_files:
        year = int(mf.stem)

        # Read prices from enriched temp parquet via DuckDB
        pcon = duckdb.connect(":memory:")
        rows = pcon.execute(f"""
            SELECT ticker, ts, open, high, low, close, volume, trading_day_num
            FROM read_parquet('{mf}')
            ORDER BY ticker, ts
        """).fetchall()
        pcon.close()

        # Create year .db.tmp
        sconn = open_year_db_tmp(year)
        write_reference_tables(sconn, tickers_rows, calendar_rows)
        create_table(sconn, "prices", with_indexes=False)
        sconn.executemany("INSERT INTO prices VALUES (?, ?, ?, ?, ?, ?, ?, ?)", rows)
        create_indexes(sconn, "prices")
        sconn.commit()
        sconn.close()

        # Optimize and atomic rename
        tmp_path = OUTPUT_DIR / f"{year}.db.tmp"
        dest_path = OUTPUT_DIR / f"{year}.db"
        optimize_db(str(tmp_path))
        tmp_path.rename(dest_path)

        count = verify_table(str(dest_path), "prices")
        total_rows += count
        log(f"  {year}.db: {count:,} price rows")

    # Clean up temp merged parquet
    shutil.rmtree(temp_merged_dir)

    log(f"  Wrote {total_rows:,} total price rows across {len(merged_files)} year dbs")
