"""Step 12: Build cap_lookup — daily_aggs_enriched sorted by (trading_day_num, ticker) for cap-range scans."""

import shutil
from pathlib import Path

from build_common import OUTPUT_DIR, PARQUET_SETTINGS, log, step, verify_parquet


@step("cap_lookup_v2", target="cap_lookup", depends_on=("daily_aggs_enriched",))
def build_cap_lookup(con):
    """Side table with rows that have market cap, sorted by (trading_day_num, ticker) for fast day+cap queries."""
    enriched_pattern = str(OUTPUT_DIR / "daily_aggs_enriched" / "**" / "*.parquet")
    lookup_dir = OUTPUT_DIR / "cap_lookup"
    building_dir = OUTPUT_DIR / "cap_lookup_building"

    if building_dir.exists():
        shutil.rmtree(building_dir)
    building_dir.mkdir(parents=True)

    log("Building cap_lookup from daily_aggs_enriched (cap IS NOT NULL)...")

    con.execute(f"""
        CREATE TABLE _cap_lookup AS
        SELECT day, ticker, cap, close, trading_day_num, cum_close
        FROM read_parquet('{enriched_pattern}', hive_partitioning=true)
        WHERE cap IS NOT NULL
    """)

    total = con.execute("SELECT COUNT(*) FROM _cap_lookup").fetchone()[0]
    log(f"  {total:,} rows with market cap")

    years = [r[0] for r in con.execute(
        "SELECT DISTINCT YEAR(day) AS yr FROM _cap_lookup ORDER BY yr"
    ).fetchall()]

    total_years = len(years)
    total_rows = 0

    for yr_idx, year in enumerate(years):
        out_dir = building_dir / f"year={year}"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / "data.parquet"

        con.execute(f"""
            COPY (
                SELECT day, ticker, cap, close, trading_day_num, cum_close
                FROM _cap_lookup
                WHERE YEAR(day) = {year}
                ORDER BY trading_day_num, ticker
            ) TO '{out_path}' ({PARQUET_SETTINGS})
        """)

        count = verify_parquet(str(out_path))
        total_rows += count
        log(f"  [{yr_idx + 1}/{total_years}] year={year}: {count:,} rows")

    con.execute("DROP TABLE _cap_lookup")

    # Atomic swap
    if lookup_dir.exists():
        stale = Path(str(lookup_dir) + "_old")
        lookup_dir.rename(stale)
        building_dir.rename(lookup_dir)
        shutil.rmtree(stale)
    else:
        building_dir.rename(lookup_dir)

    log(f"  Wrote {total_rows:,} total cap_lookup rows")
