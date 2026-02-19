#!/usr/bin/env python3
"""
Market Data Parquet Builder

Transforms raw market data from data_sources/ into sorted Parquet files in db/.
These Parquet files are later loaded into DuckDB by the simulation engine.

Usage:
    python build.py                        # run only pending (new) steps
    python build.py prices                  # rebuild prices + downstream targets
    python build.py insider_trades          # rebuild just insider_trades
    python build.py --full                  # wipe manifest, rebuild everything
    python build.py --list                  # show all steps and status
    python build.py --dry-run               # show what would run
    python build.py --dry-run prices        # show cascade for a target
"""

import argparse
import sys
import time
from datetime import datetime

import steps  # noqa: F401 — triggers @step registration via auto-import

from build_common import (
    OUTPUT_DIR, _steps, cleanup_stale_artifacts, get_downstream_targets,
    load_manifest, log, make_connection, save_manifest,
)
from summary import run_summary


# ===========================================================================
# Engine
# ===========================================================================


def determine_steps_to_run(manifest, requested_targets, full_rebuild):
    """
    Walk the step list and decide which steps to run.
    Returns list of (step_id, target, func) tuples.
    """
    if full_rebuild:
        return [(sid, tgt, fn) for sid, tgt, _, fn in _steps]

    # Determine targets that need rebuilding due to explicit request
    force_targets = set()
    for t in requested_targets:
        force_targets.add(t)
        force_targets.update(get_downstream_targets(t))

    rebuilt_this_run = set()  # targets rebuilt in this execution
    to_run = []

    for step_id, target, depends_on, func in _steps:
        should_run = False

        # New step not in manifest
        if step_id not in manifest:
            should_run = True

        # Target explicitly requested (or downstream of one)
        if target in force_targets:
            should_run = True

        # A dependency was rebuilt this run -> cascade
        if any(dep in rebuilt_this_run for dep in depends_on):
            should_run = True

        if should_run:
            to_run.append((step_id, target, func))
            rebuilt_this_run.add(target)

    return to_run


def run_build(requested_targets, full_rebuild=False, dry_run=False):
    """Main build execution."""
    cleanup_stale_artifacts()

    manifest = {} if full_rebuild else load_manifest()

    steps_to_run = determine_steps_to_run(manifest, requested_targets, full_rebuild)

    if not steps_to_run:
        log("Nothing to do — all steps up to date.")
        print()
        run_summary()
        return True

    if dry_run:
        log("Dry run — would execute these steps:")
        for step_id, target, _ in steps_to_run:
            status = "NEW" if step_id not in manifest else "REBUILD"
            log(f"  {step_id} (target={target}) [{status}]")
        return True

    log(f"Running {len(steps_to_run)} step(s)...")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    for step_id, target, func in steps_to_run:
        log(f"── Step: {step_id} (target={target}) ──")
        t0 = time.time()

        con = make_connection()
        try:
            func(con)
        except Exception as e:
            log(f"FAILED: {step_id}: {e}")
            return False
        finally:
            con.close()

        elapsed = time.time() - t0
        manifest[step_id] = {
            "completed_at": datetime.now().isoformat(),
            "elapsed_seconds": round(elapsed, 1),
        }
        save_manifest(manifest)
        log(f"  Done in {elapsed:.1f}s")

    log("Build complete.")
    print()
    run_summary()
    return True


def list_steps():
    """Show all steps and their status."""
    manifest = load_manifest()
    for step_id, target, depends_on, _ in _steps:
        if step_id in manifest:
            info = manifest[step_id]
            completed = info.get("completed_at", "?")
            elapsed = info.get("elapsed_seconds", "?")
            status = f"DONE ({completed}, {elapsed}s)"
        else:
            status = "PENDING"
        deps = f" depends_on=({', '.join(depends_on)})" if depends_on else ""
        print(f"  {step_id:30s} target={target:20s} {status}{deps}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(
        description="Build market data Parquet files from raw sources",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "targets", nargs="*",
        help="Specific target(s) to rebuild (e.g., prices, daily_aggs). Cascades to dependents.",
    )
    parser.add_argument(
        "--full", action="store_true",
        help="Wipe manifest and rebuild everything from scratch.",
    )
    parser.add_argument(
        "--list", action="store_true", dest="list_steps",
        help="Show all steps and their build status.",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Show what would run without actually running.",
    )
    args = parser.parse_args()

    if args.list_steps:
        list_steps()
        return

    # Validate target names
    known_targets = {tgt for _, tgt, _, _ in _steps}
    for t in args.targets:
        if t not in known_targets:
            sys.exit(f"Unknown target: '{t}'. Known targets: {', '.join(sorted(known_targets))}")

    success = run_build(
        requested_targets=args.targets,
        full_rebuild=args.full,
        dry_run=args.dry_run,
    )
    if not success:
        sys.exit(1)


if __name__ == "__main__":
    main()
