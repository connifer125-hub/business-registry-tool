"""
run_all.py — Scraper orchestrator
───────────────────────────────────
Runs all scrapers in tier order (easiest first).
Can also run individual markets or tiers.

Usage:
  python run_all.py                          # run all, 5000 records each
  python run_all.py --tier 1                 # Tier 1 only
  python run_all.py --market US-DE US-CO     # specific markets
  python run_all.py --limit 1000 --dry-run   # preview without writing to DB
"""

import argparse
import importlib
import sys
import time
from pathlib import Path
from datetime import datetime

# Scraper registry — ordered by tier and recommended build order
SCRAPERS = [
    # (market_code, tier, module_path, run_kwargs)
    ("US-DE",  1, "scrapers.tier1.us.delaware",      {}),
    ("US-CO",  1, "scrapers.tier1.us.colorado",      {}),
    ("US-WY",  1, "scrapers.tier1.us.wyoming",       {}),
    ("US-FL",  1, "scrapers.tier1.us.florida",       {}),
    ("CA-FED", 1, "scrapers.tier1.canada.federal",   {}),
    ("US-CA",  2, "scrapers.tier2.us.california",    {}),
    ("EU-UK",  3, "scrapers.tier3.eu.uk",            {"mode": "bulk"}),
]


def get_scraper_module(module_path: str):
    try:
        return importlib.import_module(module_path)
    except ImportError as e:
        print(f"  ✗ Could not import {module_path}: {e}")
        return None


def run_scraper(market: str, module_path: str, limit: int, dry_run: bool, extra_kwargs: dict):
    print(f"\n{'='*60}")
    print(f"  Market: {market}")
    print(f"  Module: {module_path}")
    print(f"  Limit:  {limit} | Dry run: {dry_run}")
    print(f"{'='*60}")

    mod = get_scraper_module(module_path)
    if not mod:
        return {"market": market, "status": "import_error", "inserted": 0}

    start = time.time()
    try:
        result = mod.run(max_records=limit, dry_run=dry_run, **extra_kwargs)
        elapsed = time.time() - start
        print(f"  ✓ Done in {elapsed:.1f}s")
        return {"market": market, "status": "success", **(result or {})}
    except Exception as e:
        print(f"  ✗ Scraper failed: {e}")
        return {"market": market, "status": "error", "error": str(e)}


def main():
    parser = argparse.ArgumentParser(description="Business Registry scraper orchestrator")
    parser.add_argument("--tier",    type=int, choices=[1, 2, 3],    help="Run only this tier")
    parser.add_argument("--market",  nargs="+",                       help="Run specific market(s) e.g. US-DE US-CO")
    parser.add_argument("--limit",   type=int, default=5000,          help="Max records per market")
    parser.add_argument("--dry-run", action="store_true",             help="Preview only, no DB writes")
    parser.add_argument("--list",    action="store_true",             help="List available scrapers and exit")
    args = parser.parse_args()

    if args.list:
        print("\nAvailable scrapers (in run order):\n")
        for market, tier, path, _ in SCRAPERS:
            print(f"  Tier {tier}  {market:<10}  {path}")
        return

    # Filter which scrapers to run
    to_run = SCRAPERS
    if args.tier:
        to_run = [(m, t, p, kw) for m, t, p, kw in SCRAPERS if t == args.tier]
    if args.market:
        to_run = [(m, t, p, kw) for m, t, p, kw in SCRAPERS if m in args.market]

    if not to_run:
        print("No scrapers matched your filters.")
        return

    print(f"\nBusiness Registry Scraper — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"Running {len(to_run)} scraper(s) | limit={args.limit} | dry_run={args.dry_run}")

    results = []
    for market, tier, path, extra_kwargs in to_run:
        result = run_scraper(market, path, args.limit, args.dry_run, extra_kwargs)
        results.append(result)

    # Summary
    print(f"\n{'='*60}")
    print("RUN SUMMARY")
    print(f"{'='*60}")
    total_inserted = total_dupes = total_errors = 0
    for r in results:
        status_icon = "✓" if r["status"] == "success" else "✗"
        inserted = r.get("inserted", 0)
        dupes    = r.get("dupes", 0)
        errors   = r.get("errors", 0)
        total_inserted += inserted
        total_dupes    += dupes
        total_errors   += errors
        print(f"  {status_icon} {r['market']:<10} inserted={inserted:>6} dupes={dupes:>5} errors={errors:>4}")

    print(f"\n  TOTAL: inserted={total_inserted} dupes={total_dupes} errors={total_errors}")
    if args.dry_run:
        print("\n  [DRY RUN] No records written to database.")


if __name__ == "__main__":
    # Make project root importable
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    main()
