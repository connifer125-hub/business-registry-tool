"""
scrapers/tier1/us/delaware.py
─────────────────────────────
Delaware Division of Corporations — Tier 1 scraper
Source: Delaware Open Data (Socrata API) — no auth required

Endpoint: https://data.delaware.gov/resource/ahdy-uc97.json
Docs:     https://data.delaware.gov/Corporations/Delaware-Division-of-Corporations-Entity-Filing/ahdy-uc97

Run: python scrapers/tier1/us/delaware.py [--limit N] [--offset N] [--dry-run]
"""

import sys
import time
import argparse
import requests
from pathlib import Path

# Allow imports from project root
sys.path.insert(0, str(Path(__file__).resolve().parents[4]))
from utils.normalizer import normalize
from utils.deduplicator import find_duplicate
from db.db import insert_business, log_scrape_run, complete_scrape_run

MARKET = "US-DE"
TIER = 1
API_URL = "https://data.delaware.gov/resource/ahdy-uc97.json"
PAGE_SIZE = 1000


def fetch_page(offset: int, limit: int) -> list[dict]:
    params = {
        "$limit":  limit,
        "$offset": offset,
        "$order":  ":id",
    }
    resp = requests.get(API_URL, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def run(max_records: int = 10000, dry_run: bool = False):
    print(f"\n{'[DRY RUN] ' if dry_run else ''}Delaware scraper starting — max {max_records} records")
    run_id = None if dry_run else log_scrape_run(MARKET, TIER)

    scraped = inserted = dupes = errors = 0
    offset = 0

    while scraped < max_records:
        batch_size = min(PAGE_SIZE, max_records - scraped)
        try:
            records = fetch_page(offset, batch_size)
        except requests.RequestException as e:
            print(f"  ✗ Fetch error at offset {offset}: {e}")
            errors += 1
            break

        if not records:
            print("  ✓ No more records.")
            break

        for raw in records:
            scraped += 1
            try:
                normalized = normalize(MARKET, raw)

                if not normalized.get("business_name"):
                    continue

                dup_id, confidence = find_duplicate(normalized)
                if dup_id:
                    print(f"  ~ Duplicate ({confidence:.0%}): {normalized['business_name']} → #{dup_id}")
                    dupes += 1
                    if not dry_run:
                        from db.db import mark_duplicate
                        bid = insert_business(normalized)
                        if bid:
                            mark_duplicate(bid, dup_id, confidence)
                    continue

                if not dry_run:
                    bid = insert_business(normalized)
                    if bid:
                        inserted += 1
                else:
                    print(f"  [preview] {normalized['business_name']} | {normalized['entity_type']} | {normalized['status']}")
                    inserted += 1

            except Exception as e:
                print(f"  ✗ Error processing record: {e}")
                errors += 1

        print(f"  Page {offset // PAGE_SIZE + 1}: scraped={scraped} inserted={inserted} dupes={dupes} errors={errors}")
        offset += batch_size

        # Polite rate limiting — Socrata is free, don't abuse it
        time.sleep(0.5)

    if run_id:
        complete_scrape_run(run_id, scraped, inserted, dupes, errors)

    print(f"\n{'[DRY RUN] ' if dry_run else ''}Delaware complete: {scraped} scraped | {inserted} inserted | {dupes} dupes | {errors} errors")
    return {"scraped": scraped, "inserted": inserted, "dupes": dupes, "errors": errors}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Delaware business registry scraper")
    parser.add_argument("--limit",   type=int, default=10000, help="Max records to fetch")
    parser.add_argument("--dry-run", action="store_true",     help="Preview only, no DB writes")
    args = parser.parse_args()
    run(max_records=args.limit, dry_run=args.dry_run)
