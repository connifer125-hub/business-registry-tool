"""
scrapers/tier1/us/wyoming.py
─────────────────────────────
Wyoming Secretary of State — Tier 1 scraper
Source: WyoBiz public search + data export

Wyoming offers a bulk file export. This scraper uses their public
search API endpoint which returns JSON records.

Portal:  https://wyobiz.wyo.gov
Run: python scrapers/tier1/us/wyoming.py [--limit N] [--dry-run]
"""

import sys
import time
import argparse
import requests
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[4]))
from utils.normalizer import normalize
from utils.deduplicator import find_duplicate
from db.db import insert_business, log_scrape_run, complete_scrape_run, mark_duplicate

MARKET = "US-WY"
TIER = 1

# Wyoming SOS search API — returns paginated JSON
# Searches all active entities alphabetically
SEARCH_URL = "https://wyobiz.wyo.gov/Business/FilingSearch.aspx"
API_URL    = "https://wyobiz.wyo.gov/api/business/search"

# WY also offers a bulk CSV — if available, prefer that
BULK_CSV_URL = "https://wyobiz.wyo.gov/downloads/AllEntities.csv"

PAGE_SIZE = 500


def fetch_bulk_csv(max_records: int) -> list[dict]:
    """Try to fetch the bulk CSV first — faster and more reliable."""
    import csv, io
    print("  Attempting bulk CSV download...")
    try:
        resp = requests.get(BULK_CSV_URL, timeout=60, stream=True)
        resp.raise_for_status()
        content = resp.content.decode("utf-8", errors="replace")
        reader = csv.DictReader(io.StringIO(content))
        records = []
        for i, row in enumerate(reader):
            if i >= max_records:
                break
            records.append(dict(row))
        print(f"  Bulk CSV: {len(records)} records loaded")
        return records
    except Exception as e:
        print(f"  Bulk CSV unavailable ({e}) — falling back to search API")
        return []


def fetch_via_search(offset: int, limit: int) -> list[dict]:
    """Fallback: page through search results."""
    # WY SOS search: search empty string returns all, sorted by ID
    params = {
        "SearchValue": "",
        "SearchType":  "BusinessName",
        "Status":      "Active",
        "Skip":        offset,
        "Take":        limit,
    }
    headers = {"Accept": "application/json", "X-Requested-With": "XMLHttpRequest"}
    resp = requests.get(API_URL, params=params, headers=headers, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    # API returns either a list or {"Results": [...]}
    if isinstance(data, list):
        return data
    return data.get("Results", data.get("results", []))


def run(max_records: int = 10000, dry_run: bool = False):
    print(f"\n{'[DRY RUN] ' if dry_run else ''}Wyoming scraper starting — max {max_records} records")
    run_id = None if dry_run else log_scrape_run(MARKET, TIER)

    scraped = inserted = dupes = errors = 0

    # Try bulk CSV first
    raw_records = fetch_bulk_csv(max_records)

    # If bulk failed, fall back to search API pagination
    if not raw_records:
        offset = 0
        while scraped < max_records:
            batch_size = min(PAGE_SIZE, max_records - scraped)
            try:
                batch = fetch_via_search(offset, batch_size)
                if not batch:
                    break
                raw_records.extend(batch)
                offset += batch_size
                time.sleep(0.75)
            except Exception as e:
                print(f"  ✗ Fetch error at offset {offset}: {e}")
                errors += 1
                break

    for raw in raw_records[:max_records]:
        scraped += 1
        try:
            normalized = normalize(MARKET, raw)
            if not normalized.get("business_name"):
                continue

            dup_id, confidence = find_duplicate(normalized)
            if dup_id:
                dupes += 1
                if not dry_run:
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
            print(f"  ✗ Error: {e}")
            errors += 1

    if run_id:
        complete_scrape_run(run_id, scraped, inserted, dupes, errors)

    print(f"\n{'[DRY RUN] ' if dry_run else ''}Wyoming complete: {scraped} scraped | {inserted} inserted | {dupes} dupes | {errors} errors")
    return {"scraped": scraped, "inserted": inserted, "dupes": dupes, "errors": errors}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Wyoming business registry scraper")
    parser.add_argument("--limit",   type=int, default=10000)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    run(max_records=args.limit, dry_run=args.dry_run)
