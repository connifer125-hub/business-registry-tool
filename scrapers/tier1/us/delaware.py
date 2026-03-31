"""
scrapers/tier1/us/delaware.py
─────────────────────────────
Delaware Trade, Business & Fictitious Names — Tier 1 scraper
Source: Delaware Open Data (Socrata API) — no auth required

Endpoint: https://data.delaware.gov/resource/i7m4-42sn.json
"""

import sys
import time
import argparse
import requests
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from utils.normalizer import normalize
from utils.deduplicator import find_existing_same_source, refresh_existing, find_duplicate
from db.db import insert_business, log_scrape_run, complete_scrape_run, mark_duplicate

MARKET = "US-DE"
TIER = 1
API_URL = "https://data.delaware.gov/resource/i7m4-42sn.json"
PAGE_SIZE = 100


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

    scraped = inserted = refreshed = dupes = errors = 0
    offset = 0

    while scraped < max_records:
        batch_size = min(PAGE_SIZE, max_records - scraped)
        try:
            records = fetch_page(offset, batch_size)
        except requests.RequestException as e:
            print(f"  x Fetch error at offset {offset}: {e}")
            errors += 1
            break

        if not records:
            print("  Done. No more records.")
            break

        for raw in records:
            scraped += 1
            try:
                mapped = {
                    "businessname":       raw.get("trade_name__c") or raw.get("owner_company__c") or "",
                    "trade_name":         raw.get("trade_name__c"),
                    "entity_kind":        raw.get("business_nature__c"),
                    "status":             "Active",
                    "registered_address": raw.get("streetaddressline1__c") or raw.get("address__c"),
                    "address_line1":      raw.get("streetaddressline1__c") or "",
                    "address_line2":      raw.get("streetaddressline2__c") or "",
                    "city":               raw.get("city__c") or "",
                    "zip":                raw.get("zip__c") or "",
                    "incdate":            raw.get("formation_date__c"),
                    "registered_agent":   raw.get("affiant_name__c"),
                    "phone":              raw.get("phone_number__c"),
                    "filing_id":          raw.get("assignedfilingid__c"),
                    "license_number":     raw.get("associated_license_numbers"),
                    **raw
                }

                normalized = normalize(MARKET, mapped)
                if not normalized.get("business_name"):
                    continue

                if dry_run:
                    print(f"  [preview] {normalized['business_name']} | {normalized.get('city')} | {normalized.get('address_line1')}")
                    inserted += 1
                    continue

                # Check if this record already exists from same source (refresh)
                existing = find_existing_same_source(normalized)
                if existing:
                    result = refresh_existing(existing, normalized)
                    if result["changed_fields"]:
                        print(f"  ~ Refreshed #{existing['business_id']} {normalized['business_name']} — changed: {', '.join(result['changed_fields'])}")
                    refreshed += 1
                    continue

                # Check for cross-source duplicates
                dup_id, confidence = find_duplicate(normalized)
                if dup_id:
                    dupes += 1
                    bid = insert_business(normalized)
                    if bid:
                        mark_duplicate(bid, dup_id, confidence)
                    continue

                # New record — insert
                bid = insert_business(normalized)
                if bid:
                    inserted += 1

            except Exception as e:
                print(f"  x Error processing record: {e}")
                errors += 1

        print(f"  Page {offset // PAGE_SIZE + 1}: scraped={scraped} new={inserted} refreshed={refreshed} dupes={dupes} errors={errors}")
        offset += batch_size
        time.sleep(0.5)

    if run_id:
        complete_scrape_run(run_id, scraped, inserted, dupes, errors)

    print(f"\n{'[DRY RUN] ' if dry_run else ''}Delaware complete: {scraped} scraped | {inserted} new | {refreshed} refreshed | {dupes} cross-source dupes | {errors} errors")
    return {"scraped": scraped, "inserted": inserted, "refreshed": refreshed, "dupes": dupes, "errors": errors}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Delaware business registry scraper")
    parser.add_argument("--limit",   type=int, default=10000, help="Max records to fetch")
    parser.add_argument("--dry-run", action="store_true",     help="Preview only, no DB writes")
    args = parser.parse_args()
    run(max_records=args.limit, dry_run=args.dry_run)
