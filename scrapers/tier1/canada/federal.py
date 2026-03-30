"""
scrapers/tier1/canada/federal.py
─────────────────────────────────
Canada Federal — Corporations Canada (ISED) — Tier 1 scraper

Source: ISED Open Data bulk CSV download
URL:    https://ised-isde.canada.ca/site/corporations-canada/en/open-data

The monthly CSV contains all federal corporations registered under the
Canada Business Corporations Act (CBCA). ~400k records.

Download URL changes periodically — this scraper auto-discovers the latest file.
Fallback: direct download from the open data catalog.

Run: python scrapers/tier1/canada/federal.py [--limit N] [--dry-run]
"""

import sys
import csv
import io
import time
import argparse
import requests
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[4]))
from utils.normalizer import normalize
from utils.deduplicator import find_duplicate
from db.db import insert_business, log_scrape_run, complete_scrape_run, mark_duplicate

MARKET = "CA-FED"
TIER = 1

# ISED open data catalog — we fetch this to find the current CSV link
CATALOG_URL = "https://ised-isde.canada.ca/site/corporations-canada/en/open-data"

# Known stable direct download URL (updated periodically by ISED)
DIRECT_CSV_URL = (
    "https://ised-isde.canada.ca/cc/lgcy/fdrlCrpSrch.html"
    "?lang=eng&V_TOKEN=1&prejudice=true&_export=csv"
)

# Fallback: ISED open data API (if CSV link changes)
API_FALLBACK = "https://ised-isde.canada.ca/cc/lgcy/fdrlCrpSrch.json"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; business-registry-scraper/1.0)",
    "Accept-Language": "en-CA,en;q=0.9",
}


def discover_csv_url() -> str | None:
    """Try to find the current CSV download link from the open data page."""
    try:
        resp = requests.get(CATALOG_URL, headers=HEADERS, timeout=20)
        resp.raise_for_status()
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(resp.text, "lxml")
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if href.endswith(".csv") and ("corporation" in href.lower() or "corp" in href.lower()):
                if href.startswith("http"):
                    return href
                return "https://ised-isde.canada.ca" + href
    except Exception as e:
        print(f"  Could not auto-discover CSV URL: {e}")
    return None


def fetch_csv(max_records: int) -> list[dict]:
    """Download and parse the ISED bulk CSV."""
    # Try to discover the URL, fall back to known URL
    csv_url = discover_csv_url() or DIRECT_CSV_URL
    print(f"  Downloading ISED CSV from: {csv_url}")

    try:
        resp = requests.get(csv_url, headers=HEADERS, timeout=120, stream=True)
        resp.raise_for_status()

        # Detect encoding — ISED files are sometimes latin-1
        encoding = resp.encoding or "utf-8"
        content = resp.content.decode(encoding, errors="replace")

        reader = csv.DictReader(io.StringIO(content))
        records = []
        for i, row in enumerate(reader):
            if i >= max_records:
                break
            records.append(dict(row))

        print(f"  Downloaded {len(records)} records from ISED CSV")
        return records

    except Exception as e:
        print(f"  CSV download failed: {e}")
        return []


def map_ised_fields(raw: dict) -> dict:
    """
    Map ISED CSV field names (which vary by year) to our normalizer's expected keys.
    ISED CSV headers have changed over the years — handle common variants.
    """
    def get(*keys):
        for k in keys:
            v = raw.get(k) or raw.get(k.lower()) or raw.get(k.upper())
            if v and str(v).strip():
                return str(v).strip()
        return None

    return {
        "corporation_name":         get("Corporation Name", "CORPORATION_NAME", "corp_name", "Name"),
        "corporation_type":         get("Type", "Corporation Type", "CORP_TYPE", "entity_type"),
        "status":                   get("Status", "STATUS", "Corp Status"),
        "registered_office_address": get("Registered Office Address", "Address", "REG_ADDRESS"),
        "city":                     get("City", "CITY"),
        "province":                 get("Province", "PROVINCE", "Jurisdiction"),
        "postal_code":              get("Postal Code", "POSTAL_CODE", "PostalCode"),
        "date_incorporated":        get("Date Incorporated", "DATE_INCORPORATED", "Incorporation Date", "Inc Date"),
        "dissolution_date":         get("Date Dissolved", "DATE_DISSOLVED", "Dissolution Date"),
        "naics_code":               get("NAICS", "NAICS Code", "naics"),
        "primary_activity":         get("Primary Activity", "Activity", "NAICS Description"),
        **raw  # preserve all original fields in raw_data
    }


def run(max_records: int = 50000, dry_run: bool = False):
    print(f"\n{'[DRY RUN] ' if dry_run else ''}Canada Federal (ISED) scraper starting — max {max_records} records")
    run_id = None if dry_run else log_scrape_run(MARKET, TIER)

    raw_records = fetch_csv(max_records)
    if not raw_records:
        print("  No records fetched. Check the ISED download URL.")
        if run_id:
            complete_scrape_run(run_id, 0, 0, 0, 1)
        return

    scraped = len(raw_records)
    inserted = dupes = errors = 0

    for i, raw in enumerate(raw_records):
        try:
            mapped = map_ised_fields(raw)
            normalized = normalize(MARKET, mapped)

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
                if i < 20:  # only print first 20 in dry run
                    print(f"  [preview] {normalized['business_name']} | {normalized.get('province')} | {normalized['status']}")
                inserted += 1

        except Exception as e:
            print(f"  ✗ Error on record {i}: {e}")
            errors += 1

        if i > 0 and i % 5000 == 0:
            print(f"  Progress: {i}/{scraped} processed — inserted={inserted} dupes={dupes} errors={errors}")

    if run_id:
        complete_scrape_run(run_id, scraped, inserted, dupes, errors)

    print(f"\n{'[DRY RUN] ' if dry_run else ''}Canada Federal complete: {scraped} scraped | {inserted} inserted | {dupes} dupes | {errors} errors")
    return {"scraped": scraped, "inserted": inserted, "dupes": dupes, "errors": errors}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Canada Federal (ISED) business registry scraper")
    parser.add_argument("--limit",   type=int, default=50000)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    run(max_records=args.limit, dry_run=args.dry_run)
