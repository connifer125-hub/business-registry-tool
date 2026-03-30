"""
scrapers/tier1/us/florida.py
─────────────────────────────
Florida Division of Corporations (Sunbiz) — Tier 1 scraper

Florida is one of the best US states for open business data:
- Full bulk data download available (paid, ~$50 one-time)
- Free search portal with stable HTML structure
- This scraper uses the Sunbiz XML/CSV data export approach
  and falls back to HTML scraping for targeted queries.

Portal:    https://dos.fl.gov/sunbiz
Bulk data: https://dos.fl.gov/sunbiz/search/

Run: python scrapers/tier1/us/florida.py [--limit N] [--dry-run]
"""

import sys
import time
import argparse
import requests
from bs4 import BeautifulSoup
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[4]))
from utils.normalizer import normalize
from utils.deduplicator import find_duplicate
from db.db import insert_business, log_scrape_run, complete_scrape_run, mark_duplicate

MARKET = "US-FL"
TIER = 1

# Sunbiz search endpoint — returns HTML results page
SEARCH_URL = "https://search.sunbiz.org/Inquiry/CorporationSearch/SearchResults"

# Search by sequential filing numbers for broad coverage
# FL entity numbers are formatted like: L99000000001
DETAIL_URL = "https://search.sunbiz.org/Inquiry/CorporationSearch/GetFilingImages"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; business-registry-scraper/1.0)",
    "Accept": "text/html,application/xhtml+xml",
}


def search_by_name(query: str, page: int = 1) -> list[dict]:
    """
    Search Sunbiz by business name prefix.
    Returns list of basic records from search results page.
    """
    params = {
        "SearchTerm":      query,
        "SearchType":      "RegisteredAgentName",
        "SearchNameOrder": "FIRSTNAME",
        "Page":            page,
    }
    resp = requests.get(SEARCH_URL, params=params, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    return parse_search_results(resp.text)


def parse_search_results(html: str) -> list[dict]:
    """Parse the Sunbiz search results table."""
    soup = BeautifulSoup(html, "lxml")
    results = []

    table = soup.find("table", class_="search-results")
    if not table:
        # Try alternate table structure
        table = soup.find("table")

    if not table:
        return results

    rows = table.find_all("tr")[1:]  # skip header
    for row in rows:
        cells = row.find_all("td")
        if len(cells) < 3:
            continue
        results.append({
            "Name":         cells[0].get_text(strip=True),
            "FilingNumber": cells[1].get_text(strip=True),
            "Status":       cells[2].get_text(strip=True),
            "FileDate":     cells[3].get_text(strip=True) if len(cells) > 3 else None,
            "EntityType":   cells[4].get_text(strip=True) if len(cells) > 4 else None,
        })
    return results


def search_alphabetical(max_records: int) -> list[dict]:
    """
    Step through alphabet to get broad coverage.
    Sunbiz caps results per query so we use multiple starting letters.
    """
    import string
    all_records = []
    letters = list(string.ascii_uppercase)

    for letter in letters:
        if len(all_records) >= max_records:
            break

        page = 1
        while len(all_records) < max_records:
            try:
                records = search_by_name(letter, page=page)
                if not records:
                    break
                all_records.extend(records)
                print(f"    Letter {letter}, page {page}: +{len(records)} records (total {len(all_records)})")
                page += 1
                time.sleep(1.0)  # Be polite with FL servers
            except Exception as e:
                print(f"    ✗ Error on letter {letter}, page {page}: {e}")
                break

    return all_records[:max_records]


def run(max_records: int = 5000, dry_run: bool = False):
    print(f"\n{'[DRY RUN] ' if dry_run else ''}Florida (Sunbiz) scraper starting — max {max_records} records")
    print("  Note: Florida Tier 1 uses alphabetical HTML scraping. Rate limit: ~1 req/sec.")
    print("  For full bulk data, purchase the FL SOS data file at dos.fl.gov/sunbiz")
    run_id = None if dry_run else log_scrape_run(MARKET, TIER)

    raw_records = search_alphabetical(max_records)
    scraped = len(raw_records)
    inserted = dupes = errors = 0

    for raw in raw_records:
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

    print(f"\n{'[DRY RUN] ' if dry_run else ''}Florida complete: {scraped} scraped | {inserted} inserted | {dupes} dupes | {errors} errors")
    return {"scraped": scraped, "inserted": inserted, "dupes": dupes, "errors": errors}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Florida Sunbiz business registry scraper")
    parser.add_argument("--limit",   type=int, default=5000)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    run(max_records=args.limit, dry_run=args.dry_run)
