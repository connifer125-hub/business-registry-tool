"""
scrapers/tier3/eu/uk.py
────────────────────────
UK Companies House — Tier 3 scraper

Two modes:
  1. BULK CSV (recommended for initial load) — free monthly download, no key needed
     http://download.companieshouse.gov.uk/en_output.html
  2. REST API  — free key required, good for individual lookups + enrichment
     https://developer.company-information.service.gov.uk

Get a free API key: https://developer.company-information.service.gov.uk/get-started

Run:
  python scrapers/tier3/eu/uk.py --mode bulk  [--limit N] [--dry-run]
  python scrapers/tier3/eu/uk.py --mode api   [--query "acme"] [--limit N] [--dry-run]
"""

import sys
import csv
import io
import os
import time
import argparse
import requests
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()
sys.path.insert(0, str(Path(__file__).resolve().parents[4]))
from utils.normalizer import normalize
from utils.deduplicator import find_duplicate
from db.db import insert_business, log_scrape_run, complete_scrape_run, mark_duplicate

MARKET = "US-UK"  # stored as EU-UK
MARKET = "EU-UK"
TIER = 3

BULK_INDEX_URL = "http://download.companieshouse.gov.uk/en_output.html"
API_BASE       = "https://api.company-information.service.gov.uk"
API_KEY        = os.getenv("COMPANIES_HOUSE_API_KEY", "")

HEADERS_API = {
    "User-Agent": "business-registry-scraper/1.0 (portfolio project)",
}


# ─────────────────────────────────────────────
# MODE 1: Bulk CSV
# ─────────────────────────────────────────────

def get_bulk_csv_urls() -> list[str]:
    """Discover the current bulk CSV download links from the Companies House page."""
    resp = requests.get(BULK_INDEX_URL, timeout=20)
    resp.raise_for_status()
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(resp.text, "lxml")
    urls = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "BasicCompanyData" in href and href.endswith(".zip"):
            urls.append(href if href.startswith("http") else "http://download.companieshouse.gov.uk/" + href)
    return urls


def parse_bulk_csv(content: str, max_records: int) -> list[dict]:
    """Parse a Companies House bulk CSV file."""
    reader = csv.DictReader(io.StringIO(content))
    records = []
    for i, row in enumerate(reader):
        if i >= max_records:
            break
        records.append(dict(row))
    return records


def run_bulk(max_records: int = 100000, dry_run: bool = False):
    """Download and process the Companies House bulk CSV."""
    import zipfile, io as bio

    print("  Fetching bulk CSV index...")
    urls = get_bulk_csv_urls()

    if not urls:
        print("  ✗ Could not find bulk CSV links. Check: " + BULK_INDEX_URL)
        return []

    print(f"  Found {len(urls)} bulk file(s). Downloading first file...")
    resp = requests.get(urls[0], timeout=180, stream=True)
    resp.raise_for_status()

    print("  Extracting ZIP...")
    zf = zipfile.ZipFile(bio.BytesIO(resp.content))
    csv_name = [n for n in zf.namelist() if n.endswith(".csv")][0]
    content = zf.read(csv_name).decode("utf-8", errors="replace")

    records = parse_bulk_csv(content, max_records)
    print(f"  Parsed {len(records)} records from bulk CSV")
    return records


# ─────────────────────────────────────────────
# MODE 2: REST API
# ─────────────────────────────────────────────

def api_search(query: str, start_index: int = 0, items_per_page: int = 100) -> list[dict]:
    """Search Companies House API."""
    if not API_KEY:
        print("  ✗ No COMPANIES_HOUSE_API_KEY set. Add it to .env")
        return []

    url = f"{API_BASE}/search/companies"
    params = {"q": query, "start_index": start_index, "items_per_page": items_per_page}
    resp = requests.get(url, params=params, headers=HEADERS_API, auth=(API_KEY, ""), timeout=20)
    resp.raise_for_status()
    data = resp.json()
    return data.get("items", [])


def api_get_company(company_number: str) -> dict:
    """Get full company profile including SIC codes and registered office."""
    url = f"{API_BASE}/company/{company_number}"
    resp = requests.get(url, headers=HEADERS_API, auth=(API_KEY, ""), timeout=20)
    resp.raise_for_status()
    return resp.json()


# ─────────────────────────────────────────────
# Main run
# ─────────────────────────────────────────────

def process_records(raw_records: list[dict], dry_run: bool) -> dict:
    inserted = dupes = errors = 0

    for raw in raw_records:
        try:
            # Map Companies House bulk CSV fields to our normalizer's expected keys
            if "CompanyName" in raw or "CompanyNumber" in raw:
                raw = {
                    "company_name":   raw.get("CompanyName", ""),
                    "company_number": raw.get("CompanyNumber", ""),
                    "type":           raw.get("CompanyCategory", ""),
                    "company_status": raw.get("CompanyStatus", ""),
                    "date_of_creation": raw.get("IncorporationDate", ""),
                    "date_of_cessation": raw.get("DissolutionDate", ""),
                    "sic_codes": [raw.get("SICCode.SicText_1", "").split(" - ")[0]] if raw.get("SICCode.SicText_1") else [],
                    "registered_office_address": {
                        "address_line_1": raw.get("RegAddress.AddressLine1", ""),
                        "address_line_2": raw.get("RegAddress.AddressLine2", ""),
                        "locality":       raw.get("RegAddress.PostTown", ""),
                        "region":         raw.get("RegAddress.County", ""),
                        "postal_code":    raw.get("RegAddress.PostCode", ""),
                    },
                    **raw
                }

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
                inserted += 1

        except Exception as e:
            print(f"  ✗ Error: {e}")
            errors += 1

    return {"inserted": inserted, "dupes": dupes, "errors": errors}


def run(mode: str = "bulk", max_records: int = 10000,
        query: str = "A", dry_run: bool = False):
    print(f"\n{'[DRY RUN] ' if dry_run else ''}UK Companies House scraper — mode={mode} max={max_records}")
    run_id = None if dry_run else log_scrape_run(MARKET, TIER)

    if mode == "bulk":
        raw_records = run_bulk(max_records, dry_run)
    else:
        # API mode: page through a search query
        raw_records = []
        start = 0
        while len(raw_records) < max_records:
            batch = api_search(query, start_index=start)
            if not batch:
                break
            raw_records.extend(batch)
            start += len(batch)
            time.sleep(0.5)  # API rate limit: 600 req/5 min

    scraped = len(raw_records)
    counts = process_records(raw_records, dry_run)

    if run_id:
        complete_scrape_run(run_id, scraped, counts["inserted"], counts["dupes"], counts["errors"])

    print(f"\n{'[DRY RUN] ' if dry_run else ''}UK complete: {scraped} scraped | {counts['inserted']} inserted | {counts['dupes']} dupes | {counts['errors']} errors")
    return counts


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="UK Companies House scraper")
    parser.add_argument("--mode",    choices=["bulk", "api"], default="bulk")
    parser.add_argument("--limit",   type=int, default=10000)
    parser.add_argument("--query",   type=str, default="A", help="Search query (api mode only)")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    run(mode=args.mode, max_records=args.limit, query=args.query, dry_run=args.dry_run)
