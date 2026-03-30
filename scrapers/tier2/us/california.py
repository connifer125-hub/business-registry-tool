"""
scrapers/tier2/us/california.py
────────────────────────────────
California Secretary of State — Tier 2 scraper
Source: Bizfile Online portal (JS-rendered, requires Playwright)

Portal: https://bizfileonline.sos.ca.gov/search/business

CA has no bulk download — records are fetched via the search portal.
This scraper pages through results alphabetically.

Prerequisites:
  pip install playwright
  playwright install chromium

Run: python scrapers/tier2/us/california.py [--limit N] [--dry-run]
"""

import sys
import time
import asyncio
import argparse
import json
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[4]))
from utils.normalizer import normalize
from utils.deduplicator import find_duplicate
from db.db import insert_business, log_scrape_run, complete_scrape_run, mark_duplicate

MARKET = "US-CA"
TIER = 2
SEARCH_URL = "https://bizfileonline.sos.ca.gov/search/business"

# CA SOS internal API endpoint (called by the JS app)
API_URL = "https://bizfileonline.sos.ca.gov/api/Records/businesssearch"

HEADERS = {
    "User-Agent":   "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept":       "application/json",
    "Content-Type": "application/json",
    "Origin":       "https://bizfileonline.sos.ca.gov",
    "Referer":      "https://bizfileonline.sos.ca.gov/search/business",
}


async def fetch_via_playwright(query: str, page_num: int) -> list[dict]:
    """Use Playwright to interact with the CA bizfile portal."""
    from playwright.async_api import async_playwright

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
        )
        page = await context.new_page()

        results = []
        try:
            await page.goto(SEARCH_URL, wait_until="networkidle", timeout=30000)

            # Fill search field
            await page.fill('input[placeholder*="business" i], input[type="search"], input[name*="search" i]', query)
            await page.keyboard.press("Enter")
            await page.wait_for_load_state("networkidle", timeout=15000)

            # Extract results from rendered table
            rows = await page.query_selector_all("table tbody tr, .result-row, .search-result")
            for row in rows:
                cells = await row.query_selector_all("td")
                if len(cells) >= 2:
                    name   = await cells[0].inner_text() if cells else ""
                    number = await cells[1].inner_text() if len(cells) > 1 else ""
                    status = await cells[2].inner_text() if len(cells) > 2 else ""
                    btype  = await cells[3].inner_text() if len(cells) > 3 else ""
                    results.append({
                        "businessName":    name.strip(),
                        "businessNumber":  number.strip(),
                        "status":          status.strip(),
                        "businessType":    btype.strip(),
                    })

        except Exception as e:
            print(f"    Playwright error: {e}")
        finally:
            await browser.close()

        return results


def fetch_via_api(query: str, skip: int = 0, take: int = 100) -> list[dict]:
    """
    Attempt to call the CA SOS internal search API directly.
    This works if the portal doesn't require a session cookie.
    Falls back to Playwright if this returns 403/401.
    """
    import requests
    payload = {
        "SEARCH_VALUE":     query,
        "SEARCH_FILTER_TYPE_ID": "0",
        "SEARCH_TYPE_ID":   "1",
        "FILING_TYPE_ID":   "",
        "STATUS_ID":        "0",
        "FILING_DATE":      "",
        "ENTITY_TYPE":      "",
        "skip":             skip,
        "take":             take,
    }
    resp = requests.post(API_URL, json=payload, headers=HEADERS, timeout=30)
    if resp.status_code in (401, 403):
        return []  # will fall back to Playwright
    resp.raise_for_status()
    data = resp.json()
    return data.get("hits", data.get("results", data if isinstance(data, list) else []))


def run_alphabet_scan(max_records: int, dry_run: bool) -> list[dict]:
    """Page through all businesses alphabetically using API or Playwright fallback."""
    import string
    all_records = []

    for letter in string.ascii_uppercase:
        if len(all_records) >= max_records:
            break

        skip = 0
        while len(all_records) < max_records:
            # Try direct API first (faster, no browser overhead)
            records = fetch_via_api(letter, skip=skip, take=100)

            if not records:
                # Fall back to Playwright
                print(f"    API unavailable for '{letter}', using Playwright...")
                records = asyncio.run(fetch_via_playwright(letter, skip // 100))

            if not records:
                break

            all_records.extend(records)
            print(f"    Letter {letter}, page {skip//100 + 1}: +{len(records)} (total {len(all_records)})")
            skip += 100
            time.sleep(1.5)  # Polite delay for CA portal

            if len(records) < 100:
                break  # last page

    return all_records[:max_records]


def run(max_records: int = 5000, dry_run: bool = False):
    print(f"\n{'[DRY RUN] ' if dry_run else ''}California scraper starting — max {max_records} records")
    print("  Tier 2: uses CA SOS portal (JS-rendered). Playwright required for fallback.")
    run_id = None if dry_run else log_scrape_run(MARKET, TIER)

    raw_records = run_alphabet_scan(max_records, dry_run)
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

    print(f"\n{'[DRY RUN] ' if dry_run else ''}California complete: {scraped} scraped | {inserted} inserted | {dupes} dupes | {errors} errors")
    return {"scraped": scraped, "inserted": inserted, "dupes": dupes, "errors": errors}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="California SOS business registry scraper")
    parser.add_argument("--limit",   type=int, default=5000)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    run(max_records=args.limit, dry_run=args.dry_run)
