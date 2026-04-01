"""
utils/enricher.py
─────────────────
Claude AI enrichment for business records.
Matches business_nature descriptions to official SIC codes.
"""

import sys
import os
import json
import time
import urllib.request
import urllib.error
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from db.db import get_conn

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")

SIC_SYSTEM_PROMPT = """You are a business classification expert. Given a business description, return the single best matching SIC (Standard Industrial Classification) code and a clean one-sentence business description.

Respond ONLY with a JSON object in this exact format, no other text:
{"sic_code": "5812", "sic_description": "Eating Places", "clean_description": "Full-service restaurant providing dine-in meals to the public."}

Common SIC codes for reference:
- Restaurants/Food Service: 5812
- Retail Stores (general): 5900
- Software/Technology: 7372
- Computer Services/IT: 7371
- Healthcare/Medical: 8099
- Dental: 8021
- Legal Services: 8111
- Accounting: 8721
- Real Estate: 6500
- Construction (general): 1500
- Trucking/Transport: 4210
- Auto Repair: 7538
- Hair/Beauty Salons: 7231
- Cleaning Services: 7349
- Consulting: 7389
- Marketing/Advertising: 7311
- Insurance: 6411
- Financial Services: 6199
- Child Care: 8351
- Fitness/Gyms: 7991
- Hotels/Lodging: 7011
- Agriculture: 0100
- Manufacturing (general): 3999
- Wholesale Trade: 5190
- E-commerce/Online Retail: 5999
- Nonprofit/Charity: 8399
- Education: 8200
- Churches/Religious: 8661"""


def enrich_record(business_id: int, business_nature: str, business_name: str) -> dict:
    """Call Claude API to get SIC code and clean description for one record."""

    if not ANTHROPIC_API_KEY:
        return {"error": "No ANTHROPIC_API_KEY set in environment"}

    prompt = f'Business name: "{business_name}"\nBusiness description: "{business_nature}"\n\nReturn the best SIC code and a clean description.'

    payload = json.dumps({
        "model": "claude-haiku-4-5-20251001",
        "max_tokens": 200,
        "system": SIC_SYSTEM_PROMPT,
        "messages": [{"role": "user", "content": prompt}]
    }).encode("utf-8")

    req = urllib.request.Request(
        "https://api.anthropic.com/v1/messages",
        data=payload,
        headers={
            "x-api-key":          ANTHROPIC_API_KEY,
            "anthropic-version":  "2023-06-01",
            "content-type":       "application/json",
        },
        method="POST"
    )

    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read())
            text = data["content"][0]["text"].strip()
            result = json.loads(text)
            return result
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        return {"error": f"HTTP {e.code}: {body[:300]}"}
    except Exception as e:
        return {"error": str(e)}


def enrich_batch(market: str = "US-DE", limit: int = 50, dry_run: bool = False) -> dict:
    """
    Enrich records that have a business_nature but no SIC code.
    Writes SIC code and enriched description back to the database.
    """
    enriched = skipped = errors = 0

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT business_id, business_name,
                       raw_data->>'business_nature__c' as business_nature
                FROM businesses
                WHERE source_market = %s
                  AND (sic_code IS NULL OR sic_code = '')
                  AND raw_data->>'business_nature__c' IS NOT NULL
                  AND raw_data->>'business_nature__c' != ''
                ORDER BY business_id
                LIMIT %s
            """, (market, limit))
            records = cur.fetchall()

    print(f"\n{'[DRY RUN] ' if dry_run else ''}Enriching {len(records)} records from {market}...")

    for business_id, business_name, business_nature in records:
        if not business_nature or len(business_nature.strip()) < 3:
            skipped += 1
            continue

        result = enrich_record(business_id, business_nature, business_name)

        if "error" in result:
            print(f"  x Error on #{business_id} {business_name}: {result['error']}")
            errors += 1
            continue

        sic_code   = result.get("sic_code", "")
        sic_desc   = result.get("sic_description", "")
        clean_desc = result.get("clean_description", "")

        print(f"  {business_name[:40]:<40} -> SIC {sic_code} ({sic_desc})")

        if not dry_run:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE businesses
                        SET sic_code = %s,
                            industry_desc = %s,
                            updated_at = NOW()
                        WHERE business_id = %s
                    """, (sic_code, clean_desc or sic_desc, business_id))

        enriched += 1
        time.sleep(0.3)

    print(f"\n{'[DRY RUN] ' if dry_run else ''}Enrichment complete: {enriched} enriched | {skipped} skipped | {errors} errors")
    return {"enriched": enriched, "skipped": skipped, "errors": errors}


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--market",  default="US-DE")
    parser.add_argument("--limit",   type=int, default=50)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    enrich_batch(market=args.market, limit=args.limit, dry_run=args.dry_run)
