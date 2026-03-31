"""
utils/enricher_google.py
─────────────────────────
Google Places enrichment + pre-QA address corroboration.

For each business record:
1. Search Google Places by business name + city + state
2. Pull back: website, phone, formatted address, place_id, maps URL
3. Compare Google address against registry address
4. Assign a match confidence score and route accordingly:
   - 0.90+ match  → auto_approve
   - 0.70-0.89    → review (human queue, likely good)
   - 0.50-0.69    → flag (address discrepancy)
   - not found    → flag (cannot corroborate)
   - <0.50        → flag (significant mismatch)
"""

import os
import sys
import time
import json
import urllib.request
import urllib.parse
from pathlib import Path
from rapidfuzz import fuzz

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from db.db import get_conn

GOOGLE_API_KEY = os.getenv("GOOGLE_PLACES_API_KEY", "")

PLACES_SEARCH_URL = "https://maps.googleapis.com/maps/api/place/findplacefromtext/json"
PLACES_DETAIL_URL = "https://maps.googleapis.com/maps/api/place/details/json"
TEXTSEARCH_URL    = "https://maps.googleapis.com/maps/api/place/textsearch/json"


def normalize_address(addr: str) -> str:
    if not addr:
        return ""
    addr = addr.upper().strip()
    replacements = {
        " STREET": " ST", " AVENUE": " AVE", " BOULEVARD": " BLVD",
        " DRIVE": " DR", " ROAD": " RD", " LANE": " LN",
        " COURT": " CT", " PLACE": " PL", " SUITE": " STE",
        ".": "", ",": "", "#": ""
    }
    for old, new in replacements.items():
        addr = addr.replace(old, new)
    return " ".join(addr.split())


def address_match_score(registry_addr: str, google_addr: str) -> float:
    if not registry_addr or not google_addr:
        return 0.0
    r = normalize_address(registry_addr)
    g = normalize_address(google_addr)
    return fuzz.token_sort_ratio(r, g) / 100.0


def search_google_places(business_name: str, city: str, state: str) -> dict | None:
    if not GOOGLE_API_KEY:
        print("    No GOOGLE_PLACES_API_KEY set")
        return None

    # Use Text Search — more reliable than Find Place for business lookups
    query = f"{business_name}, {city}, {state}, USA"
    params = urllib.parse.urlencode({
        "query": query,
        "key":   GOOGLE_API_KEY
    })

    try:
        url  = f"{TEXTSEARCH_URL}?{params}"
        req  = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())

        status  = data.get("status")
        results = data.get("results", [])

        if status == "REQUEST_DENIED":
            print(f"    Google API denied: {data.get('error_message', 'unknown')}")
            return None

        if status == "ZERO_RESULTS" or not results:
            return None

        return results[0]

    except Exception as e:
        print(f"    Places search error: {e}")
        return None


def get_place_details(place_id: str) -> dict:
    if not GOOGLE_API_KEY or not place_id:
        return {}

    params = urllib.parse.urlencode({
        "place_id": place_id,
        "fields":   "name,formatted_address,formatted_phone_number,website,url",
        "key":      GOOGLE_API_KEY
    })

    try:
        url = f"{PLACES_DETAIL_URL}?{params}"
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
        return data.get("result", {})

    except Exception as e:
        print(f"    Places detail error: {e}")
        return {}


def determine_qa_routing(match_score: float, found: bool) -> tuple:
    if not found:
        return "flag", "Business not found on Google Places — cannot corroborate"
    if match_score >= 0.90:
        return "auto_approve", f"Google address match {match_score:.0%} — high confidence"
    if match_score >= 0.70:
        return "review", f"Google address match {match_score:.0%} — likely match, review recommended"
    if match_score >= 0.50:
        return "flag", f"Google address match {match_score:.0%} — partial mismatch, needs review"
    return "flag", f"Google address match {match_score:.0%} — significant mismatch or different location"


def enrich_with_google(business_id: int, business_name: str, city: str,
                       state: str, registry_address: str, dry_run: bool = False) -> dict:
    result = {
        "business_id":         business_id,
        "google_found":        False,
        "google_place_id":     None,
        "google_address":      None,
        "google_phone":        None,
        "website_url":         None,
        "google_maps_url":     None,
        "address_match_score": 0.0,
        "address_match":       False,
        "pre_qa_status":       "flag",
        "pre_qa_note":         "Not processed",
    }

    candidate = search_google_places(business_name, city or "", state or "")
    if not candidate:
        result["pre_qa_note"] = "Business not found on Google Places"
        return result

    place_id       = candidate.get("place_id")
    google_address = candidate.get("formatted_address", "")

    # Get full details for website and phone
    details = get_place_details(place_id) if place_id else {}
    if details.get("formatted_address"):
        google_address = details["formatted_address"]

    match_score = address_match_score(registry_address, google_address)
    routing_status, routing_note = determine_qa_routing(match_score, True)

    result.update({
        "google_found":        True,
        "google_place_id":     place_id,
        "google_address":      google_address,
        "google_phone":        details.get("formatted_phone_number"),
        "website_url":         details.get("website"),
        "google_maps_url":     details.get("url"),
        "address_match_score": round(match_score, 3),
        "address_match":       match_score >= 0.70,
        "pre_qa_status":       routing_status,
        "pre_qa_note":         routing_note,
    })

    if not dry_run and place_id:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE businesses SET
                        google_place_id      = %s,
                        google_address       = %s,
                        google_phone         = %s,
                        website_url          = %s,
                        google_maps_url      = %s,
                        address_match_score  = %s,
                        address_match        = %s,
                        pre_qa_status        = %s,
                        pre_qa_note          = %s,
                        updated_at           = NOW()
                    WHERE business_id = %s
                """, (
                    place_id, google_address,
                    details.get("formatted_phone_number"),
                    details.get("website"),
                    details.get("url"),
                    round(match_score, 3),
                    match_score >= 0.70,
                    routing_status,
                    routing_note,
                    business_id
                ))

    return result


def enrich_batch(market: str = "US-DE", limit: int = 20, dry_run: bool = False) -> dict:
    enriched = skipped = errors = auto_approved = flagged = reviewed = 0

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT business_id, business_name, city, state_province,
                       address_line1, registered_address
                FROM businesses
                WHERE source_market = %s
                  AND (google_place_id IS NULL OR google_place_id = '')
                  AND business_name IS NOT NULL
                ORDER BY business_id
                LIMIT %s
            """, (market, limit))
            records = cur.fetchall()

    print(f"\n{'[DRY RUN] ' if dry_run else ''}Google Places enrichment — {len(records)} records from {market}")

    for business_id, business_name, city, state, addr1, reg_addr in records:
        registry_address = addr1 or reg_addr or ""

        try:
            result = enrich_with_google(
                business_id, business_name, city, state,
                registry_address, dry_run
            )

            status_icon = "✓" if result["address_match"] else "~" if result["google_found"] else "✗"
            website_str = result.get("website_url") or "no website"
            print(f"  {status_icon} {business_name[:40]:<40} | match={result['address_match_score']:.0%} | {result['pre_qa_status']} | {website_str[:40]}")

            if result["pre_qa_status"] == "auto_approve":
                auto_approved += 1
            elif result["pre_qa_status"] == "review":
                reviewed += 1
            else:
                flagged += 1

            enriched += 1

        except Exception as e:
            print(f"  x Error on {business_name}: {e}")
            errors += 1

        time.sleep(0.2)

    print(f"\n{'[DRY RUN] ' if dry_run else ''}Google enrichment complete:")
    print(f"  {enriched} enriched | {auto_approved} auto-approve | {reviewed} review | {flagged} flagged | {errors} errors")
    return {"enriched": enriched, "auto_approved": auto_approved, "flagged": flagged, "errors": errors}


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--market",  default="US-DE")
    parser.add_argument("--limit",   type=int, default=20)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    enrich_batch(market=args.market, limit=args.limit, dry_run=args.dry_run)
