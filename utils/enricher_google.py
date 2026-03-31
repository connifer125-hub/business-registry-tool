"""
utils/enricher_google.py
─────────────────────────
Google Places enrichment + pre-QA address corroboration.
Uses Places API (New) — Text Search endpoint.

Match score logic:
  0.90+  → auto_approve
  0.70+  → review
  0.50+  → flag (partial mismatch)
  found but low match → flag (address discrepancy — common with trade name filings)
  not found → flag (cannot corroborate)
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

GOOGLE_API_KEY   = os.getenv("GOOGLE_PLACES_API_KEY", "")
NEW_TEXTSEARCH   = "https://places.googleapis.com/v1/places:searchText"


def normalize_address(addr: str) -> str:
    if not addr:
        return ""
    addr = addr.upper().strip()
    for old, new in {
        " STREET": " ST", " AVENUE": " AVE", " BOULEVARD": " BLVD",
        " DRIVE": " DR", " ROAD": " RD", " LANE": " LN",
        " COURT": " CT", " PLACE": " PL", " SUITE": " STE",
        ".": "", ",": "", "#": ""
    }.items():
        addr = addr.replace(old, new)
    return " ".join(addr.split())


def address_match_score(registry_addr: str, google_addr: str) -> float:
    if not registry_addr or not google_addr:
        return 0.0
    return fuzz.token_sort_ratio(
        normalize_address(registry_addr),
        normalize_address(google_addr)
    ) / 100.0


def search_places_new(business_name: str, city: str, state: str) -> dict | None:
    """Use the Places API (New) Text Search endpoint."""
    if not GOOGLE_API_KEY:
        print("    No GOOGLE_PLACES_API_KEY set")
        return None

    query   = f"{business_name} {city} {state} USA"
    payload = json.dumps({"textQuery": query, "maxResultCount": 1}).encode("utf-8")

    req = urllib.request.Request(
        NEW_TEXTSEARCH,
        data    = payload,
        headers = {
            "Content-Type":     "application/json",
            "X-Goog-Api-Key":   GOOGLE_API_KEY,
            "X-Goog-FieldMask": "places.id,places.displayName,places.formattedAddress,places.nationalPhoneNumber,places.websiteUri,places.googleMapsUri"
        },
        method = "POST"
    )

    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data   = json.loads(resp.read())
            places = data.get("places", [])
            return places[0] if places else None
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        print(f"    Places API error {e.code}: {body[:200]}")
        return None
    except Exception as e:
        print(f"    Places search error: {e}")
        return None


def determine_qa_routing(match_score: float, found: bool) -> tuple:
    if not found:
        return "flag", "Business not found on Google Places — cannot corroborate"
    if match_score >= 0.90:
        return "auto_approve", f"Google address match {match_score:.0%} — high confidence"
    if match_score >= 0.70:
        return "review", f"Google address match {match_score:.0%} — likely match, review recommended"
    if match_score >= 0.50:
        return "flag", f"Google address match {match_score:.0%} — partial mismatch, needs review"
    return "flag", f"Google address match {match_score:.0%} — low match, may be registered agent address vs operating address"


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

    place = search_places_new(business_name, city or "", state or "")
    if not place:
        result["pre_qa_note"] = "Business not found on Google Places"
        return result

    google_address = place.get("formattedAddress", "")
    match_score    = address_match_score(registry_address, google_address)
    routing_status, routing_note = determine_qa_routing(match_score, True)

    result.update({
        "google_found":        True,
        "google_place_id":     place.get("id"),
        "google_address":      google_address,
        "google_phone":        place.get("nationalPhoneNumber"),
        "website_url":         place.get("websiteUri"),
        "google_maps_url":     place.get("googleMapsUri"),
        "address_match_score": round(match_score, 3),
        "address_match":       match_score >= 0.70,
        "pre_qa_status":       routing_status,
        "pre_qa_note":         routing_note,
    })

    if not dry_run and place.get("id"):
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
                    place.get("id"), google_address,
                    place.get("nationalPhoneNumber"),
                    place.get("websiteUri"),
                    place.get("googleMapsUri"),
                    round(match_score, 3),
                    match_score >= 0.70,
                    routing_status,
                    routing_note,
                    business_id
                ))

    return result


def enrich_batch(market: str = "US-DE", limit: int = 20, dry_run: bool = False) -> dict:
    enriched = errors = auto_approved = flagged = reviewed = 0

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

            icon = "✓" if result["address_match"] else "~" if result["google_found"] else "✗"
            site = (result.get("website_url") or "no website")[:45]
            print(f"  {icon} {business_name[:40]:<40} | {result['address_match_score']:.0%} | {result['pre_qa_status']:<12} | {site}")

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
    return {"enriched": enriched, "auto_approved": auto_approved,
            "reviewed": reviewed, "flagged": flagged, "errors": errors}


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--market",  default="US-DE")
    parser.add_argument("--limit",   type=int, default=20)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    enrich_batch(market=args.market, limit=args.limit, dry_run=args.dry_run)
