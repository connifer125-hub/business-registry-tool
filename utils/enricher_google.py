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


import re

def extract_domain(url: str) -> str:
    """Pull bare domain from a URL for comparison."""
    if not url:
        return ""
    url = url.lower().strip()
    for prefix in ["https://", "http://", "www."]:
        if url.startswith(prefix):
            url = url[len(prefix):]
    return url.split("/")[0].split("?")[0]


def extract_dotcom_from_name(business_name: str) -> str | None:
    """
    If the business name itself contains a .com domain, return it.
    'HELOC.com' → 'heloc.com'
    'Carrie's Auto' → None
    """
    match = re.search(r'[\w\-]+\.com', business_name, re.IGNORECASE)
    return match.group(0).lower() if match else None


def validate_website_url(raw_url: str, business_name: str, city: str, state: str) -> str | None:
    """
    Decide whether to accept the URL Google returned.

    Case 1 — business name contains .com (e.g. 'HELOC.com'):
      Only accept if the returned domain exactly matches the .com in the name.
      'heloc.com' for 'HELOC.com' → accept.
      'quickenloans.com' for 'HELOC.com' → reject.

    Case 2 — business name does NOT contain .com (e.g. "Carrie's Auto"):
      Accept whatever Google returns — the location-based search already
      did the filtering. 'autobycarrie.com' is fine for "Carrie's Auto Boca Raton".
      Only reject if the domain is completely generic (google.com, facebook.com,
      yelp.com etc.) which would mean Google didn't find a real website.
    """
    if not raw_url:
        return None

    domain = extract_domain(raw_url)

    # Always reject generic platforms — these are not the business's own site
    GENERIC_DOMAINS = {
        "google.com", "maps.google.com", "facebook.com", "yelp.com",
        "yellowpages.com", "bbb.org", "linkedin.com", "instagram.com",
        "twitter.com", "x.com", "tripadvisor.com", "foursquare.com",
        "mapquest.com", "apple.com", "bing.com"
    }
    if any(domain == g or domain.endswith("."+g) for g in GENERIC_DOMAINS):
        return None

    # Case 1: business name has .com in it — strict domain match required
    expected_domain = extract_dotcom_from_name(business_name)
    if expected_domain:
        if domain == expected_domain or domain == "www." + expected_domain:
            return raw_url
        else:
            return None  # Google returned wrong site for a .com-named business

    # Case 2: normal business name — accept what Google returned
    # The location search context already did the filtering
    return raw_url


def result_name_matches_query(result_name: str, business_name: str) -> bool:
    """
    Check that the Google Places result name is plausibly the business
    we searched for. Loose match — location context handled precision.
    """
    if not result_name or not business_name:
        return False
    score = fuzz.token_sort_ratio(
        business_name.lower().strip(),
        result_name.lower().strip()
    )
    # If the business name contains .com, be stricter — 70% threshold
    # For normal names, 50% is fine — location search is doing the work
    expected_domain = extract_dotcom_from_name(business_name)
    threshold = 70 if expected_domain else 50
    return score >= threshold
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

    # Verify the result name actually matches what we searched for
    result_name = place.get("displayName", {}).get("text", "") if isinstance(place.get("displayName"), dict) else place.get("displayName", "")
    if not result_name_matches_query(result_name, business_name):
        result["pre_qa_note"] = f"Google result '{result_name}' does not match '{business_name}' — rejected to avoid false enrichment"
        return result

    google_address = place.get("formattedAddress", "")
    match_score    = address_match_score(registry_address, google_address)
    routing_status, routing_note = determine_qa_routing(match_score, True)

    # Validate and accept or reject the website URL
    raw_url     = place.get("websiteUri")
    website_url = validate_website_url(raw_url, business_name, city or "", state or "")
    if raw_url and not website_url:
        routing_note += f" | Website '{extract_domain(raw_url)}' rejected — not the business's own site"

    result.update({
        "google_found":        True,
        "google_place_id":     place.get("id"),
        "google_address":      google_address,
        "google_phone":        place.get("nationalPhoneNumber"),
        "website_url":         website_url,
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
                    website_url,
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
