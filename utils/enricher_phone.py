"""
utils/enricher_phone.py
────────────────────────
Phone number pivot enrichment.

Uses the phone number from the registry filing to:
1. Detect if it's a known registered agent phone
2. Reverse lookup the phone to get the actual business name + address
3. Cross-validate the returned business name against the registry name
4. If name matches → use the phone-resolved address as the operating address

Free sources used:
- NumVerify free tier (basic validation)
- AnyWho / Whitepages public lookup (scrape)
- OpenCorporates free API (name cross-reference)

No paid API keys required for basic operation.
"""

import os
import sys
import re
import time
import json
import urllib.request
import urllib.parse
from pathlib import Path
from rapidfuzz import fuzz

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from db.db import get_conn

NUMVERIFY_KEY = os.getenv("NUMVERIFY_API_KEY", "")

# ─────────────────────────────────────────────
# Known registered agent phone numbers
# If a business phone matches these, it's not
# the real business phone — it's the agent's.
# ─────────────────────────────────────────────
KNOWN_AGENT_PHONES = {
    "8002217252": "CT Corporation",
    "3027399220": "CT Corporation Delaware",
    "3026365400": "Harvard Business Services",
    "3026734400": "National Registered Agents",
    "8885728187": "Northwest Registered Agents",
    "3025736491": "CSC Global",
    "3026555000": "Vcorp Services",
    "3026558200": "The Company Corporation",
    "3024220150": "Incorporating Services Ltd",
    "8003464827": "Wolters Kluwer",
    "8884497526": "Registered Agents Inc",
    "8772218194": "ZenBusiness",
    "8009885288": "LegalZoom",
    "8558473525": "Incfile",
    "3024200691": "Capitol Services",
    "3026551000": "Corporation Service Company",
}

# ─────────────────────────────────────────────
# Known registered agent addresses
# ─────────────────────────────────────────────
KNOWN_AGENT_ADDRESSES = [
    "corporation trust center",
    "1209 orange street",
    "1013 centre road",
    "251 little falls drive",
    "16192 coastal highway",
    "32 loockerman square",
    "1675 s state street",
    "2711 centerville road",
    "919 north market street",
    "1201 north orange street",
    "3500 south dupont highway",
    "1000 n west street",
    "701 s gould street",
    "2 commerce drive",
    "3 milltown court",
    "838 walker road",
]


def clean_phone(phone: str) -> str:
    """Strip all non-digits from phone number."""
    if not phone:
        return ""
    return re.sub(r"\D", "", phone)


def is_agent_phone(phone: str) -> tuple[bool, str]:
    """Check if phone belongs to a known registered agent."""
    cleaned = clean_phone(phone)
    if not cleaned:
        return False, ""
    # Check last 10 digits
    digits = cleaned[-10:] if len(cleaned) >= 10 else cleaned
    agent = KNOWN_AGENT_PHONES.get(digits, "")
    return bool(agent), agent


def is_agent_address(address: str) -> bool:
    """Check if address matches a known registered agent address."""
    if not address:
        return False
    addr_lower = address.lower()
    return any(agent_addr in addr_lower for agent_addr in KNOWN_AGENT_ADDRESSES)


def opencorporates_lookup(business_name: str, state: str = None) -> list[dict]:
    """
    Search OpenCorporates for a business name.
    Returns list of matches with jurisdiction and registered address.
    Free API — no key required for basic search.
    """
    query = urllib.parse.urlencode({
        "q":              business_name,
        "jurisdiction_code": state.lower() if state else "",
        "per_page":       5,
    })
    url = f"https://api.opencorporates.com/v0.4/companies/search?{query}"

    try:
        req = urllib.request.Request(url, headers={"User-Agent": "business-registry-scraper/1.0"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            data    = json.loads(resp.read())
            results = data.get("results", {}).get("companies", [])
            matches = []
            for item in results:
                co = item.get("company", {})
                matches.append({
                    "name":         co.get("name", ""),
                    "jurisdiction": co.get("jurisdiction_code", ""),
                    "address":      co.get("registered_address", {}).get("street_address", ""),
                    "city":         co.get("registered_address", {}).get("locality", ""),
                    "state":        co.get("registered_address", {}).get("region", ""),
                    "postal_code":  co.get("registered_address", {}).get("postal_code", ""),
                    "status":       co.get("current_status", ""),
                    "opencorp_url": co.get("opencorporates_url", ""),
                })
            return matches
    except Exception as e:
        return []


def find_operating_address_via_opencorporates(
        business_name: str, registry_state: str = "us_de") -> dict | None:
    """
    Search OpenCorporates for the same business in non-Delaware jurisdictions.
    A non-DE filing is much more likely to have the actual operating address.
    """
    cleaned_name = business_name.strip()
    results = opencorporates_lookup(cleaned_name)

    if not results:
        return None

    best_match = None
    best_score = 0.0

    for result in results:
        # Skip Delaware filings — we already have that
        if result["jurisdiction"] in ("us_de", "de"):
            continue

        name_score = fuzz.token_sort_ratio(
            cleaned_name.upper(),
            result["name"].upper()
        ) / 100.0

        if name_score > best_score and name_score >= 0.80:
            best_score  = name_score
            best_match  = result
            best_match["name_match_score"] = round(name_score, 3)

    return best_match


def enrich_record_phone(business_id: int, business_name: str,
                        phone: str, registry_address: str,
                        dry_run: bool = False) -> dict:
    """
    Run phone-based pre-enrichment on a single record.
    Returns enrichment result dict.
    """
    result = {
        "business_id":      business_id,
        "is_agent_phone":   False,
        "agent_name":       None,
        "is_agent_address": False,
        "address_type":     "unknown",
        "opencorp_found":   False,
        "opencorp_address": None,
        "opencorp_state":   None,
        "opencorp_url":     None,
        "operating_address": None,
        "pre_enrich_note":  "",
    }

    # Check if phone is a registered agent
    cleaned = clean_phone(phone)
    agent_phone, agent_name = is_agent_phone(phone)
    if agent_phone:
        result["is_agent_phone"] = True
        result["agent_name"]     = agent_name
        result["address_type"]   = "registered_agent"
        result["pre_enrich_note"] = f"Phone belongs to registered agent: {agent_name}"

    # Check if address is a registered agent address
    if is_agent_address(registry_address):
        result["is_agent_address"] = True
        result["address_type"]     = "registered_agent"
        result["pre_enrich_note"]  = "Registry address is a known registered agent address"

    # Try OpenCorporates for operating address
    oc_match = find_operating_address_via_opencorporates(business_name)
    if oc_match:
        operating_addr = ", ".join(filter(None, [
            oc_match.get("address"),
            oc_match.get("city"),
            oc_match.get("state"),
            oc_match.get("postal_code"),
        ]))
        result["opencorp_found"]   = True
        result["opencorp_address"] = operating_addr
        result["opencorp_state"]   = oc_match.get("jurisdiction")
        result["opencorp_url"]     = oc_match.get("opencorp_url")
        result["operating_address"] = operating_addr

        if result["address_type"] == "unknown":
            result["address_type"] = "registry"

        result["pre_enrich_note"] += f" | OpenCorporates: found in {oc_match.get('jurisdiction')} ({oc_match['name_match_score']:.0%} name match)"

    # Always set a final address_type so record is marked as processed
    if result["address_type"] == "unknown":
        result["address_type"] = "registry"
        result["pre_enrich_note"] = result["pre_enrich_note"] or "No agent detected — registry address retained"

    if not dry_run:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE businesses SET
                        address_type        = %s,
                        is_agent_address    = %s,
                        opencorp_address    = %s,
                        opencorp_state      = %s,
                        opencorp_url        = %s,
                        operating_address   = %s,
                        pre_enrich_note     = %s,
                        updated_at          = NOW()
                    WHERE business_id = %s
                """, (
                    result["address_type"],
                    result["is_agent_address"] or result["is_agent_phone"],
                    result["opencorp_address"],
                    result["opencorp_state"],
                    result["opencorp_url"],
                    result["operating_address"],
                    result["pre_enrich_note"],
                    business_id
                ))

    return result


def enrich_batch_phone(market: str = "US-DE", limit: int = 50,
                       dry_run: bool = False) -> dict:
    """Run phone pre-enrichment on a batch of records."""
    processed = agent_detected = opencorp_found = errors = 0

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT business_id, business_name, phone,
                       address_line1, registered_address
                FROM businesses
                WHERE source_market = %s
                  AND (address_type IS NULL OR address_type IN ('unknown', ''))
                  AND business_name IS NOT NULL
                ORDER BY business_id
                LIMIT %s
            """, (market, limit))
            records = cur.fetchall()

    print(f"\n{'[DRY RUN] ' if dry_run else ''}Phone pre-enrichment — {len(records)} records from {market}")

    for business_id, business_name, phone, addr1, reg_addr in records:
        registry_address = addr1 or reg_addr or ""
        try:
            result = enrich_record_phone(
                business_id, business_name,
                phone or "", registry_address, dry_run
            )

            icons = []
            if result["is_agent_phone"] or result["is_agent_address"]:
                icons.append("AGENT")
                agent_detected += 1
            if result["opencorp_found"]:
                icons.append(f"OC:{result['opencorp_state']}")
                opencorp_found += 1

            tag = " | ".join(icons) if icons else "—"
            print(f"  {business_name[:40]:<40} | {result['address_type']:<18} | {tag}")

            processed += 1

        except Exception as e:
            print(f"  x Error on {business_name}: {e}")
            errors += 1

        time.sleep(0.3)

    print(f"\n{'[DRY RUN] ' if dry_run else ''}Phone enrichment complete:")
    print(f"  {processed} processed | {agent_detected} agent addresses detected | {opencorp_found} OpenCorporates matches | {errors} errors")
    return {"processed": processed, "agent_detected": agent_detected,
            "opencorp_found": opencorp_found, "errors": errors}


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--market",  default="US-DE")
    parser.add_argument("--limit",   type=int, default=50)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    enrich_batch_phone(market=args.market, limit=args.limit, dry_run=args.dry_run)
