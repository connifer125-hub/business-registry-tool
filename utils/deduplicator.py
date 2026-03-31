"""
utils/deduplicator.py
─────────────────────
Fuzzy duplicate detection + change tracking for business records.

Logic:
- Same source market + same business name = refresh candidate
- On refresh: diff the record, log any changes, update in place
- New business from different source = genuine duplicate check
- Tracks: field changes, status changes, dissolution, reactivation
"""

from rapidfuzz import fuzz
import psycopg2.extras
import sys
from pathlib import Path
from datetime import datetime, timezone

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from db.db import get_conn

SIMILARITY_THRESHOLD = 0.90

# Fields we track changes on
TRACKED_FIELDS = [
    "business_name", "trade_name", "entity_type", "status",
    "address_line1", "address_line2", "city", "state_province",
    "postal_code", "phone", "registered_agent", "sic_code",
    "industry_desc", "dissolution_date"
]


def clean_name(name: str) -> str:
    if not name:
        return ""
    name = name.upper().strip()
    for suffix in [" LLC", " INC", " CORP", " LTD", " LP", " LLP", " CO", ".", ","]:
        name = name.replace(suffix, "")
    return name.strip()


def log_change(business_id: int, change_type: str, field_name: str = None,
               old_value: str = None, new_value: str = None,
               source_market: str = None, notes: str = None):
    """Write a change record to the change_log table."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO change_log
                    (business_id, change_type, field_name, old_value, new_value, source_market, notes)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (business_id, change_type, field_name,
                  str(old_value) if old_value else None,
                  str(new_value) if new_value else None,
                  source_market, notes))


def find_existing_same_source(record: dict) -> dict | None:
    """
    Find an existing record from the same source market with the same name.
    This is a refresh candidate — not a cross-source duplicate.
    """
    name   = clean_name(record.get("business_name", ""))
    market = record.get("source_market", "")
    state  = record.get("state_province", "")

    if not name or not market:
        return None

    first_word = name.split()[0] if name.split() else name

    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT *
                FROM businesses
                WHERE source_market = %s
                  AND state_province = %s
                  AND business_name ILIKE %s
                  AND qa_status != 'rejected'
                ORDER BY scraped_at DESC
                LIMIT 20
            """, (market, state, f"%{first_word}%"))
            candidates = cur.fetchall()

    for candidate in candidates:
        score = fuzz.token_sort_ratio(
            name, clean_name(candidate["business_name"])
        ) / 100.0
        if score >= SIMILARITY_THRESHOLD:
            return dict(candidate)

    return None


def refresh_existing(existing: dict, new_record: dict) -> dict:
    """
    Compare new scraped record against existing DB record.
    Log any field changes. Update the record in place.
    Returns summary of what changed.
    """
    business_id = existing["business_id"]
    changes     = []
    updates     = {}

    for field in TRACKED_FIELDS:
        old_val = existing.get(field)
        new_val = new_record.get(field)

        # Normalize for comparison
        old_str = str(old_val).strip().upper() if old_val else ""
        new_str = str(new_val).strip().upper() if new_val else ""

        if old_str != new_str and new_str:
            changes.append(field)
            updates[field] = new_val
            log_change(
                business_id  = business_id,
                change_type  = "status_change" if field == "status" else "field_change",
                field_name   = field,
                old_value    = old_val,
                new_value    = new_val,
                source_market = new_record.get("source_market"),
                notes        = f"Updated on rescrape"
            )

    # Update the record if anything changed
    if updates:
        updates["last_verified_at"] = datetime.now(timezone.utc)
        set_clause = ", ".join(f"{k} = %({k})s" for k in updates)
        updates["business_id"] = business_id
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"UPDATE businesses SET {set_clause}, times_seen = times_seen + 1, updated_at = NOW() WHERE business_id = %(business_id)s",
                    updates
                )
    else:
        # No changes — just update last_verified and times_seen
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE businesses
                    SET last_verified_at = NOW(), times_seen = times_seen + 1
                    WHERE business_id = %s
                """, (business_id,))

    return {"business_id": business_id, "changed_fields": changes}


def find_duplicate(record: dict, threshold: float = SIMILARITY_THRESHOLD) -> tuple:
    """
    Cross-source duplicate check only.
    Same-source matches are handled by find_existing_same_source + refresh_existing.
    Returns (matching_business_id, confidence) or (None, 0.0).
    """
    name   = clean_name(record.get("business_name", ""))
    state  = record.get("state_province", "")
    market = record.get("source_market", "")

    if not name:
        return None, 0.0

    first_word = name.split()[0] if name.split() else name

    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT business_id, business_name, city, state_province, source_market
                FROM businesses
                WHERE state_province = %s
                  AND source_market != %s
                  AND business_name ILIKE %s
                  AND qa_status != 'rejected'
                LIMIT 100
            """, (state, market, f"%{first_word}%"))
            candidates = cur.fetchall()

    best_id    = None
    best_score = 0.0

    for candidate in candidates:
        candidate_name = clean_name(candidate["business_name"])
        score = fuzz.token_sort_ratio(name, candidate_name) / 100.0

        if score >= (threshold - 0.05):
            if record.get("city") and candidate.get("city"):
                if record["city"].upper() == candidate["city"].upper():
                    score = min(score + 0.03, 1.0)

        if score >= threshold and score > best_score:
            best_score = score
            best_id    = candidate["business_id"]

    return best_id, best_score


def batch_deduplicate(source_market: str, threshold: float = SIMILARITY_THRESHOLD) -> int:
    from db.db import mark_duplicate

    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT business_id, business_name, city, state_province, source_market
                FROM businesses
                WHERE source_market = %s AND qa_status = 'pending'
                ORDER BY business_id
            """, (source_market,))
            records = cur.fetchall()

    found = 0
    for record in records:
        dup_id, confidence = find_duplicate(dict(record), threshold)
        if dup_id and dup_id != record["business_id"]:
            mark_duplicate(record["business_id"], dup_id, confidence)
            found += 1

    print(f"Deduplication for {source_market}: {found} cross-source duplicates found out of {len(records)} records")
    return found
