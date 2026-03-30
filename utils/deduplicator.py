"""
utils/deduplicator.py
─────────────────────
Fuzzy duplicate detection for business records.
Uses rapidfuzz for name similarity + address/state matching as tiebreaker.

Threshold: 0.90 = likely duplicate, 0.95+ = near-certain
"""

from rapidfuzz import fuzz
import psycopg2.extras
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from db.db import get_conn

SIMILARITY_THRESHOLD = 0.90


def clean_name(name: str) -> str:
    """Normalize business name for comparison."""
    if not name:
        return ""
    name = name.upper().strip()
    for suffix in [" LLC", " INC", " CORP", " LTD", " LP", " LLP", " CO", ".", ","]:
        name = name.replace(suffix, "")
    return name.strip()


def find_duplicate(record: dict, threshold: float = SIMILARITY_THRESHOLD) -> tuple[int | None, float]:
    """
    Check if a normalized record already exists in the DB.
    Returns (matching_business_id, confidence) or (None, 0.0).
    
    Strategy:
    1. Pull existing records from same state/province with similar first word
    2. Fuzzy match on cleaned business name
    3. Use state + city as tiebreaker
    """
    name = clean_name(record.get("business_name", ""))
    state = record.get("state_province", "")

    if not name:
        return None, 0.0

    # Use first word of business name to narrow candidates
    first_word = name.split()[0] if name.split() else name

    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT business_id, business_name, city, state_province
                FROM businesses
                WHERE state_province = %s
                  AND business_name ILIKE %s
                  AND qa_status != 'rejected'
                LIMIT 100
            """, (state, f"%{first_word}%"))
            candidates = cur.fetchall()

    best_id = None
    best_score = 0.0

    for candidate in candidates:
        candidate_name = clean_name(candidate["business_name"])
        score = fuzz.token_sort_ratio(name, candidate_name) / 100.0

        # Boost score slightly if same city
        if score >= (threshold - 0.05):
            if record.get("city") and candidate.get("city"):
                if record["city"].upper() == candidate["city"].upper():
                    score = min(score + 0.03, 1.0)

        if score >= threshold and score > best_score:
            best_score = score
            best_id = candidate["business_id"]

    return best_id, best_score


def batch_deduplicate(source_market: str, threshold: float = SIMILARITY_THRESHOLD) -> int:
    """
    Run deduplication across all pending records from a given market.
    Marks duplicates in-place. Returns count of duplicates found.
    """
    from db.db import mark_duplicate

    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT business_id, business_name, city, state_province
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

    print(f"Deduplication complete for {source_market}: {found} duplicates flagged out of {len(records)} records")
    return found
