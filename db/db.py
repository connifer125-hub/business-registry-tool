"""
db/db.py — Database connection and helper utilities
"""

import os
import json
import psycopg2
import psycopg2.extras
from contextlib import contextmanager
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://localhost/business_registry")


@contextmanager
def get_conn():
    conn = psycopg2.connect(DATABASE_URL)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def insert_business(record: dict) -> int | None:
    """
    Insert a normalized business record. Returns new business_id or None if skipped.
    record should match the unified schema from utils/normalizer.py
    """
    sql = """
        INSERT INTO businesses (
            business_name, trade_name, entity_type, status,
            registered_address, city, state_province, postal_code, country,
            registered_date, dissolution_date,
            sic_code, naics_code, industry_desc,
            registered_agent, officer_names,
            source_market, source_url, raw_data,
            qa_status
        ) VALUES (
            %(business_name)s, %(trade_name)s, %(entity_type)s, %(status)s,
            %(registered_address)s, %(city)s, %(state_province)s, %(postal_code)s, %(country)s,
            %(registered_date)s, %(dissolution_date)s,
            %(sic_code)s, %(naics_code)s, %(industry_desc)s,
            %(registered_agent)s, %(officer_names)s,
            %(source_market)s, %(source_url)s, %(raw_data)s,
            'pending'
        )
        RETURNING business_id;
    """
    record.setdefault("trade_name", None)
    record.setdefault("entity_type", None)
    record.setdefault("status", None)
    record.setdefault("registered_address", None)
    record.setdefault("city", None)
    record.setdefault("postal_code", None)
    record.setdefault("country", "US")
    record.setdefault("registered_date", None)
    record.setdefault("dissolution_date", None)
    record.setdefault("sic_code", None)
    record.setdefault("naics_code", None)
    record.setdefault("industry_desc", None)
    record.setdefault("registered_agent", None)
    record.setdefault("officer_names", [])
    record.setdefault("source_url", None)
    if isinstance(record.get("raw_data"), dict):
        record["raw_data"] = json.dumps(record["raw_data"])

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, record)
            row = cur.fetchone()
            return row[0] if row else None


def log_scrape_run(market: str, tier: int) -> int:
    """Start a scrape run log. Returns run_id."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO scrape_runs (market, tier) VALUES (%s, %s) RETURNING run_id",
                (market, tier)
            )
            return cur.fetchone()[0]


def complete_scrape_run(run_id: int, scraped: int, inserted: int, dupes: int, errors: int):
    """Mark a scrape run as complete with counts."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE scrape_runs
                SET completed_at = NOW(),
                    records_scraped = %s,
                    records_inserted = %s,
                    duplicates_found = %s,
                    errors = %s
                WHERE run_id = %s
            """, (scraped, inserted, dupes, errors, run_id))


def get_pending_records(limit=50, offset=0, source_market=None):
    """Fetch pending QA records for the admin UI."""
    where = "WHERE qa_status = 'pending'"
    params = []
    if source_market:
        where += " AND source_market = %s"
        params.append(source_market)
    params += [limit, offset]

    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(f"""
                SELECT business_id, business_name, trade_name, entity_type, status,
                       city, state_province, country, registered_date,
                       source_market, qa_status, qa_notes,
                       is_duplicate_of, duplicate_confidence, scraped_at
                FROM businesses
                {where}
                ORDER BY scraped_at DESC
                LIMIT %s OFFSET %s
            """, params)
            return cur.fetchall()


def update_qa_status(business_id: int, status: str, notes: str = None, reviewer: str = "admin"):
    """Update QA status on a record."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE businesses
                SET qa_status = %s, qa_notes = %s, reviewed_by = %s, reviewed_at = NOW()
                WHERE business_id = %s
            """, (status, notes, reviewer, business_id))


def mark_duplicate(business_id: int, duplicate_of: int, confidence: float):
    """Flag a record as a duplicate of another."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE businesses
                SET is_duplicate_of = %s, duplicate_confidence = %s, qa_status = 'flagged',
                    qa_notes = 'Auto-flagged as potential duplicate'
                WHERE business_id = %s
            """, (duplicate_of, confidence, business_id))


def get_stats():
    """Dashboard stats for admin UI."""
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT
                    COUNT(*) FILTER (WHERE qa_status = 'pending')   AS pending,
                    COUNT(*) FILTER (WHERE qa_status = 'approved')  AS approved,
                    COUNT(*) FILTER (WHERE qa_status = 'rejected')  AS rejected,
                    COUNT(*) FILTER (WHERE qa_status = 'flagged')   AS flagged,
                    COUNT(*)                                         AS total
                FROM businesses
            """)
            stats = dict(cur.fetchone())

            cur.execute("""
                SELECT source_market, COUNT(*) as count
                FROM businesses
                GROUP BY source_market
                ORDER BY count DESC
            """)
            stats["by_market"] = [dict(r) for r in cur.fetchall()]
            return stats
