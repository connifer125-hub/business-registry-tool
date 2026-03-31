"""
admin/app.py — Flask QA Admin Interface
────────────────────────────────────────
Business Registry QA tool — review, flag, approve, reject, export.

Run: python admin/app.py
     → http://localhost:5000
"""

import csv
import io
import sys
from pathlib import Path
from flask import Flask, render_template, request, redirect, url_for, flash, Response, jsonify

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from db.db import (
    get_pending_records, update_qa_status, get_stats,
    get_conn
)
import psycopg2.extras

app = Flask(__name__)
app.secret_key = "change-this-in-production"

# ─────────────────────────────────────────────
# Dashboard
# ─────────────────────────────────────────────
@app.route("/")
def dashboard():
    stats = get_stats()
    return render_template("dashboard.html", stats=stats)


# ─────────────────────────────────────────────
# Record queue — paginated, filterable
# ─────────────────────────────────────────────
@app.route("/queue")
def queue():
    page         = int(request.args.get("page", 1))
    per_page     = int(request.args.get("per_page", 50))
    market       = request.args.get("market", "")
    status       = request.args.get("status", "pending")
    search       = request.args.get("search", "").strip()
    offset       = (page - 1) * per_page

    records, total = _fetch_records(
        limit=per_page, offset=offset,
        source_market=market or None,
        qa_status=status or None,
        search=search or None
    )
    total_pages = (total + per_page - 1) // per_page

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT source_market FROM businesses ORDER BY source_market")
            markets = [r[0] for r in cur.fetchall()]

    return render_template("queue.html",
        records=records, page=page, per_page=per_page,
        total=total, total_pages=total_pages,
        markets=markets, selected_market=market,
        selected_status=status, search=search
    )


def _fetch_records(limit, offset, source_market=None, qa_status="pending", search=None):
    conditions = []
    params = []

    if qa_status:
        conditions.append("qa_status = %s")
        params.append(qa_status)
    if source_market:
        conditions.append("source_market = %s")
        params.append(source_market)
    if search:
        conditions.append("business_name ILIKE %s")
        params.append(f"%{search}%")

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(f"SELECT COUNT(*) FROM businesses {where}", params)
            total = cur.fetchone()["count"]

            cur.execute(f"""
                SELECT business_id, business_name, trade_name, entity_type, status,
                       city, state_province, country, registered_date,
                       source_market, qa_status, qa_notes,
                       is_duplicate_of, duplicate_confidence, scraped_at
                FROM businesses {where}
                ORDER BY scraped_at DESC
                LIMIT %s OFFSET %s
            """, params + [limit, offset])
            return cur.fetchall(), total


# ─────────────────────────────────────────────
# Single record detail
# ─────────────────────────────────────────────
@app.route("/record/<int:business_id>")
def record_detail(business_id):
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM businesses WHERE business_id = %s", (business_id,))
            record = cur.fetchone()
    if not record:
        flash("Record not found.", "error")
        return redirect(url_for("queue"))
    return render_template("record_detail.html", record=record)


# ─────────────────────────────────────────────
# QA actions
# ─────────────────────────────────────────────
@app.route("/action", methods=["POST"])
def action():
    business_id = int(request.form["business_id"])
    action_type = request.form["action"]
    notes       = request.form.get("notes", "")

    if action_type not in ("approved", "rejected", "flagged"):
        flash("Invalid action.", "error")
        return redirect(url_for("queue"))

    update_qa_status(business_id, action_type, notes)
    flash(f"Record #{business_id} marked as {action_type}.", "success")
    return redirect(request.referrer or url_for("queue"))


@app.route("/bulk-action", methods=["POST"])
def bulk_action():
    ids         = request.form.getlist("ids")
    action_type = request.form["action"]

    if action_type not in ("approved", "rejected", "flagged"):
        flash("Invalid action.", "error")
        return redirect(url_for("queue"))

    for bid in ids:
        update_qa_status(int(bid), action_type, "Bulk action")

    flash(f"{len(ids)} records marked as {action_type}.", "success")
    return redirect(url_for("queue"))


# ─────────────────────────────────────────────
# Export
# ─────────────────────────────────────────────
@app.route("/export")
def export():
    market  = request.args.get("market", "")
    status  = request.args.get("status", "approved")
    fmt     = request.args.get("format", "csv")

    conditions = []
    params = []
    if status:
        conditions.append("qa_status = %s"); params.append(status)
    if market:
        conditions.append("source_market = %s"); params.append(market)
    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(f"""
                SELECT business_id, business_name, trade_name, entity_type, status,
                       registered_address, city, state_province, postal_code, country,
                       registered_date, dissolution_date, sic_code, naics_code,
                       industry_desc, registered_agent, officer_names,
                       source_market, source_url, qa_status, scraped_at
                FROM businesses {where}
                ORDER BY source_market, business_name
            """, params)
            rows = cur.fetchall()

    if fmt == "csv":
        output = io.StringIO()
        if rows:
            writer = csv.DictWriter(output, fieldnames=rows[0].keys())
            writer.writeheader()
            for row in rows:
                writer.writerow(dict(row))
        filename = f"businesses_{market or 'all'}_{status}.csv"
        return Response(
            output.getvalue(),
            mimetype="text/csv",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )

    return jsonify([dict(r) for r in rows])


# ─────────────────────────────────────────────
# Market access guide
# ─────────────────────────────────────────────
@app.route("/markets")
def markets():
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM market_registry ORDER BY tier, country, market_name")
            markets = cur.fetchall()
    return render_template("markets.html", markets=markets)


# ─────────────────────────────────────────────
# Scraper trigger routes
# ─────────────────────────────────────────────
@app.route("/run-scraper/delaware")
def run_delaware():
    import requests as req
    import subprocess
    limit = request.args.get("limit", "100")

    sample = req.get("https://data.delaware.gov/resource/i7m4-42sn.json?$limit=1").json()

    result = subprocess.run(
        [sys.executable, "scrapers/tier1/us/delaware.py", "--limit", limit, "--dry-run"],
        capture_output=True, text=True, cwd=str(Path(__file__).resolve().parents[1])
    )
    output = result.stdout + "\n" + result.stderr
    return f"<pre style='font-family:monospace;padding:20px;'>SAMPLE FIELDS:\n{sample}\n\nSCRAPER OUTPUT:\n{output}</pre>"


@app.route("/run-scraper/delaware/live")
def run_delaware_live():
    import subprocess
    limit = request.args.get("limit", "100")
    result = subprocess.run(
        [sys.executable, "scrapers/tier1/us/delaware.py", "--limit", limit],
        capture_output=True, text=True, cwd=str(Path(__file__).resolve().parents[1])
    )
    output = result.stdout + "\n" + result.stderr
    return f"<pre style='font-family:monospace;padding:20px;'>{output}</pre>"


@app.route("/run-scraper/colorado")
def run_colorado():
    import subprocess
    limit = request.args.get("limit", "100")
    result = subprocess.run(
        [sys.executable, "scrapers/tier1/us/colorado.py", "--limit", limit],
        capture_output=True, text=True, cwd=str(Path(__file__).resolve().parents[1])
    )
    output = result.stdout + "\n" + result.stderr
    return f"<pre style='font-family:monospace;padding:20px;'>{output}</pre>"


@app.route("/scrapers")
def scraper_index():
    return """
    <html><body style='font-family:sans-serif;padding:40px;'>
    <h2>Scraper triggers</h2>
    <ul>
      <li><a href='/run-scraper/delaware'>Inspect Delaware fields (dry run)</a></li>
      <li><a href='/run-scraper/delaware/live?limit=100'>Run Delaware live (100 records)</a></li>
      <li><a href='/run-scraper/delaware/live?limit=500'>Run Delaware live (500 records)</a></li>
      <li><a href='/run-scraper/colorado?limit=100'>Run Colorado (100 records)</a></li>
    </ul>
    <p><a href='/'>Back to admin</a></p>
    </body></html>
    """


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000, debug=True)
