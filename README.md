# Business Registry Scraper & QA Admin Tool

A portfolio project demonstrating firmographic data collection from public business registries across the US, Canada, and EU — with a QA admin interface for review, deduplication, and export.

---

## What it does

- Scrapes business registration data (name, address, status, SIC/NAICS, officers, filing dates) from public registries
- Normalizes data into a unified firmographic schema
- Surfaces records in a Flask-based QA admin UI for review, flagging, approval, and export
- Exports clean data to CSV or SQL

---

## Market tiers (sorted by data accessibility)

| Tier | Markets | Access method |
|------|---------|---------------|
| **Tier 1** | Delaware, Wyoming, Florida, Colorado, New Mexico, Canada (ISED) | Bulk CSV download or open REST API |
| **Tier 2** | California, Texas, New York, most US states, BC/ON/AB provinces | HTML web portal scraping (BeautifulSoup/Playwright) |
| **Tier 3** | UK, Germany, France, Italy, Spain, Denmark | Requires registration, fee, or API key |

---

## Firmographic schema (normalized)

Every record — regardless of source — gets normalized to:

```
business_id, business_name, trade_name, entity_type, status,
state_province, country, registered_address, registered_date,
dissolution_date, sic_code, naics_code, registered_agent,
officer_names, source_market, source_url, raw_data, scraped_at,
qa_status, qa_notes, is_duplicate_of
```

---

## Project structure

```
business-registry-tool/
├── scrapers/
│   ├── tier1/
│   │   ├── us/          # Delaware, WY, FL, CO, NM
│   │   └── canada/      # ISED federal + provincial
│   ├── tier2/
│   │   ├── us/          # CA, TX, NY, and more
│   │   └── canada/      # BC, ON, AB
│   └── tier3/
│       └── eu/          # UK, DE, FR, IT
├── db/
│   ├── schema.sql       # Full PostgreSQL schema
│   └── db.py            # DB connection + helpers
├── admin/
│   ├── app.py           # Flask QA admin app
│   ├── templates/       # Jinja2 HTML templates
│   └── static/          # CSS + JS
├── utils/
│   ├── normalizer.py    # Maps any source → unified schema
│   └── deduplicator.py  # Fuzzy match for duplicate detection
├── exports/             # CSV/SQL exports land here
├── docs/
│   └── market_access_guide.md
├── requirements.txt
└── .env.example
```

---

## Setup

```bash
git clone https://github.com/YOUR_USERNAME/business-registry-tool
cd business-registry-tool
pip install -r requirements.txt
cp .env.example .env          # add your DB_URL
psql -U postgres -f db/schema.sql
python admin/app.py           # starts QA admin on localhost:5000
```

To run a scraper:
```bash
python scrapers/tier1/us/delaware.py
python scrapers/tier1/us/wyoming.py
python scrapers/tier2/us/california.py
```

---

## Tech stack

- **Python** — scraping (requests, BeautifulSoup, Playwright for JS-heavy portals)
- **PostgreSQL** — primary data store
- **Flask** — QA admin web interface
- **pandas** — normalization + export
- **rapidfuzz** — duplicate detection (fuzzy name matching)

---

## Portfolio notes

This project demonstrates:
- Multi-source public data ingestion and normalization
- Tiered architecture based on real-world data access constraints
- QA workflow tooling (the same pattern used in Best Rated Spots)
- Firmographic data modeling (SIC/NAICS codes, entity types, officer data)
- Experience with enterprise data concepts (deduplication, SLA-aware source tiering)
