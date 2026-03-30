# GitHub Setup & Railway Deployment Guide

---

## 1. Create the GitHub repo

```bash
# In your terminal, from inside the project folder:
cd business-registry-tool

git init
git add .
git commit -m "Initial commit — business registry scraper + QA admin"

# On GitHub.com: create a new repo called "business-registry-tool" (no README)
# Then push:
git remote add origin https://github.com/YOUR_USERNAME/business-registry-tool.git
git branch -M main
git push -u origin main
```

---

## 2. Set up the database locally (for development)

```bash
# Create the database
createdb business_registry

# Run the schema
psql -d business_registry -f db/schema.sql

# Verify tables were created
psql -d business_registry -c "\dt"
```

---

## 3. Configure environment

```bash
cp .env.example .env
```

Edit `.env`:
```
DATABASE_URL=postgresql://localhost/business_registry
FLASK_SECRET_KEY=pick-something-random-here
```

For Tier 3 scrapers, also add:
```
COMPANIES_HOUSE_API_KEY=your_key_here   # free from developer.company-information.service.gov.uk
```

---

## 4. Install dependencies

```bash
pip install -r requirements.txt

# If using California scraper (Tier 2, Playwright):
playwright install chromium
```

---

## 5. Test with a dry run

```bash
# Preview Delaware scraper without writing to DB
python scrapers/tier1/us/delaware.py --limit 50 --dry-run

# Run all Tier 1 scrapers, 1000 records each, dry run
python run_all.py --tier 1 --limit 1000 --dry-run
```

---

## 6. Run the QA Admin

```bash
python admin/app.py
# → http://localhost:5000
```

---

## 7. Deploy to Railway

### Option A: Admin UI on Railway (recommended)

Railway is ideal for the Flask admin app — same setup as LAdog Dispatch.

1. Push your repo to GitHub (step 1 above)
2. Go to [railway.app](https://railway.app) → New Project → Deploy from GitHub repo
3. Select `business-registry-tool`
4. Railway will auto-detect Python and run `admin/app.py`

Add environment variables in Railway dashboard:
```
DATABASE_URL    = (Railway PostgreSQL connection string)
FLASK_SECRET_KEY = your-secret
PORT            = 5000
```

Add a `Procfile` at root (Railway uses this):
```
web: python admin/app.py
```

Or add a `railway.json`:
```json
{
  "build": { "builder": "nixpacks" },
  "deploy": { "startCommand": "python admin/app.py" }
}
```

### Option B: Run scrapers as Railway cron jobs

In Railway, you can add a separate service for each scraper as a cron:
- Service: `delaware-scraper`
- Command: `python run_all.py --market US-DE --limit 10000`
- Cron: `0 2 * * 0` (runs every Sunday at 2am)

---

## 8. Recommended next steps after initial deploy

1. Run Delaware + Colorado scrapers (Tier 1, no setup needed)
2. Download Canada Federal bulk CSV and run `federal.py`
3. Download UK Companies House bulk CSV and run `uk.py --mode bulk`
4. Open the admin UI, review records, test approve/reject/export
5. Tweak deduplication threshold in `utils/deduplicator.py` if needed
6. Add more Tier 2 scrapers (CA, TX, NY) once Tier 1 is stable
