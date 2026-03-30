# Market Access Guide

A reference for each supported registry — access method, data quality, and gotchas.

---

## Tier 1 — Open APIs and bulk CSV (Start here)

### 🇺🇸 Delaware (`US-DE`)
- **URL:** https://data.delaware.gov/resource/ahdy-uc97
- **Access:** Socrata open data API — no key, no rate limit for reasonable use
- **Coverage:** All domestic and foreign entities registered in Delaware
- **Key fields:** Business name, entity kind, status, filing date, registered agent
- **Gotchas:** Addresses are often the registered agent's address (e.g. CT Corp), not the real business address. Delaware is used by ~65% of Fortune 500s, so you'll see massive volume.
- **Scraper:** `scrapers/tier1/us/delaware.py`

### 🇺🇸 Wyoming (`US-WY`)
- **URL:** https://wyobiz.wyo.gov
- **Access:** Data portal with CSV exports; search API available
- **Coverage:** Good. WY is popular for LLCs (low cost, no state income tax)
- **Scraper:** `scrapers/tier1/us/wyoming.py` *(stub — implement similar to Delaware)*

### 🇺🇸 Florida (`US-FL`)
- **URL:** https://dos.fl.gov/sunbiz
- **Access:** Sunbiz search API is open. Bulk data purchase available ($50 for full DB)
- **Coverage:** Excellent — FL is a high-volume business registration state
- **Key fields:** Filing date, status, registered agent, principal address, DBA
- **Scraper:** `scrapers/tier1/us/florida.py` *(stub)*

### 🇺🇸 Colorado (`US-CO`)
- **URL:** https://data.colorado.gov/resource/4ykn-tg5h
- **Access:** Socrata open data — same pattern as Delaware, no key required
- **Scraper:** `scrapers/tier1/us/colorado.py` *(nearly identical to delaware.py)*

### 🇨🇦 Canada Federal — ISED (`CA-FED`)
- **URL:** https://ised-isde.canada.ca/site/corporations-canada/en/open-data
- **Access:** Free CSV bulk download — updated monthly
- **Coverage:** Federal corporations only (excludes provincially-registered businesses)
- **Key fields:** Corporation name, type, status, province, NAICS code, incorporation date
- **Gotchas:** French/English bilingual names; NAICS codes are well-populated (useful for industry filtering)
- **Scraper:** `scrapers/tier1/canada/federal.py` *(stub)*

---

## Tier 2 — HTML portal scraping

### 🇺🇸 California (`US-CA`)
- **URL:** https://bizfileonline.sos.ca.gov
- **Access:** Search by name or ID. No bulk download. Playwright needed (JS-heavy portal)
- **Coverage:** All CA entities — very high volume (millions of records)
- **Gotchas:** Rate limit aggressively. Portal has CAPTCHA on heavy use. Recommend 1-2 req/sec max.
- **Scraper:** `scrapers/tier2/us/california.py` *(uses Playwright)*

### 🇺🇸 Texas (`US-TX`)
- **URL:** https://www.sos.state.tx.us/corp/sosda/
- **Access:** SOSDirect — free search, JS portal
- **Gotchas:** Requires accepting terms on first visit. Playwright handles this.

### 🇺🇸 New York (`US-NY`)
- **URL:** https://apps.dos.ny.gov/publicInquiry/
- **Access:** Search portal. BeautifulSoup works for basic pages; Playwright for advanced
- **Key fields:** DOS ID, filing date, county, agent, status

### 🇨🇦 British Columbia (`CA-BC`)
- **URL:** https://www.bcregistry.gov.bc.ca
- **Access:** BC Registries API exists but requires OAuth. Web portal is scrapeable.

---

## Tier 3 — Registration or fee required

### 🇬🇧 United Kingdom (`EU-UK`)
- **URL:** https://find-and-update.company-information.service.gov.uk
- **Access:** Companies House API — **free API key required** (register at developer.company-information.service.gov.uk)
- **Bulk download:** Monthly CSV at http://download.companieshouse.gov.uk/en_output.html — **no key needed for bulk**
- **Coverage:** All UK registered companies (~5 million)
- **Key fields:** Company number, name, type, status, SIC codes, registered address, filing dates, officers
- **Recommendation:** Use the bulk download monthly rather than the API for initial load. Use the API only for individual record enrichment.

### 🇫🇷 France (`EU-FR`)
- **URL:** https://data.inpi.fr
- **Access:** Free INPI API key — register at inpi.fr
- **Coverage:** BODACC + RCS data. Good coverage of French SARLs, SAS, SA.

### 🇳🇴 Norway (`EU-NO`)
- **URL:** https://data.brreg.no/enhetsregisteret/api/enheter
- **Access:** Open REST API — **no key required** — best Tier 3 option
- **Coverage:** All Norwegian registered entities
- **Scraper:** Can be implemented similarly to Tier 1 (just a REST API call)

### 🇩🇰 Denmark (`EU-DK`)
- **URL:** https://datacvr.virk.dk
- **Access:** CVR REST API — Danish business registration required (NemID)

### 🇩🇪 Germany (`EU-DE`)
- **URL:** https://www.unternehmensregister.de
- **Access:** Requires German registration. Limited free access. Recommend skipping for portfolio purposes.

---

## Recommended build order

1. `US-DE` — Delaware (Tier 1, fully working scraper included)
2. `US-CO` — Colorado (copy Delaware scraper, change endpoint)
3. `CA-FED` — Canada Federal (bulk CSV download, simple parse)
4. `EU-UK` — UK bulk download (no key needed for monthly CSV)
5. `EU-NO` — Norway (open REST API, no key)
6. `US-CA` — California (Tier 2, Playwright)
7. `US-TX` / `US-NY` — Texas / New York (Tier 2)
