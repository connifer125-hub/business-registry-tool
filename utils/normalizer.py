"""
utils/normalizer.py
Maps raw scraped records from any source to the unified firmographic schema.
"""

from datetime import datetime
from typing import Optional


ENTITY_TYPE_MAP = {
    "LLC": "LLC",
    "LIMITED LIABILITY COMPANY": "LLC",
    "L.L.C.": "LLC",
    "CORP": "Corporation",
    "CORPORATION": "Corporation",
    "INC": "Corporation",
    "INCORPORATED": "Corporation",
    "LP": "Limited Partnership",
    "LIMITED PARTNERSHIP": "Limited Partnership",
    "LLP": "Limited Liability Partnership",
    "SOLE PROP": "Sole Proprietorship",
    "NONPROFIT": "Nonprofit",
    "NON-PROFIT": "Nonprofit",
    "LTEE": "Corporation",
    "LTÉE": "Corporation",
    "INC.": "Corporation",
    "PRIVATE LIMITED COMPANY": "Private Ltd (UK)",
    "PUBLIC LIMITED COMPANY": "Public Ltd (UK)",
    "LTD": "Private Ltd (UK)",
    "PLC": "Public Ltd (UK)",
}

STATUS_MAP = {
    "GOOD STANDING": "Active",
    "ACTIVE": "Active",
    "IN GOOD STANDING": "Active",
    "DISSOLVED": "Dissolved",
    "CANCELLED": "Dissolved",
    "REVOKED": "Revoked",
    "SUSPENDED": "Suspended",
    "DELINQUENT": "Delinquent",
    "INACTIVE": "Inactive",
    "VOID": "Dissolved",
}


def normalize_entity_type(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    return ENTITY_TYPE_MAP.get(raw.strip().upper(), raw.strip().title())


def normalize_status(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    return STATUS_MAP.get(raw.strip().upper(), raw.strip().title())


def parse_date(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%d/%m/%Y", "%B %d, %Y",
                "%d %b %Y", "%Y%m%d", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(str(raw).strip()[:10], fmt[:len(str(raw).strip())]).strftime("%Y-%m-%d")
        except ValueError:
            continue
    # Try just first 8 chars for YYYYMMDD format
    try:
        s = str(raw).strip()
        if len(s) >= 8 and s[:8].isdigit():
            return datetime.strptime(s[:8], "%Y%m%d").strftime("%Y-%m-%d")
    except ValueError:
        pass
    return None


def build_full_address(street: str = None, street2: str = None,
                       city: str = None, state: str = None,
                       postal: str = None, country: str = None) -> str:
    """Combine address components into a single full address string."""
    parts = []
    if street:
        parts.append(street.strip())
    if street2:
        parts.append(street2.strip())
    city_state_zip = " ".join(filter(None, [
        city.strip() if city else None,
        state.strip() if state else None,
        postal.strip() if postal else None,
    ]))
    if city_state_zip:
        parts.append(city_state_zip)
    if country and country.strip().upper() not in ("US", "USA", "UNITED STATES"):
        parts.append(country.strip())
    return ", ".join(parts)


# ─────────────────────────────────────────────
# Delaware
# ─────────────────────────────────────────────
def normalize_delaware(raw: dict) -> dict:
    street   = raw.get("streetaddressline1__c") or raw.get("registered_address") or ""
    street2  = raw.get("streetaddressline2__c") or ""
    city     = raw.get("city__c") or raw.get("city") or ""
    state    = raw.get("state__c") or "DE"
    postal   = raw.get("zip__c") or raw.get("zip") or ""
    country  = raw.get("country__c") or "US"

    # owner/mailing address (separate from business address)
    owner_address = raw.get("address__c") or ""

    full_address = build_full_address(street, street2, city, state, postal, country)

    return {
        "business_name":      raw.get("businessname") or raw.get("trade_name__c") or raw.get("owner_company__c") or "",
        "trade_name":         raw.get("trade_name__c"),
        "entity_type":        normalize_entity_type(raw.get("entity_kind") or raw.get("business_nature__c")),
        "status":             normalize_status(raw.get("status")) or "Active",

        # Address — all components separate AND combined
        "registered_address": full_address,          # combined full address
        "address_line1":      street,                # street line 1
        "address_line2":      street2,               # street line 2
        "city":               city,
        "state_province":     state,
        "postal_code":        postal,
        "country":            country if country else "US",

        # Owner/mailing address (different from registered address)
        "owner_address":      owner_address,

        # Contact
        "phone":              raw.get("phone_number__c"),

        # People
        "registered_agent":   raw.get("affiant_name__c"),
        "officer_names":      list(filter(None, [
            " ".join(filter(None, [
                raw.get("firstname__c"),
                raw.get("lastname__c")
            ]))
        ])),

        # Dates
        "registered_date":    parse_date(raw.get("formation_date__c") or raw.get("incdate")),
        "dissolution_date":   parse_date(raw.get("termination_date__c") or raw.get("dissolution_date")),

        # Industry
        "sic_code":           raw.get("sic_code"),
        "naics_code":         None,
        "industry_desc":      raw.get("business_nature__c"),

        # IDs
        "filing_id":          raw.get("assignedfilingid__c") or raw.get("sessionid__c"),
        "license_number":     raw.get("associated_license_numbers"),

        # Source
        "source_market":      "US-DE",
        "source_url":         None,
        "raw_data":           raw,
    }


# ─────────────────────────────────────────────
# Wyoming
# ─────────────────────────────────────────────
def normalize_wyoming(raw: dict) -> dict:
    street  = raw.get("Address") or raw.get("address") or ""
    city    = raw.get("City") or raw.get("city") or ""
    state   = "WY"
    postal  = raw.get("Zip") or raw.get("zip") or ""

    return {
        "business_name":      raw.get("BusinessName") or raw.get("business_name") or "",
        "trade_name":         None,
        "entity_type":        normalize_entity_type(raw.get("EntityType") or raw.get("type")),
        "status":             normalize_status(raw.get("Status") or raw.get("status")),
        "registered_address": build_full_address(street, None, city, state, postal, "US"),
        "address_line1":      street,
        "address_line2":      None,
        "city":               city,
        "state_province":     state,
        "postal_code":        postal,
        "country":            "US",
        "owner_address":      None,
        "phone":              raw.get("Phone"),
        "registered_agent":   raw.get("RegisteredAgent"),
        "officer_names":      [],
        "registered_date":    parse_date(raw.get("FilingDate") or raw.get("filing_date")),
        "dissolution_date":   None,
        "sic_code":           None,
        "naics_code":         None,
        "industry_desc":      None,
        "filing_id":          raw.get("FilingID"),
        "license_number":     None,
        "source_market":      "US-WY",
        "source_url":         None,
        "raw_data":           raw,
    }


# ─────────────────────────────────────────────
# Florida
# ─────────────────────────────────────────────
def normalize_florida(raw: dict) -> dict:
    street  = raw.get("PrincipalAddress") or raw.get("Name") or ""
    city    = raw.get("PrincipalCity") or ""
    state   = "FL"
    postal  = raw.get("PrincipalZip") or ""

    return {
        "business_name":      raw.get("Name") or raw.get("business_name") or "",
        "trade_name":         raw.get("DBAName"),
        "entity_type":        normalize_entity_type(raw.get("EntityType")),
        "status":             normalize_status(raw.get("Status")),
        "registered_address": build_full_address(street, None, city, state, postal, "US"),
        "address_line1":      street,
        "address_line2":      None,
        "city":               city,
        "state_province":     state,
        "postal_code":        postal,
        "country":            "US",
        "owner_address":      None,
        "phone":              None,
        "registered_agent":   raw.get("RegisteredAgent"),
        "officer_names":      [],
        "registered_date":    parse_date(raw.get("FileDate")),
        "dissolution_date":   parse_date(raw.get("Dissolution_Date")),
        "sic_code":           None,
        "naics_code":         None,
        "industry_desc":      None,
        "filing_id":          None,
        "license_number":     None,
        "source_market":      "US-FL",
        "source_url":         "https://dos.fl.gov/sunbiz",
        "raw_data":           raw,
    }


# ─────────────────────────────────────────────
# California
# ─────────────────────────────────────────────
def normalize_california(raw: dict) -> dict:
    street  = raw.get("address") or ""
    city    = raw.get("city") or ""
    state   = "CA"
    postal  = raw.get("zip") or ""

    return {
        "business_name":      raw.get("businessName") or raw.get("name") or "",
        "trade_name":         None,
        "entity_type":        normalize_entity_type(raw.get("businessType")),
        "status":             normalize_status(raw.get("status")),
        "registered_address": build_full_address(street, None, city, state, postal, "US"),
        "address_line1":      street,
        "address_line2":      None,
        "city":               city,
        "state_province":     state,
        "postal_code":        postal,
        "country":            "US",
        "owner_address":      None,
        "phone":              None,
        "registered_agent":   raw.get("agent"),
        "officer_names":      [],
        "registered_date":    parse_date(raw.get("registrationDate")),
        "dissolution_date":   None,
        "sic_code":           raw.get("sic"),
        "naics_code":         None,
        "industry_desc":      None,
        "filing_id":          None,
        "license_number":     None,
        "source_market":      "US-CA",
        "source_url":         "https://bizfileonline.sos.ca.gov",
        "raw_data":           raw,
    }


# ─────────────────────────────────────────────
# Canada Federal
# ─────────────────────────────────────────────
def normalize_canada_federal(raw: dict) -> dict:
    street  = raw.get("registered_office_address") or ""
    city    = raw.get("city") or ""
    state   = raw.get("province") or ""
    postal  = raw.get("postal_code") or ""

    return {
        "business_name":      raw.get("corporation_name") or raw.get("name") or "",
        "trade_name":         None,
        "entity_type":        normalize_entity_type(raw.get("corporation_type")),
        "status":             normalize_status(raw.get("status")),
        "registered_address": build_full_address(street, None, city, state, postal, "CA"),
        "address_line1":      street,
        "address_line2":      None,
        "city":               city,
        "state_province":     state,
        "postal_code":        postal,
        "country":            "CA",
        "owner_address":      None,
        "phone":              None,
        "registered_agent":   None,
        "officer_names":      [],
        "registered_date":    parse_date(raw.get("date_incorporated")),
        "dissolution_date":   parse_date(raw.get("dissolution_date")),
        "sic_code":           None,
        "naics_code":         raw.get("naics_code"),
        "industry_desc":      raw.get("primary_activity"),
        "filing_id":          None,
        "license_number":     None,
        "source_market":      "CA-FED",
        "source_url":         "https://ised-isde.canada.ca",
        "raw_data":           raw,
    }


# ─────────────────────────────────────────────
# UK Companies House
# ─────────────────────────────────────────────
def normalize_uk(raw: dict) -> dict:
    address = raw.get("registered_office_address", {})
    street  = address.get("address_line_1") or ""
    street2 = address.get("address_line_2") or ""
    city    = address.get("locality") or ""
    state   = address.get("region") or ""
    postal  = address.get("postal_code") or ""
    officers = raw.get("officers", [])
    officer_names = [o.get("name", "") for o in officers if isinstance(o, dict)]

    return {
        "business_name":      raw.get("company_name") or "",
        "trade_name":         None,
        "entity_type":        normalize_entity_type(raw.get("type")),
        "status":             normalize_status(raw.get("company_status")),
        "registered_address": build_full_address(street, street2, city, state, postal, "UK"),
        "address_line1":      street,
        "address_line2":      street2,
        "city":               city,
        "state_province":     state,
        "postal_code":        postal,
        "country":            "UK",
        "owner_address":      None,
        "phone":              None,
        "registered_agent":   None,
        "officer_names":      officer_names,
        "registered_date":    raw.get("date_of_creation"),
        "dissolution_date":   raw.get("date_of_cessation"),
        "sic_code":           (raw.get("sic_codes") or [None])[0],
        "naics_code":         None,
        "industry_desc":      None,
        "filing_id":          raw.get("company_number"),
        "license_number":     None,
        "source_market":      "EU-UK",
        "source_url":         f"https://find-and-update.company-information.service.gov.uk/company/{raw.get('company_number','')}",
        "raw_data":           raw,
    }


# ─────────────────────────────────────────────
# Router
# ─────────────────────────────────────────────
NORMALIZER_MAP = {
    "US-DE":  normalize_delaware,
    "US-WY":  normalize_wyoming,
    "US-FL":  normalize_florida,
    "US-CA":  normalize_california,
    "CA-FED": normalize_canada_federal,
    "EU-UK":  normalize_uk,
}


def normalize(source_market: str, raw: dict) -> dict:
    fn = NORMALIZER_MAP.get(source_market)
    if not fn:
        raise ValueError(f"No normalizer registered for market: {source_market}")
    return fn(raw)
