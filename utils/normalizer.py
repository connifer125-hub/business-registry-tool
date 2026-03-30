"""
utils/normalizer.py
Maps raw scraped records from any source to the unified firmographic schema.
Each market source has its own mapping function.
"""

from datetime import datetime
from typing import Optional


ENTITY_TYPE_MAP = {
    # US common
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
    # Canada
    "LTEE": "Corporation",
    "LTÉE": "Corporation",
    "INC.": "Corporation",
    # UK
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
    """Try multiple date formats. Returns ISO date string or None."""
    if not raw:
        return None
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%d/%m/%Y", "%B %d, %Y", "%d %b %Y", "%Y%m%d"):
        try:
            return datetime.strptime(str(raw).strip(), fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


# ─────────────────────────────────────────────
# Per-source normalization functions
# ─────────────────────────────────────────────

def normalize_delaware(raw: dict) -> dict:
    """
    Normalize a Delaware Socrata API record.
    Source fields vary — common ones listed here.
    """
    return {
        "business_name":      raw.get("businessname") or raw.get("file_name") or "",
        "trade_name":         raw.get("trade_name") or raw.get("dba_name"),
        "entity_type":        normalize_entity_type(raw.get("entity_kind") or raw.get("entitytype")),
        "status":             normalize_status(raw.get("status")),
        "registered_address": raw.get("registered_address"),
        "city":               raw.get("city"),
        "state_province":     "DE",
        "postal_code":        raw.get("zip"),
        "country":            "US",
        "registered_date":    parse_date(raw.get("incdate") or raw.get("incorporation_date")),
        "dissolution_date":   parse_date(raw.get("dissolution_date")),
        "registered_agent":   raw.get("registered_agent"),
        "officer_names":      [],
        "sic_code":           None,
        "naics_code":         None,
        "industry_desc":      None,
        "source_market":      "US-DE",
        "source_url":         "https://icis.corp.delaware.gov",
        "raw_data":           raw,
    }


def normalize_wyoming(raw: dict) -> dict:
    return {
        "business_name":      raw.get("BusinessName") or raw.get("business_name") or "",
        "trade_name":         None,
        "entity_type":        normalize_entity_type(raw.get("EntityType") or raw.get("type")),
        "status":             normalize_status(raw.get("Status") or raw.get("status")),
        "registered_address": raw.get("Address"),
        "city":               raw.get("City"),
        "state_province":     "WY",
        "postal_code":        raw.get("Zip"),
        "country":            "US",
        "registered_date":    parse_date(raw.get("FilingDate") or raw.get("filing_date")),
        "dissolution_date":   None,
        "registered_agent":   raw.get("RegisteredAgent"),
        "officer_names":      [],
        "sic_code":           None,
        "naics_code":         None,
        "industry_desc":      None,
        "source_market":      "US-WY",
        "source_url":         "https://wyobiz.wyo.gov",
        "raw_data":           raw,
    }


def normalize_florida(raw: dict) -> dict:
    return {
        "business_name":      raw.get("Name") or raw.get("business_name") or "",
        "trade_name":         raw.get("DBAName"),
        "entity_type":        normalize_entity_type(raw.get("EntityType")),
        "status":             normalize_status(raw.get("Status")),
        "registered_address": raw.get("PrincipalAddress"),
        "city":               raw.get("PrincipalCity"),
        "state_province":     "FL",
        "postal_code":        raw.get("PrincipalZip"),
        "country":            "US",
        "registered_date":    parse_date(raw.get("FileDate")),
        "dissolution_date":   parse_date(raw.get("Dissolution_Date")),
        "registered_agent":   raw.get("RegisteredAgent"),
        "officer_names":      [],
        "sic_code":           None,
        "naics_code":         None,
        "industry_desc":      None,
        "source_market":      "US-FL",
        "source_url":         "https://dos.fl.gov/sunbiz",
        "raw_data":           raw,
    }


def normalize_california(raw: dict) -> dict:
    return {
        "business_name":      raw.get("businessName") or raw.get("name") or "",
        "trade_name":         None,
        "entity_type":        normalize_entity_type(raw.get("businessType")),
        "status":             normalize_status(raw.get("status")),
        "registered_address": raw.get("address"),
        "city":               raw.get("city"),
        "state_province":     "CA",
        "postal_code":        raw.get("zip"),
        "country":            "US",
        "registered_date":    parse_date(raw.get("registrationDate")),
        "dissolution_date":   None,
        "registered_agent":   raw.get("agent"),
        "officer_names":      [],
        "sic_code":           raw.get("sic"),
        "naics_code":         None,
        "industry_desc":      None,
        "source_market":      "US-CA",
        "source_url":         "https://bizfileonline.sos.ca.gov",
        "raw_data":           raw,
    }


def normalize_canada_federal(raw: dict) -> dict:
    return {
        "business_name":      raw.get("corporation_name") or raw.get("name") or "",
        "trade_name":         None,
        "entity_type":        normalize_entity_type(raw.get("corporation_type")),
        "status":             normalize_status(raw.get("status")),
        "registered_address": raw.get("registered_office_address"),
        "city":               raw.get("city"),
        "state_province":     raw.get("province"),
        "postal_code":        raw.get("postal_code"),
        "country":            "CA",
        "registered_date":    parse_date(raw.get("date_incorporated") or raw.get("incorporation_date")),
        "dissolution_date":   parse_date(raw.get("dissolution_date")),
        "registered_agent":   None,
        "officer_names":      [],
        "sic_code":           None,
        "naics_code":         raw.get("naics_code"),
        "industry_desc":      raw.get("primary_activity"),
        "source_market":      "CA-FED",
        "source_url":         "https://ised-isde.canada.ca",
        "raw_data":           raw,
    }


def normalize_uk(raw: dict) -> dict:
    """UK Companies House API response normalization."""
    address = raw.get("registered_office_address", {})
    officers = raw.get("officers", [])
    officer_names = [o.get("name", "") for o in officers if isinstance(o, dict)]

    return {
        "business_name":      raw.get("company_name") or "",
        "trade_name":         None,
        "entity_type":        normalize_entity_type(raw.get("type")),
        "status":             normalize_status(raw.get("company_status")),
        "registered_address": ", ".join(filter(None, [
            address.get("address_line_1"),
            address.get("address_line_2")
        ])),
        "city":               address.get("locality"),
        "state_province":     address.get("region"),
        "postal_code":        address.get("postal_code"),
        "country":            "UK",
        "registered_date":    raw.get("date_of_creation"),
        "dissolution_date":   raw.get("date_of_cessation"),
        "registered_agent":   None,
        "officer_names":      officer_names,
        "sic_code":           (raw.get("sic_codes") or [None])[0],
        "naics_code":         None,
        "industry_desc":      None,
        "source_market":      "EU-UK",
        "source_url":         f"https://find-and-update.company-information.service.gov.uk/company/{raw.get('company_number','')}",
        "raw_data":           raw,
    }


# ─────────────────────────────────────────────
# Router — call normalize(source_market, raw)
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
