"""
etl/extract.py
--------------
Pulls adverse event reports from the openFDA /drug/event endpoint
for each of the six target mental-health medications.

Handles:
  - Pagination (limit/skip)
  - Optional API key via .env
  - Rate-limit backoff (429 responses)
  - Per-drug raw JSON saved to data/raw/ for reproducibility
"""

import os
import time
import json
import logging
import requests
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ------------------------------------------------------------------
# Target drugs — brand name used for API query, generic stored too
# ------------------------------------------------------------------
DRUGS = [
    {"brand": "adderall",    "generic": "amphetamine"},
    {"brand": "ritalin",     "generic": "methylphenidate"},
    {"brand": "xanax",       "generic": "alprazolam"},
    {"brand": "lexapro",     "generic": "escitalopram"},
    {"brand": "olanzapine",  "generic": "olanzapine"},
    {"brand": "seroquel",    "generic": "quetiapine"},
]

BASE_URL   = "https://api.fda.gov/drug/event.json"
PAGE_SIZE  = 100     # openFDA max per request
MAX_PAGES  = 10      # 10 pages × 100 = 1,000 reports per drug (adjust up for production)
RAW_DIR    = Path(__file__).parent.parent / "data" / "raw"


def _build_params(drug_name: str, skip: int) -> dict:
    """Build openFDA query params for a drug name."""
    params = {
        "search": f'patient.drug.medicinalproduct:"{drug_name}"+AND+patient.drug.drugcharacterization:"1"',
        "limit": PAGE_SIZE,
        "skip":  skip,
    }
    api_key = os.getenv("OPENFDA_API_KEY")
    if api_key:
        params["api_key"] = api_key
    return params


def _fetch_page(drug_name: str, skip: int, retries: int = 3) -> list[dict]:
    """Fetch one page of results. Retries on 429 (rate limit)."""
    params = _build_params(drug_name, skip)
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(BASE_URL, params=params, timeout=30)
            if resp.status_code == 200:
                return resp.json().get("results", [])
            elif resp.status_code == 404:
                log.warning(f"No results for {drug_name} at skip={skip}")
                return []
            elif resp.status_code == 429:
                wait = 2 ** attempt
                log.warning(f"Rate limited. Waiting {wait}s (attempt {attempt}/{retries})")
                time.sleep(wait)
            else:
                log.error(f"HTTP {resp.status_code} for {drug_name}: {resp.text[:200]}")
                return []
        except requests.RequestException as e:
            log.error(f"Request error for {drug_name}: {e}")
            if attempt == retries:
                return []
            time.sleep(2)
    return []


def extract_drug(drug: dict) -> list[dict]:
    """
    Extract all pages for one drug.
    Returns a flat list of raw FAERS report dicts.
    """
    brand   = drug["brand"]
    all_results = []

    log.info(f"Extracting: {brand}")
    for page in range(MAX_PAGES):
        skip    = page * PAGE_SIZE
        results = _fetch_page(brand, skip)
        if not results:
            break
        all_results.extend(results)
        log.info(f"  {brand}: page {page + 1} — {len(results)} records (total so far: {len(all_results)})")
        time.sleep(0.25)   # polite delay between pages

    # Save raw JSON for reproducibility / debugging
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    out_path = RAW_DIR / f"{brand}_raw.json"
    with open(out_path, "w") as f:
        json.dump(all_results, f)
    log.info(f"  Saved {len(all_results)} raw records → {out_path}")

    return all_results


def extract_all() -> dict[str, list[dict]]:
    """
    Extract all six drugs.
    Returns {brand_name: [raw_report, ...]}
    """
    results = {}
    for drug in DRUGS:
        results[drug["brand"]] = extract_drug(drug)
    return results


if __name__ == "__main__":
    data = extract_all()
    for brand, records in data.items():
        print(f"{brand}: {len(records)} records extracted")
