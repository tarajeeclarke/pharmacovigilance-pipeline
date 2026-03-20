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
    {"brand": "adderall",     "generic": "amphetamine"},
    {"brand": "methylphenidate", "generic": "methylphenidate"},
    {"brand": "alprazolam",   "generic": "alprazolam"},
    {"brand": "escitalopram", "generic": "escitalopram"},
    {"brand": "olanzapine",   "generic": "olanzapine"},
    {"brand": "quetiapine",   "generic": "quetiapine"},
]

BASE_URL   = "https://api.fda.gov/drug/event.json"
PAGE_SIZE  = 100     # openFDA max per request
MAX_PAGES  = 10      # 10 pages × 100 = 1,000 reports per drug (adjust up for production)
RAW_DIR    = Path(__file__).parent.parent / "data" / "raw"


def _build_params(drug_name: str, generic: str, skip: int) -> dict:
    """Build openFDA query params using both brand and generic name fields."""
    params = {
        "search": f'patient.drug.openfda.brand_name:"{drug_name}"+patient.drug.openfda.generic_name:"{generic}"',
        "limit": PAGE_SIZE,
        "skip":  skip,
    }
    api_key = os.getenv("OPENFDA_API_KEY")
    if api_key:
        params["api_key"] = api_key
    return params


def _fetch_page(drug: dict, skip: int, retries: int = 3) -> list[dict]:
    """Fetch one page of results. Retries on 429 (rate limit)."""
    params = _build_params(drug["brand"], drug["generic"], skip)
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(BASE_URL, params=params, timeout=30)
            if resp.status_code == 200:
                return resp.json().get("results", [])
            elif resp.status_code == 404:
                log.warning(f"No results for {drug['brand']} at skip={skip}")
                return []
            elif resp.status_code == 429:
                wait = 2 ** attempt
                log.warning(f"Rate limited. Waiting {wait}s (attempt {attempt}/{retries})")
                time.sleep(wait)
            else:
                log.error(f"HTTP {resp.status_code} for {drug['brand']}: {resp.text[:200]}")
                return []
        except requests.RequestException as e:
            log.error(f"Request error for {drug['brand']}: {e}")
            if attempt == retries:
                return []
            time.sleep(2)
    return []


def extract_drug(drug: dict) -> list[dict]:
    brand = drug["brand"]
    all_results = []

    log.info(f"Extracting: {brand}")
    for page in range(MAX_PAGES):
        skip    = page * PAGE_SIZE
        results = _fetch_page(drug, skip)
        if not results:
            break
        all_results.extend(results)
        log.info(f"  {brand}: page {page + 1} — {len(results)} records (total: {len(all_results)})")
        time.sleep(0.25)

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
