"""
topup_etl.py
------------
Targeted top-up extraction for drugs with low record counts.
Uses alternate search terms to find more matching reports.
"""
from etl.extract import extract_drug, DRUGS
from etl.transform_load import transform_load_drug

TOPUP_DRUGS = [
    {"brand": "lexapro",      "generic": "escitalopram oxalate"},
    {"brand": "celexa",       "generic": "escitalopram"},
    {"brand": "zyprexa",      "generic": "olanzapine"},
    {"brand": "klonopin",     "generic": "alprazolam"},
    {"brand": "quetiapine",   "generic": "quetiapine fumarate"},
]

if __name__ == "__main__":
    for drug in TOPUP_DRUGS:
        print(f"\nTop-up: {drug['brand']} / {drug['generic']}")
        records = extract_drug(drug)
        stats = transform_load_drug(records, drug["brand"])
        print(f"  loaded={stats['loaded']} skipped={stats['skipped']} errors={stats['errors']}")