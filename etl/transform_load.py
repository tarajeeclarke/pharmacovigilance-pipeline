"""
etl/transform_load.py
---------------------
Transforms raw openFDA JSON into relational rows and loads them
into the PostgreSQL schema defined in db/schema.sql.

Transform steps:
  1. Flatten nested JSON (patient → drugs → reactions)
  2. Deduplicate on safetyreportid
  3. Normalize drug names (lowercase, strip whitespace)
  4. Standardize dates to ISO format
  5. Validate age (flag negatives / implausible values)
  6. Map coded fields to human-readable labels

Load steps:
  - Upsert reports (skip duplicates already in DB)
  - Insert patients, drugs, reactions with FK references
  - Log each run to etl_runs table
"""

import os
import re
import logging
from datetime import datetime, date
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ------------------------------------------------------------------
# Lookup tables for coded fields
# ------------------------------------------------------------------
DRUG_ROLE_MAP = {
    "1": "suspect",
    "2": "concomitant",
    "3": "interacting",
}

REPORTER_MAP = {
    "1": "physician",
    "2": "pharmacist",
    "3": "other_hcp",
    "5": "consumer",
}

REACTION_OUTCOME_MAP = {
    "1": "recovered",
    "2": "recovering",
    "3": "not_recovered",
    "4": "fatal",
    "5": "unknown",
    "6": "recovered_with_sequelae",
}

SEX_MAP = {
    "0": "unknown",
    "1": "male",
    "2": "female",
}

# ------------------------------------------------------------------
# Database connection
# ------------------------------------------------------------------
def get_connection():
    return psycopg2.connect(
        host     = os.getenv("DB_HOST", "localhost"),
        port     = int(os.getenv("DB_PORT", 5432)),
        dbname   = os.getenv("DB_NAME", "pharmacovigilance"),
        user     = os.getenv("DB_USER"),
        password = os.getenv("DB_PASSWORD"),
    )


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------
def _parse_date(raw: str | None) -> date | None:
    """Parse openFDA date strings (YYYYMMDD or YYYY-MM-DD) to date."""
    if not raw:
        return None
    raw = str(raw).strip()
    for fmt in ("%Y%m%d", "%Y-%m-%d"):
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            continue
    return None


def _safe_float(val) -> float | None:
    try:
        f = float(val)
        return f if f >= 0 else None   # reject negatives
    except (TypeError, ValueError):
        return None


def _normalize_drug_name(name: str | None) -> str | None:
    if not name:
        return None
    return re.sub(r'\s+', ' ', name.strip().lower())


def _flag(val) -> bool | None:
    """Convert openFDA 1/2 flags to True/False (1=yes, 2=no)."""
    if val is None:
        return None
    return str(val) == "1"


# ------------------------------------------------------------------
# Transform: flatten one raw report dict → structured rows
# ------------------------------------------------------------------
def transform_report(raw: dict, drug_brand: str) -> dict | None:
    """
    Returns a dict with keys: report, patient, drugs, reactions
    or None if the report should be skipped (e.g. missing ID).
    """
    safety_id = raw.get("safetyreportid")
    if not safety_id:
        return None

    # --- Report-level fields ---
    report = {
        "safety_report_id":       str(safety_id),
        "receipt_date":           _parse_date(raw.get("receiptdate")),
        "receive_date":           _parse_date(raw.get("receivedate")),
        "serious":                _flag(raw.get("serious")),
        "serious_death":          _flag(raw.get("seriousnessdeath")),
        "serious_hospitalization":_flag(raw.get("seriousnesshospitalization")),
        "serious_lifethreat":     _flag(raw.get("seriousnesslifethreatening")),
        "serious_disability":     _flag(raw.get("seriousnessdisabling")),
        "serious_congenital":     _flag(raw.get("seriousnesscongenitalanomali")),
        "serious_other":          _flag(raw.get("seriousnessother")),
        "reporter_country":       raw.get("occurcountry"),
        "reporter_qualification": REPORTER_MAP.get(
                                      str(raw.get("primarysource", {}).get("qualification", "")),
                                      None),
        "outcome":                raw.get("patient", {}).get("patientdeath", {}).get("patientdeathdate"),
    }

    # --- Patient demographics ---
    pt = raw.get("patient", {})
    patient = {
        "age_years": _safe_float(pt.get("patientonsetage")),
        "sex":       SEX_MAP.get(str(pt.get("patientsex", "")), None),
        "weight_kg": _safe_float(pt.get("patientweight")),
    }

    # --- Drugs ---
    raw_drugs = pt.get("drug", [])
    if isinstance(raw_drugs, dict):
        raw_drugs = [raw_drugs]

    drugs = []
    for d in raw_drugs:
        role_code = str(d.get("drugcharacterization", ""))
        drugs.append({
            "medicinal_product": d.get("medicinalproduct"),
            "generic_name":      _normalize_drug_name(
                                     d.get("openfda", {}).get("generic_name", [None])[0]
                                     or d.get("medicinalproduct")),
            "manufacturer_name": d.get("openfda", {}).get("manufacturer_name", [None])[0],
            "drug_role":         role_code,
            "drug_role_label":   DRUG_ROLE_MAP.get(role_code, "unknown"),
            "indication":        d.get("drugindication"),
        })

    # --- Reactions ---
    raw_reactions = pt.get("reaction", [])
    if isinstance(raw_reactions, dict):
        raw_reactions = [raw_reactions]

    reactions = []
    for r in raw_reactions:
        reactions.append({
            "reaction_term": r.get("reactionmeddrapt"),
            "outcome":       REACTION_OUTCOME_MAP.get(str(r.get("reactionoutcome", "")), None),
        })

    return {
        "report":    report,
        "patient":   patient,
        "drugs":     drugs,
        "reactions": reactions,
    }


# ------------------------------------------------------------------
# Load: insert one transformed report into Postgres
# ------------------------------------------------------------------
def load_report(cur, transformed: dict, etl_run_id: int) -> bool:
    """
    Insert report + patient + drugs + reactions.
    Returns True if inserted, False if duplicate (skipped).
    """
    report = transformed["report"]

    # Upsert report — skip if safetyreportid already exists
    cur.execute("""
        INSERT INTO reports (
            safety_report_id, receipt_date, receive_date,
            serious, serious_death, serious_hospitalization,
            serious_lifethreat, serious_disability, serious_congenital, serious_other,
            reporter_country, reporter_qualification, outcome, etl_run_id
        ) VALUES (
            %(safety_report_id)s, %(receipt_date)s, %(receive_date)s,
            %(serious)s, %(serious_death)s, %(serious_hospitalization)s,
            %(serious_lifethreat)s, %(serious_disability)s, %(serious_congenital)s, %(serious_other)s,
            %(reporter_country)s, %(reporter_qualification)s, %(outcome)s, %(etl_run_id)s
        )
        ON CONFLICT (safety_report_id) DO NOTHING
        RETURNING id
    """, {**report, "etl_run_id": etl_run_id})

    row = cur.fetchone()
    if not row:
        return False   # duplicate — skipped

    report_id = row[0]

    # Patient
    p = transformed["patient"]
    cur.execute("""
        INSERT INTO patients (report_id, age_years, sex, weight_kg)
        VALUES (%s, %s, %s, %s)
    """, (report_id, p["age_years"], p["sex"], p["weight_kg"]))

    # Drugs
    if transformed["drugs"]:
        execute_values(cur, """
            INSERT INTO drugs
                (report_id, medicinal_product, generic_name, manufacturer_name,
                 drug_role, drug_role_label, indication)
            VALUES %s
        """, [(
            report_id,
            d["medicinal_product"],
            d["generic_name"],
            d["manufacturer_name"],
            d["drug_role"],
            d["drug_role_label"],
            d["indication"],
        ) for d in transformed["drugs"]])

    # Reactions
    if transformed["reactions"]:
        execute_values(cur, """
            INSERT INTO reactions (report_id, reaction_term, outcome)
            VALUES %s
        """, [(report_id, r["reaction_term"], r["outcome"])
              for r in transformed["reactions"]])

    return True


# ------------------------------------------------------------------
# Orchestrate: transform + load all records for one drug
# ------------------------------------------------------------------
def transform_load_drug(raw_records: list[dict], drug_brand: str) -> dict:
    """
    Full ETL for one drug.
    Returns stats dict: {extracted, loaded, skipped, errors}
    """
    conn = get_connection()
    stats = {"extracted": len(raw_records), "loaded": 0, "skipped": 0, "errors": 0}

    try:
        with conn:
            with conn.cursor() as cur:
                # Open ETL run log entry
                cur.execute("""
                    INSERT INTO etl_runs (drug_name, endpoint, rows_extracted, status)
                    VALUES (%s, %s, %s, 'started')
                    RETURNING id
                """, (drug_brand, "/drug/event", len(raw_records)))
                etl_run_id = cur.fetchone()[0]

                # Process each report
                seen_ids = set()
                for raw in raw_records:
                    try:
                        transformed = transform_report(raw, drug_brand)
                        if not transformed:
                            stats["skipped"] += 1
                            continue

                        sid = transformed["report"]["safety_report_id"]
                        if sid in seen_ids:
                            stats["skipped"] += 1
                            continue
                        seen_ids.add(sid)

                        inserted = load_report(cur, transformed, etl_run_id)
                        if inserted:
                            stats["loaded"] += 1
                        else:
                            stats["skipped"] += 1

                    except Exception as e:
                        log.warning(f"Error on report: {e}")
                        stats["errors"] += 1

                # Close ETL run log entry
                cur.execute("""
                    UPDATE etl_runs
                    SET rows_loaded   = %s,
                        rows_skipped  = %s,
                        status        = 'complete',
                        completed_at  = NOW()
                    WHERE id = %s
                """, (stats["loaded"], stats["skipped"], etl_run_id))

    except Exception as e:
        log.error(f"Fatal ETL error for {drug_brand}: {e}")
        stats["errors"] += 1
    finally:
        conn.close()

    log.info(f"{drug_brand}: loaded={stats['loaded']} skipped={stats['skipped']} errors={stats['errors']}")
    return stats


# ------------------------------------------------------------------
# Entry point
# ------------------------------------------------------------------
def run_full_etl(extracted_data: dict[str, list[dict]]):
    """Run transform+load for all drugs. Pass output of extract.extract_all()."""
    total = {"loaded": 0, "skipped": 0, "errors": 0}
    for brand, records in extracted_data.items():
        stats = transform_load_drug(records, brand)
        for k in ("loaded", "skipped", "errors"):
            total[k] += stats[k]
    log.info(f"ETL complete — total loaded: {total['loaded']} | skipped: {total['skipped']} | errors: {total['errors']}")
    return total


if __name__ == "__main__":
    from extract import extract_all
    data = extract_all()
    run_full_etl(data)
