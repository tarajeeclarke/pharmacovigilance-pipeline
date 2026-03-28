"""
etl/transform_load.py
---------------------
Transforms raw openFDA JSON into relational rows and loads them
into the PostgreSQL schema defined in db/schema.sql.
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

DRUG_ROLE_MAP = {"1": "suspect", "2": "concomitant", "3": "interacting"}
REPORTER_MAP  = {"1": "physician", "2": "pharmacist", "3": "other_hcp", "5": "consumer"}
REACTION_OUTCOME_MAP = {
    "1": "recovered", "2": "recovering", "3": "not_recovered",
    "4": "fatal", "5": "unknown", "6": "recovered_with_sequelae",
}
SEX_MAP = {"0": "unknown", "1": "male", "2": "female"}


def get_connection():
    database_url = os.getenv("DATABASE_URL")
    if database_url:
        if database_url.startswith("postgres://"):
            database_url = database_url.replace("postgres://", "postgresql://", 1)
        from urllib.parse import urlparse
        r = urlparse(database_url)
        return psycopg2.connect(
            host=r.hostname, port=r.port or 5432,
            dbname=r.path[1:], user=r.username, password=r.password,
            sslmode="require",
        )
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", 5432)),
        dbname=os.getenv("DB_NAME", "pharmacovigilance"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
    )


def _parse_date(raw):
    if not raw:
        return None
    raw = str(raw).strip()
    for fmt in ("%Y%m%d", "%Y-%m-%d"):
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            continue
    return None


def _safe_float(val):
    try:
        f = float(val)
        return f if f >= 0 else None
    except (TypeError, ValueError):
        return None


def _normalize_drug_name(name):
    if not name:
        return None
    return re.sub(r'\s+', ' ', name.strip().lower())


def _flag(val):
    if val is None:
        return None
    return str(val) == "1"


def transform_report(raw, drug_brand):
    safety_id = raw.get("safetyreportid")
    if not safety_id:
        return None

    report = {
        "safety_report_id":        str(safety_id),
        "receipt_date":            _parse_date(raw.get("receiptdate")),
        "receive_date":            _parse_date(raw.get("receivedate")),
        "serious":                 _flag(raw.get("serious")),
        "serious_death":           _flag(raw.get("seriousnessdeath")),
        "serious_hospitalization": _flag(raw.get("seriousnesshospitalization")),
        "serious_lifethreat":      _flag(raw.get("seriousnesslifethreatening")),
        "serious_disability":      _flag(raw.get("seriousnessdisabling")),
        "serious_congenital":      _flag(raw.get("seriousnesscongenitalanomali")),
        "serious_other":           _flag(raw.get("seriousnessother")),
        "reporter_country":        raw.get("occurcountry"),
        "reporter_qualification":  REPORTER_MAP.get(
            str(raw.get("primarysource", {}).get("qualification", "")), None),
        "outcome": raw.get("patient", {}).get("patientdeath", {}).get("patientdeathdate"),
    }

    pt = raw.get("patient", {})
    patient = {
        "age_years": _safe_float(pt.get("patientonsetage")),
        "sex":       SEX_MAP.get(str(pt.get("patientsex", "")), None),
        "weight_kg": _safe_float(pt.get("patientweight")),
    }

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

    raw_reactions = pt.get("reaction", [])
    if isinstance(raw_reactions, dict):
        raw_reactions = [raw_reactions]

    reactions = []
    for r in raw_reactions:
        reactions.append({
            "reaction_term": r.get("reactionmeddrapt"),
            "outcome":       REACTION_OUTCOME_MAP.get(str(r.get("reactionoutcome", "")), None),
        })

    return {"report": report, "patient": patient, "drugs": drugs, "reactions": reactions}


def load_report(cur, transformed, etl_run_id):
    report = transformed["report"]

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
        return False

    report_id = row[0]

    p = transformed["patient"]
    cur.execute("""
        INSERT INTO patients (report_id, age_years, sex, weight_kg)
        VALUES (%s, %s, %s, %s)
    """, (report_id, p["age_years"], p["sex"], p["weight_kg"]))

    if transformed["drugs"]:
        execute_values(cur, """
            INSERT INTO drugs
                (report_id, medicinal_product, generic_name, manufacturer_name,
                 drug_role, drug_role_label, indication)
            VALUES %s
        """, [(
            report_id, d["medicinal_product"], d["generic_name"],
            d["manufacturer_name"], d["drug_role"], d["drug_role_label"], d["indication"],
        ) for d in transformed["drugs"]])

    if transformed["reactions"]:
        execute_values(cur, """
            INSERT INTO reactions (report_id, reaction_term, outcome)
            VALUES %s
        """, [(report_id, r["reaction_term"], r["outcome"])
              for r in transformed["reactions"]])

    return True


def transform_load_drug(raw_records, drug_brand):
    stats = {"extracted": len(raw_records), "loaded": 0, "skipped": 0, "errors": 0}

    log_conn = get_connection()
    log_conn.autocommit = True
    with log_conn.cursor() as log_cur:
        log_cur.execute("""
            INSERT INTO etl_runs (drug_name, endpoint, rows_extracted, status)
            VALUES (%s, %s, %s, 'started')
            RETURNING id
        """, (drug_brand, "/drug/event", len(raw_records)))
        etl_run_id = log_cur.fetchone()[0]

    conn = get_connection()
    seen_ids = set()

    try:
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

                with conn:
                    with conn.cursor() as cur:
                        inserted = load_report(cur, transformed, etl_run_id)
                        if inserted:
                            stats["loaded"] += 1
                        else:
                            stats["skipped"] += 1

            except Exception as e:
                conn.rollback()
                log.warning("Error on report: %s", e)
                stats["errors"] += 1
    finally:
        conn.close()

    with log_conn.cursor() as log_cur:
        log_cur.execute("""
            UPDATE etl_runs
            SET rows_loaded = %s, rows_skipped = %s,
                status = 'complete', completed_at = NOW()
            WHERE id = %s
        """, (stats["loaded"], stats["skipped"], etl_run_id))
    log_conn.close()

    log.info("%(d)s: loaded=%(l)s skipped=%(s)s errors=%(e)s", {
        "d": drug_brand, "l": stats["loaded"],
        "s": stats["skipped"], "e": stats["errors"]
    })
    return stats


def run_full_etl(extracted_data):
    total = {"loaded": 0, "skipped": 0, "errors": 0}
    for brand, records in extracted_data.items():
        stats = transform_load_drug(records, brand)
        for k in ("loaded", "skipped", "errors"):
            total[k] += stats[k]
    log.info("ETL complete - loaded: %s skipped: %s errors: %s",
             total["loaded"], total["skipped"], total["errors"])
    return total


if __name__ == "__main__":
    from extract import extract_all
    data = extract_all()
    run_full_etl(data)