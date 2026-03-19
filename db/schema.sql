-- ============================================================
-- Pharmacovigilance Schema — E2B(R3)-aligned
-- PostgreSQL
-- Run once against your pharmacovigilance database:
--   psql -U <user> -d pharmacovigilance -f schema.sql
-- ============================================================

-- Drop in reverse dependency order for clean re-runs
DROP TABLE IF EXISTS reactions   CASCADE;
DROP TABLE IF EXISTS drugs       CASCADE;
DROP TABLE IF EXISTS patients    CASCADE;
DROP TABLE IF EXISTS reports     CASCADE;
DROP TABLE IF EXISTS etl_runs    CASCADE;

-- ------------------------------------------------------------
-- etl_runs: audit log for every extract/load cycle
-- ------------------------------------------------------------
CREATE TABLE etl_runs (
    id              SERIAL PRIMARY KEY,
    drug_name       TEXT        NOT NULL,
    endpoint        TEXT        NOT NULL,          -- e.g. /drug/event
    rows_extracted  INTEGER     NOT NULL DEFAULT 0,
    rows_loaded     INTEGER     NOT NULL DEFAULT 0,
    rows_skipped    INTEGER     NOT NULL DEFAULT 0,
    status          TEXT        NOT NULL DEFAULT 'started', -- started | complete | error
    error_message   TEXT,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at    TIMESTAMPTZ
);

-- ------------------------------------------------------------
-- reports: one row per FAERS ICSR (E2B: N.1 / N.2 block)
-- ------------------------------------------------------------
CREATE TABLE reports (
    id                  SERIAL PRIMARY KEY,
    safety_report_id    TEXT        NOT NULL UNIQUE,  -- openFDA safetyreportid
    receipt_date        DATE,
    receive_date        DATE,
    serious             BOOLEAN,
    serious_death       BOOLEAN,
    serious_hospitalization BOOLEAN,
    serious_lifethreat  BOOLEAN,
    serious_disability  BOOLEAN,
    serious_congenital  BOOLEAN,
    serious_other       BOOLEAN,
    reporter_country    TEXT,
    reporter_qualification TEXT,     -- 1=physician, 2=pharmacist, 3=other HCP, 5=consumer
    outcome             TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    etl_run_id          INTEGER REFERENCES etl_runs(id)
);

CREATE INDEX idx_reports_safety_report_id ON reports(safety_report_id);
CREATE INDEX idx_reports_receipt_date     ON reports(receipt_date);
CREATE INDEX idx_reports_serious          ON reports(serious);

-- ------------------------------------------------------------
-- patients: demographics (E2B: D block)
-- ------------------------------------------------------------
CREATE TABLE patients (
    id              SERIAL PRIMARY KEY,
    report_id       INTEGER NOT NULL REFERENCES reports(id) ON DELETE CASCADE,
    age_years       NUMERIC(5,1),
    sex             TEXT,            -- 0=unknown, 1=male, 2=female
    weight_kg       NUMERIC(6,2),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_patients_report_id ON patients(report_id);

-- ------------------------------------------------------------
-- drugs: one row per drug per report (E2B: G block)
-- ------------------------------------------------------------
CREATE TABLE drugs (
    id                  SERIAL PRIMARY KEY,
    report_id           INTEGER NOT NULL REFERENCES reports(id) ON DELETE CASCADE,
    medicinal_product   TEXT,        -- brand or generic name as reported
    generic_name        TEXT,        -- normalized generic name
    manufacturer_name   TEXT,
    drug_role           TEXT,        -- 1=suspect, 2=concomitant, 3=interacting
    drug_role_label     TEXT,        -- human-readable: suspect | concomitant | interacting
    indication          TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_drugs_report_id         ON drugs(report_id);
CREATE INDEX idx_drugs_generic_name      ON drugs(generic_name);
CREATE INDEX idx_drugs_medicinal_product ON drugs(medicinal_product);

-- ------------------------------------------------------------
-- reactions: one row per MedDRA reaction per report (E2B: E block)
-- ------------------------------------------------------------
CREATE TABLE reactions (
    id              SERIAL PRIMARY KEY,
    report_id       INTEGER NOT NULL REFERENCES reports(id) ON DELETE CASCADE,
    reaction_term   TEXT,            -- MedDRA Preferred Term
    outcome         TEXT,            -- 1=recovered, 2=recovering, 3=not recovered,
                                     -- 4=fatal, 5=unknown, 6=recovered with sequelae
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_reactions_report_id   ON reactions(report_id);
CREATE INDEX idx_reactions_term        ON reactions(reaction_term);
