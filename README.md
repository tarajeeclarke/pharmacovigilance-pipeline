# Pharmacovigilance Data Engineering Pipeline

A full-stack data engineering project that ingests FDA adverse event reports via the openFDA API, stores them in a PostgreSQL database aligned to the ICH E2B(R3) pharmacovigilance standard, and surfaces safety signals through an interactive Flask + Plotly dashboard.

**Built as a graduate portfolio project — Hofstra University, MS Health Informatics**

---

## What this project does

Tracks adverse events for six mental-health medications using real public data:

| Drug | Generic |
|------|---------|
| Adderall | amphetamine |
| Ritalin | methylphenidate |
| Xanax | alprazolam |
| Lexapro | escitalopram |
| Olanzapine | olanzapine |
| Seroquel | quetiapine |

The pipeline pulls Individual Case Safety Reports (ICSRs) from the FDA Adverse Event Reporting System (FAERS), cleans and deduplicates them, loads them into a normalized PostgreSQL schema, and renders interactive safety analytics.

---

## Architecture

```
openFDA API (/drug/event)
        ↓
  etl/extract.py          — paginated REST ingestion, raw JSON saved to data/raw/
        ↓
  etl/transform_load.py   — flatten JSON, dedupe, normalize, load to PostgreSQL
        ↓
  PostgreSQL              — E2B(R3)-aligned schema (reports, patients, drugs, reactions, etl_runs)
        ↓
  app/app.py              — Flask backend, SQLAlchemy queries, Plotly charts
        ↓
  Browser dashboard       — KPI cards, heatmaps, time series, co-medication graph
```

---

## Tech stack

- **Python 3.10+** — ETL pipeline
- **PostgreSQL + psycopg2** — relational storage
- **Flask** — web framework
- **Plotly** — interactive charts
- **openFDA REST API** — public FDA data source
- **ICH E2B(R3)** — schema alignment standard

---

## Setup

### 1. Clone the repo

```bash
git clone https://github.com/<your-username>/pharmacovigilance-pipeline.git
cd pharmacovigilance-pipeline
```

### 2. Create a virtual environment

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 3. Set up PostgreSQL

Create the database and run the schema:

```bash
createdb pharmacovigilance
psql -U <your_username> -d pharmacovigilance -f db/schema.sql
```

### 4. Configure environment

```bash
cp .env.example .env
```

Edit `.env` with your PostgreSQL credentials. Optionally add a free openFDA API key from [open.fda.gov/apis/authentication](https://open.fda.gov/apis/authentication/) to increase your rate limit.

### 5. Run the ETL pipeline

```bash
python run_etl.py
```

This extracts ~1,000 reports per drug (6,000 total), transforms them, and loads them into PostgreSQL. Takes approximately 3–5 minutes depending on API response times.

### 6. Launch the dashboard

```bash
cd app
python app.py
```

Open [http://localhost:5000](http://localhost:5000) in your browser.

---

## Dashboard features

- **Overview page** — total/serious/fatal KPIs across all drugs, grouped bar chart, cross-drug reaction heatmap
- **Drug pages** — per-drug top 15 adverse reactions, reporting trend over time, co-medication neighborhood, sex distribution
- **JSON API** — `/api/data/<drug>` returns raw chart data for each drug

---

## Schema design

The PostgreSQL schema mirrors the ICH E2B(R3) Individual Case Safety Report structure:

| Table | E2B block | Key fields |
|-------|-----------|------------|
| `reports` | N.1 / N.2 | safety_report_id, seriousness flags, reporter_country |
| `patients` | D | age_years, sex, weight_kg |
| `drugs` | G | medicinal_product, generic_name, drug_role |
| `reactions` | E | reaction_term (MedDRA PT), outcome |
| `etl_runs` | — | Audit log for each pipeline run |

---

## Data notes

- Source: [FDA Adverse Event Reporting System (FAERS)](https://www.fda.gov/drugs/questions-and-answers-fdas-adverse-event-reporting-system-faers) via [openFDA](https://open.fda.gov/)
- FAERS is a spontaneous reporting system — it cannot establish causation or true incidence rates
- All data is de-identified per openFDA's public data terms
- Deduplication is performed on `safetyreportid` at both extract and load stages

---

## Future work

- Automated signal detection (disproportionality analysis / PRR)
- Risk scoring based on co-medication networks
- Integration with MIMIC-III discharge summaries for clinical NLP validation
- dbt transformation layer on top of PostgreSQL
