"""
app/app.py
----------
Flask + Plotly dashboard for pharmacovigilance analytics.

Routes:
  /                  → overview (all drugs, KPI cards)
  /drug/<name>       → drug-specific deep dive
  /api/data/<name>   → JSON endpoint for chart data (useful for future JS clients)

Run locally:
    cd app
    python app.py
"""

import os
import json
import psycopg2
import psycopg2.extras
import plotly.graph_objects as go
import plotly.express as px
from plotly.utils import PlotlyJSONEncoder
from flask import Flask, render_template, jsonify, request
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)

DRUGS = ["adderall", "ritalin", "xanax", "lexapro", "olanzapine", "seroquel"]

DRUG_LABELS = {
    "adderall":   "Adderall (amphetamine)",
    "ritalin":    "Ritalin (methylphenidate)",
    "xanax":      "Xanax (alprazolam)",
    "lexapro":    "Lexapro (escitalopram)",
    "olanzapine": "Olanzapine",
    "seroquel":   "Seroquel (quetiapine)",
}

# ------------------------------------------------------------------
# DB helpers
# ------------------------------------------------------------------
def get_db():
    return psycopg2.connect(
        host     = os.getenv("DB_HOST", "localhost"),
        port     = int(os.getenv("DB_PORT", 5432)),
        dbname   = os.getenv("DB_NAME", "pharmacovigilance"),
        user     = os.getenv("DB_USER"),
        password = os.getenv("DB_PASSWORD"),
    )

def query(sql: str, params=None) -> list[dict]:
    conn = get_db()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


# ------------------------------------------------------------------
# Data queries
# ------------------------------------------------------------------
def get_kpis(drug_name: str | None = None) -> dict:
    """Return top-line KPIs, optionally filtered by drug."""
    base_filter = ""
    params = []
    if drug_name:
        base_filter = "WHERE LOWER(d.generic_name) LIKE %s OR LOWER(d.medicinal_product) LIKE %s"
        params = [f"%{drug_name}%", f"%{drug_name}%"]

    sql = f"""
        SELECT
            COUNT(DISTINCT r.id)                                         AS total_reports,
            COUNT(DISTINCT r.id) FILTER (WHERE r.serious = TRUE)        AS serious_reports,
            COUNT(DISTINCT r.id) FILTER (WHERE r.serious_death = TRUE)  AS death_reports,
            ROUND(
                100.0 * COUNT(DISTINCT r.id) FILTER (WHERE r.serious = TRUE)
                / NULLIF(COUNT(DISTINCT r.id), 0), 1
            )                                                            AS pct_serious
        FROM reports r
        JOIN drugs d ON d.report_id = r.id
        {base_filter}
    """
    rows = query(sql, params or None)
    return rows[0] if rows else {}


def get_report_counts_by_drug() -> list[dict]:
    return query("""
        SELECT
            d.generic_name,
            COUNT(DISTINCT r.id) AS report_count,
            COUNT(DISTINCT r.id) FILTER (WHERE r.serious = TRUE) AS serious_count
        FROM drugs d
        JOIN reports r ON r.id = d.report_id
        WHERE d.drug_role = '1'
        GROUP BY d.generic_name
        ORDER BY report_count DESC
    """)


def get_top_reactions(drug_name: str, limit: int = 15) -> list[dict]:
    return query("""
        SELECT
            rx.reaction_term,
            COUNT(*) AS n
        FROM reactions rx
        JOIN reports r ON r.id = rx.report_id
        JOIN drugs d   ON d.report_id = r.id
        WHERE d.drug_role = '1'
          AND (LOWER(d.generic_name) LIKE %s OR LOWER(d.medicinal_product) LIKE %s)
          AND rx.reaction_term IS NOT NULL
        GROUP BY rx.reaction_term
        ORDER BY n DESC
        LIMIT %s
    """, (f"%{drug_name}%", f"%{drug_name}%", limit))


def get_serious_by_reporter(drug_name: str) -> list[dict]:
    return query("""
        SELECT
            COALESCE(r.reporter_qualification, 'unknown') AS reporter,
            COUNT(*) AS total,
            COUNT(*) FILTER (WHERE r.serious = TRUE) AS serious
        FROM reports r
        JOIN drugs d ON d.report_id = r.id
        WHERE d.drug_role = '1'
          AND (LOWER(d.generic_name) LIKE %s OR LOWER(d.medicinal_product) LIKE %s)
        GROUP BY reporter
        ORDER BY total DESC
    """, (f"%{drug_name}%", f"%{drug_name}%"))


def get_reports_over_time(drug_name: str) -> list[dict]:
    return query("""
        SELECT
            DATE_TRUNC('quarter', r.receipt_date) AS quarter,
            COUNT(*) AS n
        FROM reports r
        JOIN drugs d ON d.report_id = r.id
        WHERE d.drug_role = '1'
          AND r.receipt_date IS NOT NULL
          AND (LOWER(d.generic_name) LIKE %s OR LOWER(d.medicinal_product) LIKE %s)
        GROUP BY quarter
        ORDER BY quarter
    """, (f"%{drug_name}%", f"%{drug_name}%"))


def get_comeds(drug_name: str, limit: int = 12) -> list[dict]:
    """Concomitant drugs — the co-medication interaction map."""
    return query("""
        SELECT
            d2.generic_name   AS co_drug,
            COUNT(DISTINCT r.id) AS co_occurrences
        FROM reports r
        JOIN drugs d1 ON d1.report_id = r.id
        JOIN drugs d2 ON d2.report_id = r.id
        WHERE d1.drug_role = '1'
          AND d2.drug_role = '2'
          AND (LOWER(d1.generic_name) LIKE %s OR LOWER(d1.medicinal_product) LIKE %s)
          AND d2.generic_name IS NOT NULL
        GROUP BY d2.generic_name
        ORDER BY co_occurrences DESC
        LIMIT %s
    """, (f"%{drug_name}%", f"%{drug_name}%", limit))


def get_sex_distribution(drug_name: str) -> list[dict]:
    return query("""
        SELECT
            COALESCE(p.sex, 'unknown') AS sex,
            COUNT(*) AS n
        FROM patients p
        JOIN reports r ON r.id = p.report_id
        JOIN drugs d   ON d.report_id = r.id
        WHERE d.drug_role = '1'
          AND (LOWER(d.generic_name) LIKE %s OR LOWER(d.medicinal_product) LIKE %s)
        GROUP BY p.sex
    """, (f"%{drug_name}%", f"%{drug_name}%"))


def get_reaction_heatmap() -> list[dict]:
    """Top 10 reactions × all 6 drugs for heatmap."""
    return query("""
        WITH top_reactions AS (
            SELECT reaction_term, COUNT(*) AS n
            FROM reactions
            WHERE reaction_term IS NOT NULL
            GROUP BY reaction_term
            ORDER BY n DESC
            LIMIT 10
        )
        SELECT
            d.generic_name,
            rx.reaction_term,
            COUNT(*) AS n
        FROM reactions rx
        JOIN reports r ON r.id = rx.report_id
        JOIN drugs d   ON d.report_id = r.id
        JOIN top_reactions tr ON tr.reaction_term = rx.reaction_term
        WHERE d.drug_role = '1'
          AND d.generic_name IN ('amphetamine','methylphenidate','alprazolam',
                                  'escitalopram','olanzapine','quetiapine')
        GROUP BY d.generic_name, rx.reaction_term
    """)


# ------------------------------------------------------------------
# Chart builders
# ------------------------------------------------------------------
def chart_bar_reactions(drug_name: str) -> str:
    rows = get_top_reactions(drug_name)
    if not rows:
        return "{}"
    fig = go.Figure(go.Bar(
        x=[r["n"] for r in rows],
        y=[r["reaction_term"] for r in rows],
        orientation="h",
        marker_color="#6366f1",
    ))
    fig.update_layout(
        title=f"Top adverse reactions — {DRUG_LABELS.get(drug_name, drug_name)}",
        xaxis_title="Report count",
        yaxis=dict(autorange="reversed"),
        margin=dict(l=220, r=20, t=50, b=40),
        height=420,
        template="plotly_white",
    )
    return json.dumps(fig, cls=PlotlyJSONEncoder)


def chart_time_series(drug_name: str) -> str:
    rows = get_reports_over_time(drug_name)
    if not rows:
        return "{}"
    quarters = [str(r["quarter"])[:10] for r in rows]
    counts   = [r["n"] for r in rows]
    fig = go.Figure(go.Scatter(
        x=quarters, y=counts,
        mode="lines+markers",
        line=dict(color="#6366f1", width=2),
        marker=dict(size=5),
    ))
    fig.update_layout(
        title=f"Reports over time (by quarter) — {DRUG_LABELS.get(drug_name, drug_name)}",
        xaxis_title="Quarter",
        yaxis_title="Report count",
        template="plotly_white",
        height=320,
        margin=dict(l=60, r=20, t=50, b=40),
    )
    return json.dumps(fig, cls=PlotlyJSONEncoder)


def chart_comeds(drug_name: str) -> str:
    rows = get_comeds(drug_name)
    if not rows:
        return "{}"
    fig = go.Figure(go.Bar(
        x=[r["co_occurrences"] for r in rows],
        y=[r["co_drug"] for r in rows],
        orientation="h",
        marker_color="#10b981",
    ))
    fig.update_layout(
        title=f"Most common co-medications — {DRUG_LABELS.get(drug_name, drug_name)}",
        xaxis_title="Co-occurrences",
        yaxis=dict(autorange="reversed"),
        margin=dict(l=180, r=20, t=50, b=40),
        height=380,
        template="plotly_white",
    )
    return json.dumps(fig, cls=PlotlyJSONEncoder)


def chart_sex_pie(drug_name: str) -> str:
    rows = get_sex_distribution(drug_name)
    if not rows:
        return "{}"
    fig = go.Figure(go.Pie(
        labels=[r["sex"] for r in rows],
        values=[r["n"] for r in rows],
        hole=0.4,
        marker_colors=["#6366f1", "#f59e0b", "#d1d5db"],
    ))
    fig.update_layout(
        title="Sex distribution",
        template="plotly_white",
        height=300,
        margin=dict(l=20, r=20, t=50, b=20),
    )
    return json.dumps(fig, cls=PlotlyJSONEncoder)


def chart_heatmap() -> str:
    rows = get_reaction_heatmap()
    if not rows:
        return "{}"
    drugs     = sorted(set(r["generic_name"] for r in rows if r["generic_name"]))
    reactions = sorted(set(r["reaction_term"] for r in rows if r["reaction_term"]))
    z = [[0] * len(reactions) for _ in drugs]
    drug_idx = {d: i for i, d in enumerate(drugs)}
    rxn_idx  = {rx: j for j, rx in enumerate(reactions)}
    for r in rows:
        if r["generic_name"] and r["reaction_term"]:
            z[drug_idx[r["generic_name"]]][rxn_idx[r["reaction_term"]]] = r["n"]
    fig = go.Figure(go.Heatmap(
        z=z,
        x=reactions,
        y=drugs,
        colorscale="Purples",
        hoverongaps=False,
    ))
    fig.update_layout(
        title="Reaction frequency heatmap — all drugs",
        xaxis=dict(tickangle=-40),
        margin=dict(l=120, r=20, t=60, b=160),
        height=420,
        template="plotly_white",
    )
    return json.dumps(fig, cls=PlotlyJSONEncoder)


def chart_overview_bar() -> str:
    rows = get_report_counts_by_drug()
    if not rows:
        return "{}"
    fig = go.Figure()
    fig.add_trace(go.Bar(
        name="Total",
        x=[r["generic_name"] for r in rows],
        y=[r["report_count"] for r in rows],
        marker_color="#c7d2fe",
    ))
    fig.add_trace(go.Bar(
        name="Serious",
        x=[r["generic_name"] for r in rows],
        y=[r["serious_count"] for r in rows],
        marker_color="#6366f1",
    ))
    fig.update_layout(
        barmode="overlay",
        title="Report volume by drug (total vs serious)",
        xaxis_title="Drug",
        yaxis_title="Report count",
        template="plotly_white",
        height=360,
        margin=dict(l=60, r=20, t=50, b=60),
    )
    return json.dumps(fig, cls=PlotlyJSONEncoder)


# ------------------------------------------------------------------
# Routes
# ------------------------------------------------------------------
@app.route("/")
def overview():
    kpis         = get_kpis()
    overview_bar = chart_overview_bar()
    heatmap      = chart_heatmap()
    return render_template(
        "overview.html",
        kpis=kpis,
        drugs=DRUGS,
        drug_labels=DRUG_LABELS,
        overview_bar=overview_bar,
        heatmap=heatmap,
    )


@app.route("/drug/<drug_name>")
def drug_view(drug_name: str):
    if drug_name not in DRUGS:
        return "Drug not found", 404
    kpis        = get_kpis(drug_name)
    bar_chart   = chart_bar_reactions(drug_name)
    time_chart  = chart_time_series(drug_name)
    comed_chart = chart_comeds(drug_name)
    sex_chart   = chart_sex_pie(drug_name)
    return render_template(
        "drug.html",
        drug=drug_name,
        label=DRUG_LABELS.get(drug_name, drug_name),
        drugs=DRUGS,
        drug_labels=DRUG_LABELS,
        kpis=kpis,
        bar_chart=bar_chart,
        time_chart=time_chart,
        comed_chart=comed_chart,
        sex_chart=sex_chart,
    )


@app.route("/api/data/<drug_name>")
def api_data(drug_name: str):
    """JSON endpoint — handy for testing queries or future front-end work."""
    if drug_name not in DRUGS:
        return jsonify({"error": "drug not found"}), 404
    return jsonify({
        "kpis":      get_kpis(drug_name),
        "reactions": get_top_reactions(drug_name),
        "comeds":    get_comeds(drug_name),
        "timeline":  get_reports_over_time(drug_name),
    })


if __name__ == "__main__":
    app.run(debug=True, port=5000)
