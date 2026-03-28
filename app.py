"""
app/app.py
----------
Flask + Plotly dashboard for pharmacovigilance analytics.
Production-ready for Render deployment.

Routes:
  /                  → overview (all drugs, KPI cards)
  /drug/<name>       → drug-specific deep dive
  /api/data/<name>   → JSON endpoint for chart data
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
from urllib.parse import urlparse

load_dotenv()

# ------------------------------------------------------------------
# App setup — template path works both locally and on Render
# ------------------------------------------------------------------
app = Flask(
    __name__,
    template_folder=os.path.join(os.path.dirname(__file__), "templates"),
)
app.config["TEMPLATES_AUTO_RELOAD"] = True

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
# DB connection — supports both individual vars and DATABASE_URL
# (Render PostgreSQL provides DATABASE_URL automatically)
# ------------------------------------------------------------------
def get_db():
    database_url = os.getenv("DATABASE_URL")
    if database_url:
        # Render provides postgres:// — psycopg2 needs postgresql://
        if database_url.startswith("postgres://"):
            database_url = database_url.replace("postgres://", "postgresql://", 1)
        result = urlparse(database_url)
        return psycopg2.connect(
            host     = result.hostname,
            port     = result.port or 5432,
            dbname   = result.path[1:],
            user     = result.username,
            password = result.password,
            sslmode  = "require",
        )
    # Fall back to individual env vars (local development)
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
def get_kpis(drug_name=None):
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


def get_report_counts_by_drug():
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


def get_top_reactions(drug_name, limit=15):
    return query("""
        SELECT rx.reaction_term, COUNT(*) AS n
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


def get_serious_by_reporter(drug_name):
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


def get_reports_over_time(drug_name):
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


def get_comeds(drug_name, limit=12):
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


def get_sex_distribution(drug_name):
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


def get_reaction_heatmap():
    return query("""
        WITH top_reactions AS (
            SELECT reaction_term, COUNT(*) AS n
            FROM reactions
            WHERE reaction_term IS NOT NULL
            GROUP BY reaction_term
            ORDER BY n DESC
            LIMIT 10
        )
        SELECT d.generic_name, rx.reaction_term, COUNT(*) AS n
        FROM reactions rx
        JOIN reports r ON r.id = rx.report_id
        JOIN drugs d   ON d.report_id = r.id
        JOIN top_reactions tr ON tr.reaction_term = rx.reaction_term
        WHERE d.drug_role = '1'
        GROUP BY d.generic_name, rx.reaction_term
    """)


# ------------------------------------------------------------------
# Chart builders
# ------------------------------------------------------------------
CHART_COLORS = {
    "primary":   "#00d4aa",
    "secondary": "#3b82f6",
    "warning":   "#f59e0b",
    "danger":    "#ef4444",
    "bg":        "rgba(0,0,0,0)",
    "paper":     "rgba(0,0,0,0)",
    "font":      "#e2e8f0",
    "grid":      "#1e2d45",
}

def _dark_layout(title="", height=380, margin=None):
    return dict(
        title       = dict(text=title, font=dict(color=CHART_COLORS["font"], size=13)),
        paper_bgcolor = CHART_COLORS["paper"],
        plot_bgcolor  = CHART_COLORS["bg"],
        font          = dict(color=CHART_COLORS["font"], family="DM Mono, monospace", size=11),
        height        = height,
        margin        = margin or dict(l=60, r=20, t=50, b=40),
        xaxis         = dict(gridcolor=CHART_COLORS["grid"], linecolor=CHART_COLORS["grid"],
                             zerolinecolor=CHART_COLORS["grid"]),
        yaxis         = dict(gridcolor=CHART_COLORS["grid"], linecolor=CHART_COLORS["grid"],
                             zerolinecolor=CHART_COLORS["grid"]),
    )


def chart_bar_reactions(drug_name):
    rows = get_top_reactions(drug_name)
    if not rows:
        return "{}"
    fig = go.Figure(go.Bar(
        x=[r["n"] for r in rows],
        y=[r["reaction_term"] for r in rows],
        orientation="h",
        marker_color=CHART_COLORS["primary"],
    ))
    fig.update_layout(**_dark_layout(
        title=f"Top adverse reactions — {DRUG_LABELS.get(drug_name, drug_name)}",
        height=420,
        margin=dict(l=220, r=20, t=50, b=40),
    ))
    fig.update_layout(yaxis=dict(autorange="reversed"))
    return json.dumps(fig, cls=PlotlyJSONEncoder)


def chart_time_series(drug_name):
    rows = get_reports_over_time(drug_name)
    if not rows:
        return "{}"
    fig = go.Figure(go.Scatter(
        x=[str(r["quarter"])[:10] for r in rows],
        y=[r["n"] for r in rows],
        mode="lines+markers",
        line=dict(color=CHART_COLORS["primary"], width=2),
        marker=dict(size=5),
    ))
    fig.update_layout(**_dark_layout(
        title=f"Reports over time — {DRUG_LABELS.get(drug_name, drug_name)}",
        height=300,
        margin=dict(l=60, r=20, t=50, b=40),
    ))
    fig.update_layout(xaxis_title="Quarter", yaxis_title="Reports")
    return json.dumps(fig, cls=PlotlyJSONEncoder)


def chart_comeds(drug_name):
    rows = get_comeds(drug_name)
    if not rows:
        return "{}"
    fig = go.Figure(go.Bar(
        x=[r["co_occurrences"] for r in rows],
        y=[r["co_drug"] for r in rows],
        orientation="h",
        marker_color=CHART_COLORS["secondary"],
    ))
    fig.update_layout(**_dark_layout(
        title=f"Most common co-medications — {DRUG_LABELS.get(drug_name, drug_name)}",
        height=380,
        margin=dict(l=180, r=20, t=50, b=40),
    ))
    fig.update_layout(yaxis=dict(autorange="reversed"))
    return json.dumps(fig, cls=PlotlyJSONEncoder)


def chart_sex_pie(drug_name):
    rows = get_sex_distribution(drug_name)
    if not rows:
        return "{}"
    fig = go.Figure(go.Pie(
        labels=[r["sex"] for r in rows],
        values=[r["n"] for r in rows],
        hole=0.4,
        marker_colors=[CHART_COLORS["primary"], CHART_COLORS["warning"], CHART_COLORS["grid"]],
    ))
    fig.update_layout(**_dark_layout(title="Sex distribution", height=300,
                                      margin=dict(l=20, r=20, t=50, b=20)))
    return json.dumps(fig, cls=PlotlyJSONEncoder)


def chart_heatmap():
    rows = get_reaction_heatmap()
    if not rows:
        return "{}"
    drugs     = sorted(set(r["generic_name"] for r in rows if r["generic_name"]))
    reactions = sorted(set(r["reaction_term"] for r in rows if r["reaction_term"]))
    z = [[0] * len(reactions) for _ in drugs]
    drug_idx  = {d: i for i, d in enumerate(drugs)}
    rxn_idx   = {rx: j for j, rx in enumerate(reactions)}
    for r in rows:
        if r["generic_name"] and r["reaction_term"]:
            z[drug_idx[r["generic_name"]]][rxn_idx[r["reaction_term"]]] = r["n"]
    fig = go.Figure(go.Heatmap(
        z=z, x=reactions, y=drugs,
        colorscale=[[0, "#0a0e17"], [0.5, "#0f6e56"], [1, "#00d4aa"]],
        hoverongaps=False,
    ))
    fig.update_layout(**_dark_layout(
        title="Reaction frequency heatmap — all drugs",
        height=420,
        margin=dict(l=120, r=20, t=60, b=160),
    ))
    fig.update_layout(xaxis=dict(tickangle=-40))
    return json.dumps(fig, cls=PlotlyJSONEncoder)


def chart_overview_bar():
    rows = get_report_counts_by_drug()
    if not rows:
        return "{}"
    fig = go.Figure()
    fig.add_trace(go.Bar(
        name="Total",
        x=[r["generic_name"] for r in rows],
        y=[r["report_count"] for r in rows],
        marker_color="#1e3a5f",
    ))
    fig.add_trace(go.Bar(
        name="Serious",
        x=[r["generic_name"] for r in rows],
        y=[r["serious_count"] for r in rows],
        marker_color=CHART_COLORS["primary"],
    ))
    fig.update_layout(**_dark_layout(
        title="Report volume by drug — total vs serious",
        height=360,
        margin=dict(l=60, r=20, t=50, b=60),
    ))
    fig.update_layout(barmode="overlay")
    return json.dumps(fig, cls=PlotlyJSONEncoder)


# ------------------------------------------------------------------
# Routes
# ------------------------------------------------------------------
@app.route("/")
def overview():
    return render_template(
        "overview.html",
        kpis         = get_kpis(),
        drugs        = DRUGS,
        drug_labels  = DRUG_LABELS,
        overview_bar = chart_overview_bar(),
        heatmap      = chart_heatmap(),
    )


@app.route("/drug/<drug_name>")
def drug_view(drug_name):
    if drug_name not in DRUGS:
        return "Drug not found", 404
    return render_template(
        "drug.html",
        drug        = drug_name,
        label       = DRUG_LABELS.get(drug_name, drug_name),
        drugs       = DRUGS,
        drug_labels = DRUG_LABELS,
        kpis        = get_kpis(drug_name),
        bar_chart   = chart_bar_reactions(drug_name),
        time_chart  = chart_time_series(drug_name),
        comed_chart = chart_comeds(drug_name),
        sex_chart   = chart_sex_pie(drug_name),
    )


@app.route("/api/data/<drug_name>")
def api_data(drug_name):
    if drug_name not in DRUGS:
        return jsonify({"error": "drug not found"}), 404
    return jsonify({
        "kpis":      get_kpis(drug_name),
        "reactions": get_top_reactions(drug_name),
        "comeds":    get_comeds(drug_name),
        "timeline":  get_reports_over_time(drug_name),
    })


@app.route("/health")
def health():
    """Health check endpoint for Render."""
    try:
        conn = get_db()
        conn.close()
        return jsonify({"status": "ok", "db": "connected"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


if __name__ == "__main__":
    app.config["TEMPLATES_AUTO_RELOAD"] = True
    app.run(debug=True, port=5000)
