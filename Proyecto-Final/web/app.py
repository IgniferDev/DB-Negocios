# web/app.py
import os, math, pyodbc
from datetime import datetime
from flask import Flask, render_template, request, jsonify
from dotenv import load_dotenv
load_dotenv()

# Config
DRIVER = "ODBC Driver 17 for SQL Server"
# DW (destination) - Kiry
DST_HOST = os.getenv("DST_HOST")
DST_DB   = os.getenv("DST_DB")
DST_USER = os.getenv("DST_USER")
DST_PASS = os.getenv("DST_PASS")
# SRC (source) - Ponki
SRC_HOST = os.getenv("SRC_HOST")
SRC_DB   = os.getenv("SRC_DB")
SRC_USER = os.getenv("SRC_USER")
SRC_PASS = os.getenv("SRC_PASS")

API_KEY = os.getenv("PROJECT_MANAGER_TOKEN", "CHANGE_ME_TOKEN")

app = Flask(__name__, template_folder="templates")


# ---- Connections ----
def get_dw_conn():
    """Conexión al DW (Kiry)."""
    if not (DST_HOST and DST_DB and DST_USER and DST_PASS):
        raise RuntimeError("DW credentials missing in .env")
    conn_str = f"DRIVER={{{DRIVER}}};SERVER={DST_HOST};DATABASE={DST_DB};UID={DST_USER};PWD={DST_PASS}"
    return pyodbc.connect(conn_str, autocommit=True)

def get_src_conn():
    """Conexión a la BD origen (Ponki)."""
    if not (SRC_HOST and SRC_DB and SRC_USER and SRC_PASS):
        raise RuntimeError("SRC credentials missing in .env")
    conn_str = f"DRIVER={{{DRIVER}}};SERVER={SRC_HOST};DATABASE={SRC_DB};UID={SRC_USER};PWD={SRC_PASS}"
    return pyodbc.connect(conn_str, autocommit=True)


# ---- CUBO endpoints ----
@app.route("/api/cube/face", methods=["GET"])
def cube_face():
    dim_row = request.args.get("dim_row", "project")   # project / team
    dim_col = request.args.get("dim_col", "year")      # year / month
    measure = request.args.get("measure", "total_hours")

    allowed_rows = {"project":"dp.project_name","team":"dt_team.team_name"}
    allowed_cols = {"year":"d.year","month":"d.month"}
    allowed_measures = {"total_hours":"f.total_hours","total_cost":"f.total_cost","total_revenue":"f.total_revenue","total_errors":"f.total_errors"}

    if dim_row not in allowed_rows or dim_col not in allowed_cols or measure not in allowed_measures:
        return jsonify({"error":"invalid parameters"}), 400

    sql = f"""
    SELECT {allowed_rows[dim_row]} AS dim_row, {allowed_cols[dim_col]} AS dim_col,
           SUM({allowed_measures[measure]}) AS value
    FROM dbo.fact_project f
    JOIN dbo.dim_project dp ON f.project_id = dp.project_id
    JOIN dbo.dim_date d ON f.start_date_id = d.date_id
    LEFT JOIN dbo.dim_team dt_team ON f.team_id = dt_team.team_id
    GROUP BY {allowed_rows[dim_row]}, {allowed_cols[dim_col]}
    ORDER BY {allowed_rows[dim_row]}, {allowed_cols[dim_col]};
    """
    cn = get_dw_conn(); cur = cn.cursor()
    rows = []
    try:
        for r in cur.execute(sql).fetchall():
            rows.append({"dim_row": r[0], "dim_col": r[1], "value": float(r[2]) if r[2] is not None else 0.0})
    finally:
        cn.close()
    return jsonify(rows)


@app.route("/api/cube/section", methods=["GET"])
def cube_section():
    year = request.args.get("year")
    team_id = request.args.get("team_id")
    project_id = request.args.get("project_id")

    where_clauses = []
    if year:
        try:
            int(year)
            where_clauses.append(f"d.year = {int(year)}")
        except:
            return jsonify({"error":"invalid year"}), 400
    if team_id:
        try:
            where_clauses.append(f"f.team_id = {int(team_id)}")
        except:
            return jsonify({"error":"invalid team_id"}), 400
    if project_id:
        try:
            where_clauses.append(f"f.project_id = {int(project_id)}")
        except:
            return jsonify({"error":"invalid project_id"}), 400

    where = " AND ".join(where_clauses) if where_clauses else "1=1"
    sql = f"""
    SELECT dp.project_id, dp.project_name, d.year, f.total_hours, f.total_cost, f.total_revenue, f.total_errors
    FROM dbo.fact_project f
    JOIN dbo.dim_project dp ON f.project_id = dp.project_id
    JOIN dbo.dim_date d ON f.start_date_id = d.date_id
    WHERE {where}
    ORDER BY f.project_id
    """
    cn = get_dw_conn(); cur = cn.cursor()
    out = []
    try:
        for r in cur.execute(sql).fetchall():
            out.append({
                "project_id": int(r[0]),
                "project_name": r[1],
                "year": int(r[2]) if r[2] is not None else None,
                "total_hours": float(r[3]) if r[3] is not None else 0,
                "total_cost": float(r[4]) if r[4] is not None else 0,
                "total_revenue": float(r[5]) if r[5] is not None else 0,
                "total_errors": int(r[6]) if r[6] is not None else 0
            })
    finally:
        cn.close()
    return jsonify(out)


@app.route("/api/cube/cell", methods=["GET"])
def cube_cell():
    project_id = request.args.get("project_id")
    year = request.args.get("year")
    if not project_id or not year:
        return jsonify({"error":"project_id and year required"}), 400

    # datos soporte -> desde BD origen (Ponki)
    sql = """
    SELECT t.log_date, e.employee_id, e.name, t.hours, t.cost
    FROM dbo.time_log t
    LEFT JOIN dbo.employee e ON t.employee_id = e.employee_id
    WHERE t.project_id = ? AND YEAR(t.log_date) = ?
    ORDER BY t.log_date
    """
    cn = get_src_conn(); cur = cn.cursor()
    rows = []
    try:
        for r in cur.execute(sql, (int(project_id), int(year))).fetchall():
            rows.append({
                "log_date": r[0].isoformat() if r[0] else None,
                "employee_id": int(r[1]) if r[1] else None,
                "employee_name": r[2],
                "hours": float(r[3]) if r[3] else 0.0,
                "cost": float(r[4]) if r[4] else 0.0
            })
    finally:
        cn.close()
    return jsonify(rows)


# ---- KPIs endpoint ----
@app.route("/api/kpis", methods=["GET"])
def api_kpis():
    dst_cn = get_dw_conn(); dst_cur = dst_cn.cursor()
    src_cn = None
    try:
        q1 = """
        SELECT dp.project_id, dp.project_name, f.total_hours, f.total_cost
        FROM dbo.fact_project f
        JOIN dbo.dim_project dp ON f.project_id = dp.project_id
        ORDER BY f.total_hours DESC
        """
        rows_q1 = [list(r) for r in dst_cur.execute(q1).fetchall()]

        # origen
        src_cn = get_src_conn(); src_cur = src_cn.cursor()
        q2 = """
        SELECT e.employee_id, e.name, ISNULL(SUM(t.hours),0) as hours_last_30, e.capacity_weekly
        FROM dbo.employee e
        LEFT JOIN dbo.time_log t ON e.employee_id = t.employee_id AND t.log_date >= DATEADD(day,-30,GETDATE())
        GROUP BY e.employee_id, e.name, e.capacity_weekly
        ORDER BY hours_last_30 DESC
        """
        rows_q2 = [list(r) for r in src_cur.execute(q2).fetchall()]

        q3 = "SELECT project_id, severity, COUNT(*) AS cnt FROM dbo.[issue] GROUP BY project_id, severity"
        rows_q3 = [list(r) for r in src_cur.execute(q3).fetchall()]

        q4 = """
        SELECT f.project_id, dp.project_name, f.total_cost,
               dstart.full_date as start_date, dend.full_date as end_date,
               CASE WHEN DATEDIFF(day, dstart.full_date, dend.full_date) > 0 THEN f.total_cost / NULLIF(DATEDIFF(day, dstart.full_date, dend.full_date),0) ELSE NULL END as burn_rate_per_day
        FROM dbo.fact_project f
        JOIN dbo.dim_project dp ON f.project_id = dp.project_id
        LEFT JOIN dbo.dim_date dstart ON f.start_date_id = dstart.date_id
        LEFT JOIN dbo.dim_date dend ON f.end_date_id = dend.date_id
        """
        rows_q4 = [list(r) for r in dst_cur.execute(q4).fetchall()]

        result = {
            "hours_cost_per_project": rows_q1,
            "utilization_last_30_by_employee": rows_q2,
            "issues_by_severity": rows_q3,
            "burn_rate": rows_q4
        }
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        try:
            if src_cn: src_cn.close()
        except: pass
        dst_cn.close()


# ---- PREDICT endpoint (usa model_params en DW) ----
@app.route("/predict", methods=["POST"])
def predict():
    token = request.headers.get("X-API-KEY")
    if token != API_KEY:
        return jsonify({"error":"Unauthorized"}), 401

    data = request.get_json() or {}
    if "project_hours" not in data:
        return jsonify({"error":"Missing project_hours"}), 400
    hours = float(data["project_hours"])

    # read sigma from DW
    cn = get_dw_conn(); cur = cn.cursor()
    try:
        row = cur.execute("SELECT TOP 1 param_value, trained_at FROM dbo.model_params WHERE param_name = ? ORDER BY trained_at DESC", ('sigma',)).fetchone()
        if not row:
            return jsonify({"error":"Model not available"}), 500
        sigma = float(row[0]); trained_on = row[1]
    finally:
        cn.close()

    expected_density = sigma * math.sqrt(math.pi/2.0)
    predicted = expected_density * hours
    def pct(p): return sigma * math.sqrt(-2.0*math.log(1.0-p)) * hours
    p10, p50, p90 = pct(0.10), pct(0.50), pct(0.90)

    # insert audit
    cn = get_dw_conn(); cur = cn.cursor()
    try:
        cur.execute("""
            IF OBJECT_ID('dbo.prediction_audit','U') IS NULL
            CREATE TABLE dbo.prediction_audit (
                run_ts DATETIME, user_token VARCHAR(200), project_hours FLOAT, predicted FLOAT, p10 FLOAT, p50 FLOAT, p90 FLOAT
            );
        """)
        cur.execute("INSERT INTO dbo.prediction_audit (run_ts,user_token,project_hours,predicted,p10,p50,p90) VALUES (?,?,?,?,?,?,?)",
                    (datetime.now(), token, hours, float(predicted), float(p10), float(p50), float(p90)))
        cn.commit()
    finally:
        cn.close()

    return jsonify({"sigma":sigma, "predicted_defects":float(predicted), "p10":float(p10), "p50":float(p50), "p90":float(p90), "trained_on": str(trained_on)})


# ---- Pages ----
@app.route("/")
def index():
    return render_template("index.html")

@app.route("/dashboard")
def dashboard_page():
    return render_template("dashboard.html")

@app.route("/cube")
def cube_page():
    return render_template("cube.html")

@app.route("/bsc")
def bsc_page():
    return render_template("bsc.html")


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.getenv("WEB_PORT", 5000)))
