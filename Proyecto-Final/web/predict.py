# web/predict.py  (importar en app.py y registrar blueprint o ruta)
import os, math, json
from flask import Flask, request, jsonify, abort
import pyodbc
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()
DRIVER = "ODBC Driver 17 for SQL Server"
DST_HOST = os.getenv("DST_HOST")
DST_DB   = os.getenv("DST_DB")
DST_USER = os.getenv("DST_USER")
DST_PASS = os.getenv("DST_PASS")

API_KEY = os.getenv("PROJECT_MANAGER_TOKEN", "mi_token_seguro")  # usar .env real

def get_dw_conn():
    conn_str = f"DRIVER={{{DRIVER}}};SERVER={DST_HOST};DATABASE={DST_DB};UID={DST_USER};PWD={DST_PASS}"
    return pyodbc.connect(conn_str, autocommit=True)

def read_sigma():
    cn = get_dw_conn()
    cur = cn.cursor()
    # Ajustar según cómo guardaste: si tienes columna sigma explicita:
    try:
        rows = cur.execute("SELECT TOP 1 sigma, mean, trained_on FROM dbo.model_params ORDER BY trained_on DESC").fetchall()
        if not rows:
            return None
        r = rows[0]
        sigma = float(r[0]) if r[0] is not None else None
        mean = float(r[1]) if len(r) > 1 and r[1] is not None else None
        trained_on = r[2] if len(r) > 2 else None
        return {"sigma": sigma, "mean": mean, "trained_on": trained_on}
    finally:
        cn.close()

def rayleigh_expected_sigma_to_defects(sigma, hours):
    # E[X] = sigma * sqrt(pi/2)
    expected_density = sigma * math.sqrt(math.pi / 2.0)
    return expected_density * hours

def rayleigh_percentile(sigma, p, hours):
    # x_p = sigma * sqrt(-2 ln(1-p))
    x_p = sigma * math.sqrt(-2.0 * math.log(1.0 - p))
    return x_p * hours

# Flask example (si ya tienes app en app.py, incorpora esta ruta)
app = Flask(__name__)

@app.route("/predict", methods=["POST"])
def predict():
    # Simple API key check
    token = request.headers.get("X-API-KEY")
    if token != API_KEY:
        return jsonify({"error": "Unauthorized"}), 401

    data = request.get_json() or {}
    if "project_hours" not in data:
        return jsonify({"error": "Missing project_hours"}), 400
    try:
        hours = float(data["project_hours"])
    except:
        return jsonify({"error": "Invalid project_hours"}), 400

    params = read_sigma()
    if not params or not params.get("sigma"):
        return jsonify({"error": "Model not available"}), 500

    sigma = params["sigma"]
    predicted = rayleigh_expected_sigma_to_defects(sigma, hours)
    p10 = rayleigh_percentile(sigma, 0.10, hours)
    p50 = rayleigh_percentile(sigma, 0.50, hours)  # mediana aproximada
    p90 = rayleigh_percentile(sigma, 0.90, hours)

    # Save audit
    cn = get_dw_conn()
    cur = cn.cursor()
    try:
        cur.execute("""
            IF OBJECT_ID('dbo.prediction_audit','U') IS NULL
            CREATE TABLE dbo.prediction_audit (
                run_ts DATETIME,
                project_hours FLOAT,
                sigma FLOAT,
                predicted FLOAT,
                p10 FLOAT,
                p50 FLOAT,
                p90 FLOAT,
                user_name VARCHAR(200)
            );
        """)
        cur.execute(
            "INSERT INTO dbo.prediction_audit (run_ts, project_hours, sigma, predicted, p10, p50, p90, user_name) VALUES (?,?,?,?,?,?,?,?)",
            (datetime.now(), hours, sigma, float(predicted), float(p10), float(p50), float(p90), request.headers.get("X-USER","unknown"))
        )
        cn.commit()
    finally:
        cn.close()

    return jsonify({
        "sigma": sigma,
        "predicted_defects": float(predicted),
        "p10": float(p10),
        "p50": float(p50),
        "p90": float(p90),
        "trained_on": str(params.get("trained_on"))
    })
