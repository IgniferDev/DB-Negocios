# modelo/train_rayleigh.py
"""
Entrena un modelo Rayleigh sobre fact_project (DW Kiry) y guarda el parámetro en model_params.
Este script usa las variables DST_HOST, DST_DB, DST_USER, DST_PASS desde .env (misma configuración que ETL).
"""
import os
import pyodbc
import pandas as pd
import math
from datetime import datetime
from dotenv import load_dotenv
import sys

# aseguramos que el package modelo se pueda importar (por si se ejecuta desde /modelo)
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.append(ROOT)

from modelo.rayleigh_model import fit_rayleigh_from_defect_density

load_dotenv()

# Leemos las mismas variables que usamos para DST en el ETL
DST_HOST = os.getenv("DST_HOST")
DST_DB   = os.getenv("DST_DB")
DST_USER = os.getenv("DST_USER")
DST_PASS = os.getenv("DST_PASS")

DRIVER = "ODBC Driver 17 for SQL Server"

def connect_dw():
    if not all([DST_HOST, DST_DB, DST_USER, DST_PASS]):
        raise RuntimeError("Faltan variables de entorno DST_HOST/DST_DB/DST_USER/DST_PASS en .env")
    conn_str = (
        f"DRIVER={{{DRIVER}}};"
        f"SERVER={DST_HOST};"
        f"DATABASE={DST_DB};"
        f"UID={DST_USER};"
        f"PWD={DST_PASS};"
    )
    print("Conexión: ", DST_HOST, DST_DB, DST_USER)
    return pyodbc.connect(conn_str, autocommit=True, timeout=10)

def train_model():
    try:
        cn = connect_dw()
    except Exception as e:
        print("ERROR: no se pudo conectar al DW:", e)
        return

    print("Conectado al DW, leyendo datos de fact_project...")
    try:
        df = pd.read_sql("SELECT project_id, total_hours, total_errors FROM fact_project WHERE total_hours > 0", cn)
    except Exception as e:
        print("ERROR leyendo fact_project:", e)
        return

    if df.empty:
        print("No hay datos suficientes en fact_project para entrenar el modelo.")
        return

    # calcular densidad de defectos
    df["defect_density"] = df["total_errors"] / df["total_hours"]

    # ajustar sigma con la función del módulo
    sigma = fit_rayleigh_from_defect_density(df["defect_density"].tolist())

    print(f"Modelo entrenado. sigma = {sigma:.8f}")

    # crear tabla model_params si no existe y guardar
    cur = cn.cursor()
    cur.execute("""
    IF OBJECT_ID('dbo.model_params','U') IS NULL
    CREATE TABLE dbo.model_params (
        model_name VARCHAR(100),
        param_name VARCHAR(100),
        param_value FLOAT,
        trained_at DATETIME
    );
    """)
    try:
        cur.execute("DELETE FROM dbo.model_params;")
    except Exception:
        pass

    cur.execute("INSERT INTO dbo.model_params (model_name, param_name, param_value, trained_at) VALUES (?,?,?,?)",
                ("rayleigh_defect_model", "sigma", float(sigma), datetime.now()))
    try:
        cn.commit()
    except:
        pass

    print("Parámetro guardado en model_params. Fin.")

if __name__ == "__main__":
    train_model()
