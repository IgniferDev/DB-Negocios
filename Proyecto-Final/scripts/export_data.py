import json
import os
import pyodbc
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

# Configuración SQL
DST_HOST = os.getenv("DST_HOST")
DST_DB = os.getenv("DST_DB")
DST_USER = os.getenv("DST_USER")
DST_PASS = os.getenv("DST_PASS")
DRIVER = "ODBC Driver 17 for SQL Server"

conn_str = f"DRIVER={{{DRIVER}}};SERVER={DST_HOST};DATABASE={DST_DB};UID={DST_USER};PWD={DST_PASS}"
conn = pyodbc.connect(conn_str)

print("Conectado a Kiry. Extrayendo datos...")

# 1. KPIs Generales
df_kpi = pd.read_sql("""
    SELECT 
        SUM(total_revenue) as revenue, 
        SUM(total_cost) as cost, 
        SUM(total_errors) as errors, 
        SUM(total_hours) as hours, 
        COUNT(DISTINCT project_id) as total_projects 
    FROM fact_project
""", conn)

# 2. Tendencia General
df_trend = pd.read_sql("""
    SELECT d.year, d.month, SUM(f.total_errors) as errors 
    FROM fact_project f 
    JOIN dim_date d ON f.end_date_id = d.date_id 
    GROUP BY d.year, d.month 
    ORDER BY d.year, d.month
""", conn)

# 3. Top Clientes (Vista estática)
df_client = pd.read_sql("""
    SELECT TOP 5 c.client_name, SUM(f.total_revenue) as revenue 
    FROM fact_project f 
    JOIN dim_client c ON f.client_id = c.client_id 
    GROUP BY c.client_name 
    ORDER BY revenue DESC
""", conn)

# --- NUEVO: 4. DATASET OLAP GRANULAR (Para Slice & Dice en Web) ---
# Extraemos datos agrupados por Año, Cliente y Equipo para que el JS pueda filtrar
query_olap = """
    SELECT 
        d.year,
        c.client_name,
        t.team_name,
        SUM(f.total_revenue) as revenue,
        SUM(f.total_errors) as errors,
        SUM(f.total_hours) as hours,
        COUNT(f.project_id) as projects
    FROM fact_project f
    JOIN dim_date d ON f.end_date_id = d.date_id
    JOIN dim_client c ON f.client_id = c.client_id
    JOIN dim_team t ON f.team_id = t.team_id
    GROUP BY d.year, c.client_name, t.team_name
"""
df_olap = pd.read_sql(query_olap, conn)

# 5. Obtener Sigma
try:
    cursor = conn.cursor()
    row = cursor.execute("SELECT TOP 1 param_value FROM model_params WHERE param_name = 'sigma' ORDER BY trained_at DESC").fetchone()
    sigma = float(row[0]) if row else 0.5
except:
    sigma = 0.5

conn.close()

# Procesamiento de KPIs
rev = df_kpi['revenue'].iloc[0] or 0
cost = df_kpi['cost'].iloc[0] or 0
roi = round(((rev - cost)/cost)*100, 2) if cost > 0 else 0
density = round((df_kpi['errors'].iloc[0] / df_kpi['hours'].iloc[0]) * 1000, 2) if df_kpi['hours'].iloc[0] > 0 else 0

# Convertir el DataFrame OLAP a lista de diccionarios para el JSON
olap_data_list = df_olap.to_dict(orient='records')

# Estructura Final JSON
data_export = {
    "kpis": {
        "revenue": rev,
        "roi": roi,
        "defect_density": density,
        "projects": int(df_kpi['total_projects'].iloc[0]),
        "sigma_raw": sigma
    },
    "charts": {
        "clients_labels": df_client['client_name'].tolist(),
        "clients_data": df_client['revenue'].tolist(),
        "trend_labels": [f"{r['year']}-{r['month']}" for _, r in df_trend.iterrows()],
        "trend_data": df_trend['errors'].tolist()
    },
    "olap_cube": olap_data_list  # <--- Aquí va la "magia" para la nueva vista
}

with open('dashboard_data.json', 'w') as f:
    json.dump(data_export, f)

print("Datos exportados a dashboard_data.json exitosamente (Incluyendo cubo OLAP).")