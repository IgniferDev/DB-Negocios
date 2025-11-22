import os
import pyodbc
import pandas as pd
import numpy as np
from flask import Flask, render_template, jsonify, request
from dotenv import load_dotenv
import math

# Cargar variables de entorno
load_dotenv()

app = Flask(__name__)

# Configuración de conexión a Kiry (DW)
DST_HOST = os.getenv("DST_HOST")
DST_DB = os.getenv("DST_DB")
DST_USER = os.getenv("DST_USER")
DST_PASS = os.getenv("DST_PASS")
DRIVER = "ODBC Driver 17 for SQL Server"

def get_db_connection():
    conn_str = f"DRIVER={{{DRIVER}}};SERVER={DST_HOST};DATABASE={DST_DB};UID={DST_USER};PWD={DST_PASS}"
    return pyodbc.connect(conn_str)

@app.route('/')
def index():
    # Renderiza el dashboard principal
    return render_template('dashboard.html')

@app.route('/api/kpi-data')
def kpi_data():
    """API para alimentar los KPIs y el Balanced Scorecard"""
    conn = get_db_connection()
    
    # --- CONSULTAS OLAP SIMULADAS SOBRE EL MODELO ESTRELLA ---
    
    # 1. Datos Generales (KPIs Misión)
    query_kpi = """
    SELECT 
        SUM(total_revenue) as revenue,
        SUM(total_cost) as cost,
        SUM(total_errors) as errors,
        SUM(total_hours) as hours,
        COUNT(DISTINCT project_id) as total_projects
    FROM fact_project
    """
    df_kpi = pd.read_sql(query_kpi, conn)
    
    # 2. Datos por Cliente (Para Gráfica de Barras - Perspectiva Cliente)
    query_client = """
    SELECT TOP 5 c.client_name, SUM(f.total_revenue) as revenue
    FROM fact_project f
    JOIN dim_client c ON f.client_id = c.client_id
    GROUP BY c.client_name
    ORDER BY revenue DESC
    """
    df_client = pd.read_sql(query_client, conn)
    
    # 3. Datos de Defectos vs Tiempo (Para Gráfica de Línea - Calidad)
    # Usamos dim_date para agrupar por mes (YYYYMM)
    query_trend = """
    SELECT d.year, d.month, SUM(f.total_errors) as errors, SUM(f.total_hours) as hours
    FROM fact_project f
    JOIN dim_date d ON f.end_date_id = d.date_id
    GROUP BY d.year, d.month
    ORDER BY d.year, d.month
    """
    df_trend = pd.read_sql(query_trend, conn)
    
    conn.close()

    # Procesamiento de datos
    total_rev = df_kpi['revenue'].iloc[0] or 0
    total_cost = df_kpi['cost'].iloc[0] or 0
    total_errors = df_kpi['errors'].iloc[0] or 0
    total_hours = df_kpi['hours'].iloc[0] or 1 # Evitar div/0
    
    # KPI Calculado: Defect Density (Errores por cada 1000 horas)
    defect_density = round((total_errors / total_hours) * 1000, 2)
    roi = round(((total_rev - total_cost) / total_cost) * 100 * 1.0, 2) if total_cost > 0 else 0

    data = {
        "kpis": {
            "revenue": f"${total_rev:,.2f}",
            "roi": f"{roi}%",
            "defect_density": defect_density,
            "projects": int(df_kpi['total_projects'].iloc[0])
        },
        "charts": {
            "clients": {
                "labels": df_client['client_name'].tolist(),
                "data": df_client['revenue'].tolist()
            },
            "trend": {
                "labels": [f"{row['year']}-{row['month']}" for _, row in df_trend.iterrows()],
                "errors": df_trend['errors'].tolist()
            }
        }
    }
    return jsonify(data)

@app.route('/api/predict', methods=['POST'])
def predict_defects():
    """
    Modelo de Predicción Rayleigh.
    Recibe: Duración estimada (input del usuario).
    Usa: Sigma (entrenado en DB).
    Retorna: Curva de probabilidad de defectos.
    """
    data = request.json
    estimated_hours = float(data.get('hours', 1000)) # Default 1000 horas
    
    conn = get_db_connection()
    # Obtener el sigma entrenado previamente
    try:
        query_sigma = "SELECT TOP 1 param_value FROM model_params WHERE param_name = 'sigma' ORDER BY trained_at DESC"
        cursor = conn.cursor()
        row = cursor.execute(query_sigma).fetchone()
        sigma = float(row[0]) if row else 0.5 # Fallback si no hay modelo entrenado
    except Exception:
        sigma = 0.5
    conn.close()

    # Generar curva Rayleigh para visualizar
    # Ecuación PDF: f(t) = (t / sigma^2) * exp(-t^2 / 2sigma^2)
    # Nota: Tu sigma fue entrenado con densidad (defectos/hora), así que lo usamos para escalar la predicción.
    
    # Simularemos la distribución de probabilidad a lo largo del tiempo del proyecto
    time_points = np.linspace(0, estimated_hours, 20)
    # Para visualización simple, usaremos la curva PDF estándar de Rayleigh escalada
    # Ajustamos sigma para que tenga sentido en el eje X (tiempo)
    # En un caso real, sigma estaría en unidades de tiempo. Asumiremos que el sigma de la DB es de densidad
    # y lo transformamos para la curva de proyección.
    
    # PURE RAYLEIGH CURVE FORMULA
    # Usamos un sigma_t proporcional a la duración para mostrar la forma de la curva
    sigma_t = estimated_hours * 0.4 
    pdf_values = []
    
    for t in time_points:
        val = (t / (sigma_t**2)) * math.exp(-(t**2) / (2*(sigma_t**2)))
        pdf_values.append(val)
        
    # Escalamos para representar "Defectos probables encontrados"
    # Factor de escala basado en el sigma de densidad original de la BD (intensidad de defectos)
    scale_factor = estimated_hours * sigma * 100 # Multiplicador heurístico para el demo
    predicted_defects = [round(v * scale_factor, 2) for v in pdf_values]
    
    total_predicted = sum(predicted_defects)

    return jsonify({
        "sigma_used": sigma,
        "total_predicted_defects": round(total_predicted),
        "chart_labels": [round(t) for t in time_points],
        "chart_data": predicted_defects
    })

if __name__ == '__main__':
    # Ejecutar en todas las interfaces para que sea visible en la VPN
    app.run(host='0.0.0.0', port=5000, debug=True)