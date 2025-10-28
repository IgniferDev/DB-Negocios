# app.py
from flask import Flask, render_template, request, jsonify
import inspect
import pandas as pd
import plotly.graph_objects as go
import os

# importar las funciones del paquete 'funciones'
from funciones.generarDatos import generar_dataset
from funciones.crearCubo import cubo_base, pivot_multimedidas
from funciones.operacionesCubo import (
    slice_por_anio, dice_subset, rollup_por_anio, rollup_por_anio_trimestre,
    drilldown_producto_region, pivot_anio_region
)

app = Flask(__name__, static_folder='static', template_folder='templates')

# Cargar dataset en memoria al iniciar la app
DF = generar_dataset()

@app.route("/")
def index():
    import funciones.crearCubo as crearCubo_mod
    import funciones.operacionesCubo as op_mod
    funcs_crear = [(n, (inspect.getdoc(o) or "").splitlines()[0] if inspect.getdoc(o) else "") 
                   for n, o in inspect.getmembers(crearCubo_mod, inspect.isfunction)]
    funcs_op = [(n, (inspect.getdoc(o) or "").splitlines()[0] if inspect.getdoc(o) else "") 
                for n, o in inspect.getmembers(op_mod, inspect.isfunction)]
    return render_template("index.html", funcs_crear=funcs_crear, funcs_op=funcs_op)

@app.route("/face", methods=["GET","POST"])
def face():
    # Cara: Producto x Región para Año + Trimestre fijados
    try:
        year = int(request.values.get("year", 2024))
    except:
        year = 2024
    try:
        trimestre = int(request.values.get("trimestre", 1))
    except:
        trimestre = 1

    df_slice = DF[(DF["Año"]==year) & (DF["Trimestre"]==trimestre)]
    pivot = pd.pivot_table(df_slice, values="Ventas", index="Producto", columns="Región", aggfunc="sum", fill_value=0)
    html_table = pivot.to_html(classes="table table-sm table-striped", border=0)

    x = list(pivot.columns)
    y = list(pivot.index)
    z = pivot.values.tolist()

    fig = go.Figure(data=go.Heatmap(z=z, x=x, y=y, colorscale="Viridis"))
    fig.update_layout(title=f"Cara del cubo: Año {year} - Trimestre {trimestre}", autosize=True, margin=dict(t=50,b=20))

    graphJSON = fig.to_plotly_json()   # dict serializable para templates

    return render_template("face.html", year=year, trimestre=trimestre, table_html=html_table, graphJSON=graphJSON)

@app.route("/section", methods=["GET","POST"])
def section():
    # Sección: elegir años y regiones (por formulario o querystring)
    years_raw = request.values.getlist("year")
    if not years_raw:
        years = [2024]
    else:
        # aceptar "2024" o múltiples
        years = [int(y) for y in years_raw if y.isdigit()]

    regiones = request.values.getlist("region")
    if not regiones:
        regiones = ["Norte", "Centro", "Sur"]

    df_sub = dice_subset(DF, anios=years, regiones=regiones)

    pivot = pd.pivot_table(df_sub, values="Ventas", index="Producto", columns="Trimestre", aggfunc="sum", fill_value=0)
    html_table = pivot.to_html(classes="table table-sm table-striped", border=0)

    graphs = []
    for prod in pivot.index:
        fig = go.Figure()
        fig.add_trace(go.Bar(x=[str(c) for c in pivot.columns], y=pivot.loc[prod].values, name=str(prod)))
        fig.update_layout(title=f"Producto {prod} - Ventas por Trimestre (Años: {','.join(map(str,years))})", margin=dict(t=40))
        graphs.append(fig.to_plotly_json())

    return render_template("section.html", years=years, regiones=regiones, table_html=html_table, graphs=graphs)

@app.route("/cube")
def cube():
    pivot = cubo_base(DF)
    html_table = pivot.to_html(classes="table table-sm table-bordered", border=0)
    return render_template("cube.html", table_html=html_table)

@app.route("/cell", methods=["GET","POST"])
def cell():
    producto = request.values.get("producto", "A")
    region = request.values.get("region", "Norte")
    try:
        year = int(request.values.get("year", 2024))
    except:
        year = 2024
    try:
        trimestre = int(request.values.get("trimestre", 1))
    except:
        trimestre = 1

    df_cell = DF[
        (DF["Producto"] == producto) &
        (DF["Región"] == region) &
        (DF["Año"] == year) &
        (DF["Trimestre"] == trimestre)
    ].copy()

    html_table = df_cell.to_html(classes="table table-sm table-striped", border=0, index=False)
    sum_ventas = float(df_cell["Ventas"].sum())
    sum_cantidad = int(df_cell["Cantidad"].sum()) if not df_cell.empty else 0

    return render_template("cell.html", producto=producto, region=region, year=year, trimestre=trimestre,
                           table_html=html_table, sum_ventas=sum_ventas, sum_cantidad=sum_cantidad)

@app.route("/api/cell_data")
def api_cell_data():
    producto = request.args.get("producto")
    region = request.args.get("region")
    year = int(request.args.get("year"))
    trimestre = int(request.args.get("trimestre"))
    df_cell = DF[(DF["Producto"]==producto) & (DF["Región"]==region) & (DF["Año"]==year) & (DF["Trimestre"]==trimestre)]
    rows = df_cell.to_dict(orient="records")
    totals = {"Ventas": float(df_cell["Ventas"].sum()), "Cantidad": int(df_cell["Cantidad"].sum()) if not df_cell.empty else 0}
    return jsonify({"rows": rows, "totals": totals})

if __name__ == "__main__":
    # Ejecutar desde la carpeta raíz del proyecto:
    # python app.py
    app.run(debug=True)
