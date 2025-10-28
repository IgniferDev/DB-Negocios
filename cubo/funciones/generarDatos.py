import numpy as np
import pandas as pd

def generar_dataset(seed: int = 42) -> pd.DataFrame:
    rng = np.random.default_rng(seed)

    anios = [2023, 2024, 2025]
    trimestres = [1, 2, 3, 4]
    meses_por_trim = {
        1: ["Ene", "Feb", "Mar"],
        2: ["Abr", "May", "Jun"],
        3: ["Jul", "Ago", "Sep"],
        4: ["Oct", "Nov", "Dic"],
    }
    regiones = ["Norte", "Centro", "Sur"]
    canales = ["Tienda", "Online"]
    productos = ["A", "B", "C"]

    rows = []
    for anio in anios:
        for t in trimestres:
            for mes in meses_por_trim[t]:
                for region in regiones:
                    for canal in canales:
                        for prod in productos:
                            base = 1000 + 50*(anio-2023) + 30*t
                            efecto_region = {"Norte": 80, "Centro": 40, "Sur": 20}[region]
                            efecto_canal = {"Tienda": 60, "Online": 30}[canal]
                            efecto_prod = {"A": 70, "B": 40, "C": 10}[prod]
                            ruido = rng.normal(0, 50)
                            ventas = max(0, base + efecto_region + efecto_canal + efecto_prod + ruido)
                            cantidad = max(1, int(ventas // 50) + rng.integers(-3, 4))
                            rows.append([anio, t, mes, region, canal, prod, cantidad, ventas])

    df = pd.DataFrame(rows, columns=[
        "Año", "Trimestre", "Mes", "Región", "Canal", "Producto", "Cantidad", "Ventas"
    ])
    return df