from funciones.generarDatos import generar_dataset
from funciones.crearCubo import *
from funciones.operacionesCubo import *

if __name__ == "__main__":
    df = generar_dataset()
    print("\n=== Datos crudos")
    print(df)

    # Cubo base
    cubo = cubo_base(df)
    print("\n=== Cubo base (Producto x Región x Año/Trimestre) ===")
    print(cubo)

    # Slice
    s2024 = slice_por_anio(df, 2024)
    print("\n=== Slice: Año = 2024 (primeras filas) ===")
    print(s2024.head())

    # Dice
    d_subset = dice_subset(df,
                           anios=[2024, 2025],
                           regiones=["Norte", "Sur"],
                           productos=["A", "B"])
    print("\n=== Dice: Años 2024-2025, Regiones Norte/Sur, Productos A/B (primeras filas) ===")
    print(d_subset.head())

    # Roll-up
    ru_anio = rollup_por_anio(df)
    ru_anio_trim = rollup_por_anio_trimestre(df)
    print("\n=== Roll-up: Ventas totales por Año ===")
    print(ru_anio)
    print("\n=== Roll-up: Ventas por Año x Trimestre ===")
    print(ru_anio_trim)

    # Drill-down
    dd = drilldown_producto_region(df, producto="A", region="Norte")
    print("\n=== Drill-down: Producto A, Región Norte (Año -> Trimestre -> Mes) ===")
    print(dd.head(15))

    # Pivot (rotación)
    piv = pivot_anio_region(df)
    print("\n=== Pivot: Año x Región (sum de Ventas) ===")
    print(piv)

    # Múltiples medidas
    mm = pivot_multimedidas(df)
    print("\n=== Pivot con múltiples medidas (Ventas y Cantidad) ===")
    print(mm)

