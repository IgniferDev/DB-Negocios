import pandas as pd

def cubo_base(df: pd.DataFrame) -> pd.DataFrame:
    """Cubo: Producto x Región x (Año, Trimestre), medida = sum(Ventas)."""
    return pd.pivot_table(
        df,
        values="Ventas",
        index=["Producto", "Región"],
        columns=["Año", "Trimestre"],
        aggfunc="sum",
        margins=True,           # Totales tipo "ALL"
        margins_name="Total"
    )

def pivot_multimedidas(df: pd.DataFrame) -> pd.DataFrame:
    """
    Pivot con múltiples medidas (Ventas y Cantidad),
    agregando por Producto x Región x Año.
    """
    return pd.pivot_table(
        df,
        values=["Ventas", "Cantidad"],
        index=["Producto", "Región"],
        columns=["Año"],
        aggfunc={"Ventas": "sum", "Cantidad": "sum"},
        margins=True
    )