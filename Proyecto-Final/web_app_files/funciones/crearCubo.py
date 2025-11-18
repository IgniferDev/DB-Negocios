import pandas as pd

def cubo_base(df: pd.DataFrame) -> pd.DataFrame:
    return pd.pivot_table(
        df,
        values="Ventas",
        index=["Producto", "Región"],
        columns=["Año", "Trimestre"],
        aggfunc="sum",
        margins=True,
        margins_name="Total"
    )

def pivot_multimedidas(df: pd.DataFrame) -> pd.DataFrame:
    return pd.pivot_table(
        df,
        values=["Ventas", "Cantidad"],
        index=["Producto", "Región"],
        columns=["Año"],
        aggfunc={"Ventas": "sum", "Cantidad": "sum"},
        margins=True
    )
