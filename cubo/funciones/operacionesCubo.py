import pandas as pd

def slice_por_anio(df: pd.DataFrame, anio: int) -> pd.DataFrame:
    return df.loc[df["Año"] == anio].copy()

def dice_subset(df: pd.DataFrame,
                anios=None, regiones=None, productos=None, canales=None) -> pd.DataFrame:
    m = pd.Series([True] * len(df))
    if anios is not None:
        m &= df["Año"].isin(anios)
    if regiones is not None:
        m &= df["Región"].isin(regiones)
    if productos is not None:
        m &= df["Producto"].isin(productos)
    if canales is not None:
        m &= df["Canal"].isin(canales)
    return df.loc[m].copy()

def rollup_por_anio(df: pd.DataFrame) -> pd.DataFrame:
    return df.groupby("Año", as_index=False)["Ventas"].sum()

def rollup_por_anio_trimestre(df: pd.DataFrame) -> pd.DataFrame:
    g = df.groupby(["Año", "Trimestre"])["Ventas"].sum()
    return g.unstack("Trimestre").fillna(0)

def drilldown_producto_region(df: pd.DataFrame, producto: str, region: str) -> pd.DataFrame:
    f = (df["Producto"] == producto) & (df["Región"] == region)
    out = (df.loc[f]
             .groupby(["Año", "Trimestre", "Mes"], as_index=False)["Ventas"]
             .sum()
             .sort_values(["Año", "Trimestre", "Mes"]))
    return out

def pivot_anio_region(df: pd.DataFrame) -> pd.DataFrame:
    return pd.pivot_table(
        df, values="Ventas", index="Año", columns="Región",
        aggfunc="sum", margins=True
    )