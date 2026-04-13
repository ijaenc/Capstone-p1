import pandas as pd


def build_orders_summary(df: pd.DataFrame) -> pd.DataFrame:
    resumen = (
        df.groupby("Número de Orden")
        .agg(
            peso_total_kg=("Peso_total_kg", "sum"),
            volumen_total_m3=("Volumen_total_m3", "sum"),
            lineas=("SKU", "count"),
            unidades=("Cantidad", "sum"),
            skus_distintos=("SKU", "nunique")
        )
        .reset_index()
    )

    resumen = resumen.rename(columns={"Número de Orden": "numero_orden"})
    return resumen