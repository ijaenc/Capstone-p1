import pandas as pd


def build_clients_by_node(pedidos_modelo: pd.DataFrame) -> pd.DataFrame:
    clientes = (
        pedidos_modelo.groupby("node_id")
        .agg(
            peso_total_kg=("peso_total_kg", "sum"),
            volumen_total_m3=("volumen_total_m3", "sum"),
            n_pedidos=("numero_orden", "count"),
            lat=("lat", "first"),
            lon=("lon", "first"),
            direccion_normalizada=("direccion_normalizada", "first"),
        )
        .reset_index()
    )

    return clientes 