import pandas as pd


def normalize_address(addr: str) -> str:
    if pd.isna(addr):
        return None
    addr = str(addr).strip().upper()
    addr = " ".join(addr.split())
    return addr


def extract_nodes_from_matrix(matriz: pd.DataFrame) -> pd.DataFrame:
    direcciones = pd.Series(matriz.index.astype(str)).drop_duplicates().reset_index(drop=True)

    nodes = pd.DataFrame({
        "node_id": range(len(direcciones)),
        "direccion_original": direcciones
    })

    nodes["direccion_normalizada"] = nodes["direccion_original"].apply(normalize_address)
    return nodes


def merge_nodes_with_geo(nodes: pd.DataFrame, geo: pd.DataFrame) -> pd.DataFrame:
    geo = geo.copy()
    geo["direccion_normalizada"] = geo["direccion_normalizada"].astype(str).str.strip().str.upper()
    geo["direccion_normalizada"] = geo["direccion_normalizada"].str.split().str.join(" ")

    merged = nodes.merge(
        geo,
        on="direccion_normalizada",
        how="left"
    )

    return merged