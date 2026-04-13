from pathlib import Path
import time
import pandas as pd
import requests


OSRM_BASE_URL = "https://router.project-osrm.org"


def build_coordinates_string(df: pd.DataFrame, lon_col: str = "lon", lat_col: str = "lat") -> str:
    return ";".join(f"{lon},{lat}" for lon, lat in zip(df[lon_col], df[lat_col]))


def osrm_table(df: pd.DataFrame, profile: str = "driving") -> dict:
    coords = build_coordinates_string(df)

    url = f"{OSRM_BASE_URL}/table/v1/{profile}/{coords}"
    params = {"annotations": "duration,distance"}

    response = requests.get(url, params=params, timeout=120)
    response.raise_for_status()

    data = response.json()
    if data.get("code") != "Ok":
        raise ValueError(f"OSRM error: {data}")

    return data


def build_nodes_for_matrix(
    nodes_master: pd.DataFrame,
    clientes_modelo: pd.DataFrame
) -> pd.DataFrame:
    deposito = nodes_master[nodes_master["tipo"] == "deposito"].copy()

    clientes = clientes_modelo.copy()

    # tomar lat/lon/direccion desde nodos_master por node_id
    clientes = clientes.merge(
        nodes_master[["node_id", "lat", "lon", "direccion_normalizada"]],
        on="node_id",
        how="left",
        suffixes=("", "_node")
    )

    clientes["tipo"] = "cliente"

    cols = ["node_id", "direccion_normalizada", "lat", "lon", "tipo"]
    deposito = deposito[cols].copy()
    clientes = clientes[cols].copy()

    nodes = pd.concat([deposito, clientes], ignore_index=True)
    nodes = nodes.drop_duplicates(subset=["node_id"]).reset_index(drop=True)

    return nodes


def build_osrm_matrix_in_blocks(
    nodes: pd.DataFrame,
    block_size: int = 50,
    sleep_seconds: float = 0.2
):
    n = len(nodes)

    duration_matrix = pd.DataFrame(index=nodes["node_id"], columns=nodes["node_id"], dtype=float)
    distance_matrix = pd.DataFrame(index=nodes["node_id"], columns=nodes["node_id"], dtype=float)

    for i_start in range(0, n, block_size):
        i_end = min(i_start + block_size, n)
        origins = nodes.iloc[i_start:i_end].copy()

        for j_start in range(0, n, block_size):
            j_end = min(j_start + block_size, n)
            dests = nodes.iloc[j_start:j_end].copy()

            block = pd.concat([origins, dests], ignore_index=True).reset_index(drop=True)

            n_orig = len(origins)
            n_dest = len(dests)

            sources = ";".join(str(i) for i in range(n_orig))
            destinations = ";".join(str(i) for i in range(n_orig, n_orig + n_dest))

            coords = build_coordinates_string(block)

            url = f"{OSRM_BASE_URL}/table/v1/driving/{coords}"
            params = {
                "annotations": "duration,distance",
                "sources": sources,
                "destinations": destinations,
            }

            print(f"Bloque filas {i_start}:{i_end} vs columnas {j_start}:{j_end}")

            response = requests.get(url, params=params, timeout=120)
            response.raise_for_status()
            data = response.json()

            if data.get("code") != "Ok":
                raise ValueError(f"OSRM error en bloque ({i_start}, {j_start}): {data}")

            durations = data["durations"]
            distances = data["distances"]

            for r, origin_id in enumerate(origins["node_id"].tolist()):
                for c, dest_id in enumerate(dests["node_id"].tolist()):
                    duration_matrix.loc[origin_id, dest_id] = durations[r][c]
                    distance_matrix.loc[origin_id, dest_id] = distances[r][c]

            time.sleep(sleep_seconds)

    return duration_matrix, distance_matrix


def save_matrices(
    duration_matrix: pd.DataFrame,
    distance_matrix: pd.DataFrame,
    out_dir: str = "data/processed"
):
    Path(out_dir).mkdir(parents=True, exist_ok=True)

    duration_matrix.to_csv(f"{out_dir}/matriz_osrm_duracion_seg.csv", encoding="utf-8-sig")
    distance_matrix.to_csv(f"{out_dir}/matriz_osrm_distancia_m.csv", encoding="utf-8-sig")