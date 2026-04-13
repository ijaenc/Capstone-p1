import pandas as pd


DEPOSITO_DIRECCION = "Poeta Pedro Prado 1556, Santiago, Quinta Normal, Región Metropolitana, Chile"
DEPOSITO_LAT = -33.42748443128771
DEPOSITO_LON = -70.68728138816309


def normalize_address(addr: str) -> str:
    if pd.isna(addr):
        return None

    addr = str(addr).strip().upper()
    addr = " ".join(addr.split())
    return addr


def build_nodes_master(matriz: pd.DataFrame, geo: pd.DataFrame) -> pd.DataFrame:

    direcciones = pd.Series(matriz.index.astype(str)).drop_duplicates().reset_index(drop=True)

    clientes = pd.DataFrame({
        "direccion_original": direcciones
    })

    clientes["direccion_normalizada"] = clientes["direccion_original"].apply(normalize_address)

    geo = geo.copy()
    geo["direccion_normalizada"] = geo["direccion_normalizada"].astype(str)
    geo["direccion_normalizada"] = geo["direccion_normalizada"].str.strip().str.upper()
    geo["direccion_normalizada"] = geo["direccion_normalizada"].str.split().str.join(" ")

    geo = geo.drop_duplicates(subset=["direccion_normalizada"])

    clientes = clientes.merge(
        geo[
            [
                "direccion_normalizada",
                "lat",
                "lon",
                "estado",
                "calidad_match"
            ]
        ],
        on="direccion_normalizada",
        how="left"
    )

    clientes["tipo"] = "cliente"

    deposito = pd.DataFrame([{
        "direccion_original": DEPOSITO_DIRECCION,
        "direccion_normalizada": normalize_address(DEPOSITO_DIRECCION),
        "lat": DEPOSITO_LAT,
        "lon": DEPOSITO_LON,
        "estado": "manual",
        "calidad_match": "exacta",
        "tipo": "deposito"
    }])

    nodes = pd.concat([deposito, clientes], ignore_index=True)

    nodes.insert(0, "node_id", range(len(nodes)))

    nodes["tiene_coordenadas"] = (
        nodes["lat"].notna() &
        nodes["lon"].notna()
    )

    return nodes