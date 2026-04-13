from pathlib import Path
import pandas as pd

from src.config import load_config
from src.load_data import load_matriz
from src.build_nodes import extract_nodes_from_matrix, merge_nodes_with_geo


def main():
    config = load_config()

    matriz = load_matriz(config["paths"]["matriz"])
    geo = pd.read_csv("data/raw/direcciones_geoapify_unido.csv")

    nodes = extract_nodes_from_matrix(matriz)
    nodes_geo = merge_nodes_with_geo(nodes, geo)

    print("Nodos extraídos desde matriz:", len(nodes))
    print("Nodos con coordenadas:", nodes_geo["lat"].notna().sum() if "lat" in nodes_geo.columns else "revisar nombre columna lat")
    print("Nodos sin coordenadas:", nodes_geo["lat"].isna().sum() if "lat" in nodes_geo.columns else "revisar nombre columna lat")

    print("\nColumnas del archivo final:")
    print(nodes_geo.columns.tolist())

    print("\nPrimeras filas:")
    print(nodes_geo.head())

    Path("data/processed").mkdir(parents=True, exist_ok=True)
    nodes_geo.to_csv("data/processed/nodos_master.csv", index=False, encoding="utf-8-sig")


if __name__ == "__main__":
    main()