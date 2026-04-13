import pandas as pd

from src.build_osrm_matrix import (
    build_nodes_for_matrix,
    build_osrm_matrix_in_blocks,
    save_matrices,
)


def main():
    nodes_master = pd.read_csv("data/processed/nodos_master.csv")
    clientes_modelo = pd.read_csv("data/processed/clientes_modelo.csv")

    nodes = build_nodes_for_matrix(nodes_master, clientes_modelo)

    print("Nodos para matriz:", len(nodes))
    print(nodes.head())

    # prueba inicial pequeña
    sample = nodes.head(20).copy()

    dur, dist = build_osrm_matrix_in_blocks(sample, block_size=10)

    print("\nDuración matriz shape:", dur.shape)
    print("Distancia matriz shape:", dist.shape)

    save_matrices(
        dur,
        dist,
        out_dir="data/processed/test_osrm"
    )

    print("\nArchivos exportados en data/processed/test_osrm")


if __name__ == "__main__":
    main()