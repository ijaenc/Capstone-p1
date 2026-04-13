import pandas as pd


def build_solver_nodes_and_matrix_from_master(
    ruta_nodos: str,
    ruta_matriz: str,
    node_ids_a_usar: list[int]
):
    """
    Carga nodos y matriz maestra, filtra por los node_id indicados
    y devuelve:
    - nodos_solver: dataframe de nodos a usar
    - matriz_solver: submatriz cuadrada de distancias
    """

    # Cargar nodos
    nodos = pd.read_csv(ruta_nodos)
    nodos["node_id"] = nodos["node_id"].astype(int)

    # Cargar matriz
    if ruta_matriz.endswith(".pkl"):
        matriz = pd.read_pickle(ruta_matriz)
    else:
        matriz = pd.read_excel(ruta_matriz, index_col=0)

    matriz.index = matriz.index.astype(int)
    matriz.columns = matriz.columns.astype(int)

    # Limpiar y ordenar ids
    node_ids_a_usar = [int(x) for x in node_ids_a_usar]
    node_ids_a_usar = list(dict.fromkeys(node_ids_a_usar))  # quita duplicados preservando orden

    # Validar que existan
    faltantes_nodos = set(node_ids_a_usar) - set(nodos["node_id"])
    faltantes_matriz = set(node_ids_a_usar) - set(matriz.index)

    if faltantes_nodos:
        raise ValueError(f"Estos node_id no existen en nodos_master: {sorted(faltantes_nodos)}")

    if faltantes_matriz:
        raise ValueError(f"Estos node_id no existen en la matriz: {sorted(faltantes_matriz)}")

    # Filtrar nodos
    nodos_solver = nodos[nodos["node_id"].isin(node_ids_a_usar)].copy()

    # Mantener el mismo orden que node_ids_a_usar
    nodos_solver["orden_tmp"] = nodos_solver["node_id"].map({nid: i for i, nid in enumerate(node_ids_a_usar)})
    nodos_solver = nodos_solver.sort_values("orden_tmp").drop(columns="orden_tmp").reset_index(drop=True)

    # Filtrar submatriz en ese mismo orden
    matriz_solver = matriz.loc[node_ids_a_usar, node_ids_a_usar].copy()

    return nodos_solver, matriz_solver
