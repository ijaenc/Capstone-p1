from src.build_solver_inputs_from_master import build_solver_nodes_and_matrix_from_master
import pandas as pd

ruta_nodos = r"C:\Users\ijaen\OneDrive\Documents\GitHub\Capstone\data\processed\nodos_master.csv"
ruta_matriz = r"C:\Users\ijaen\OneDrive\Documents\GitHub\Capstone\matriz_distancias_nodos_master.pkl"

# Cargar nodos para obtener depósito y algunos clientes
nodos = pd.read_csv(ruta_nodos)
nodos["node_id"] = nodos["node_id"].astype(int)

deposito_id = int(nodos.loc[nodos["tipo"] == "deposito", "node_id"].iloc[0])
clientes_ids = nodos.loc[nodos["tipo"] == "cliente", "node_id"].head(20).tolist()

node_ids_a_usar = [deposito_id] + clientes_ids

nodos_solver, matriz_solver = build_solver_nodes_and_matrix_from_master(
    ruta_nodos=ruta_nodos,
    ruta_matriz=ruta_matriz,
    node_ids_a_usar=node_ids_a_usar
)

print("Node IDs usados:", node_ids_a_usar)
print("Shape nodos_solver:", nodos_solver.shape)
print("Shape matriz_solver:", matriz_solver.shape)

print("\nNodos solver:")
print(nodos_solver[["node_id", "tipo"]].head(10))

print("\nSubmatriz:")
print(matriz_solver.iloc[:5, :5])
