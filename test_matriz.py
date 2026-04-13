import pandas as pd
import numpy as np

# === RUTAS ===
ruta_matriz = r"C:\Users\ijaen\OneDrive\Documents\GitHub\Capstone\data\processed\matriz_distancias_nodos_master.xlsx"
ruta_nodos = r"C:\Users\ijaen\OneDrive\Documents\GitHub\Capstone\data\processed\nodos_master.csv"

# === CARGA ===
matriz = pd.read_excel(ruta_matriz, index_col=0)
nodos = pd.read_csv(ruta_nodos)

# === NORMALIZAR IDS ===
matriz.index = matriz.index.astype(int)
matriz.columns = matriz.columns.astype(int)
nodos["node_id"] = nodos["node_id"].astype(int)

# === VALIDACIONES BÁSICAS ===
print("Shape matriz:", matriz.shape)
print("Shape nodos:", nodos.shape)

print("Matriz cuadrada:", matriz.shape[0] == matriz.shape[1])
print("NaN totales:", matriz.isna().sum().sum())
print("Negativos totales:", (matriz < 0).sum().sum())
print("Diagonal en 0:", np.all(np.diag(matriz.values) == 0))
print("Índices = columnas:", np.array_equal(matriz.index.values, matriz.columns.values))

# === DUPLICADOS ===
print("Duplicados en índice matriz:", matriz.index.duplicated().sum())
print("Duplicados en columnas matriz:", matriz.columns.duplicated().sum())
print("Duplicados en node_id nodos:", nodos["node_id"].duplicated().sum())

# === CRUCE ENTRE MATRIZ Y NODOS ===
ids_matriz = set(matriz.index)
ids_nodos = set(nodos["node_id"])

faltan_en_nodos = ids_matriz - ids_nodos
faltan_en_matriz = ids_nodos - ids_matriz

print("IDs en matriz pero no en nodos:", len(faltan_en_nodos))
print("IDs en nodos pero no en matriz:", len(faltan_en_matriz))

if len(faltan_en_nodos) > 0:
    print("Ejemplo faltan en nodos:", list(sorted(faltan_en_nodos))[:10])

if len(faltan_en_matriz) > 0:
    print("Ejemplo faltan en matriz:", list(sorted(faltan_en_matriz))[:10])

# === MUESTRA ===
print("\nPrimeras filas matriz:")
print(matriz.iloc[:5, :5])

print("\nPrimeros nodos:")
print(nodos.head())

# === GUARDAR VERSIÓN RÁPIDA ===
matriz.to_pickle("matriz_distancias_nodos_master.pkl")
print("\nArchivo .pkl guardado correctamente")