import time
import pandas as pd
import os 
import re

from src.build_solver_inputs_from_master import build_solver_nodes_and_matrix_from_master
from src.solver_desde_matriz import resolver_desde_matriz, ConfiguracionSolver

def obtener_siguiente_version(carpeta_salida: str, prefijo: str) -> str:
    """
    Busca archivos tipo prefijo_01.csv, prefijo_02.csv, etc.
    y devuelve la siguiente versión disponible como string de 2 dígitos.
    """
    os.makedirs(carpeta_salida, exist_ok=True)

    patron = re.compile(rf"^{re.escape(prefijo)}_(\d+)\.csv$")
    versiones = []

    for nombre in os.listdir(carpeta_salida):
        match = patron.match(nombre)
        if match:
            versiones.append(int(match.group(1)))

    siguiente = 1 if not versiones else max(versiones) + 1
    return f"{siguiente:02d}"

def main():
    ruta_nodos = r"C:\Users\ijaen\OneDrive\Documents\GitHub\Capstone\data\processed\nodos_master.csv"  # cambia a tu ruta local real
    ruta_matriz = r"C:\Users\ijaen\OneDrive\Documents\GitHub\Capstone\matriz_distancias_nodos_master.pkl"
    ruta_pedidos = r"C:\Users\ijaen\OneDrive\Documents\GitHub\Capstone\data\processed\pedidos_modelo.csv"  # cambia a tu ruta local real

    # 1) Leer pedidos del profe
    pedidos = pd.read_csv(ruta_pedidos)

    # 2) Quedarnos solo con pedidos con nodo
    pedidos = pedidos[pedidos["tiene_nodo"] == True].copy()
    pedidos["node_id"] = pedidos["node_id"].astype(int)

    # 3) Agregar por nodo para construir clientes_dia
    clientes_dia = (
        pedidos.groupby("node_id", as_index=False)
        .agg(
            volumen_m3=("volumen_total_m3", "sum"),
            peso_kg=("peso_total_kg", "sum"),
            n_pedidos=("numero_orden", "count")
        )
    )

    # 4) Tiempo de servicio simple
    clientes_dia["servicio_min"] = 10

    # 5) Limitar para pruebas
    N = 1000   # cambia a 200 o 500
    clientes_dia = clientes_dia.head(N).copy()

    print("Clientes/nodos a rutear:", len(clientes_dia))
    print(clientes_dia.head())

    # 6) Cargar nodos maestro
    nodos = pd.read_csv(ruta_nodos)
    nodos["node_id"] = nodos["node_id"].astype(int)

    deposito_id = int(nodos.loc[nodos["tipo"] == "deposito", "node_id"].iloc[0])

    node_ids_a_usar = [deposito_id] + clientes_dia["node_id"].tolist()

    # 7) Construir subinstancia
    nodos_solver, matriz_solver = build_solver_nodes_and_matrix_from_master(
        ruta_nodos=ruta_nodos,
        ruta_matriz=ruta_matriz,
        node_ids_a_usar=node_ids_a_usar
    )

    # 8) Configuración
    config = ConfiguracionSolver(
        capacidad_volumen_m3=45.0,
        capacidad_peso_kg=7500.0,
        jornada_max_horas=10.0,
        servicio_por_cliente_min=10.0,
        velocidad_promedio_kmh=25.0,
    )

    # 9) Resolver
    t0 = time.perf_counter()

    resultado = resolver_desde_matriz(
        nodos_solver=nodos_solver,
        matriz_solver=matriz_solver,
        clientes_dia=clientes_dia[["node_id", "volumen_m3", "peso_kg", "servicio_min"]],
        config=config,
        aplicar_2opt=True,
        time_limit_2opt_seg=0.2,
    )

    t1 = time.perf_counter()

    # 10) Mostrar resultados
    print("\n=== RESULTADO GENERAL ===")
    print("Número de rutas:", resultado["num_rutas"])
    print("Kilómetros totales:", resultado["km_totales"])
    print("Tiempo total solver (seg):", round(t1 - t0, 3))

    print("\n=== PRIMERAS 10 RUTAS ===")
    for r in resultado["resumen_rutas"][:10]:
        print(r)



     # === EXPORTAR RESULTADOS ===
    carpeta_salida = "salidas"
    os.makedirs(carpeta_salida, exist_ok=True)

    version = obtener_siguiente_version(carpeta_salida, "rutas_resumen")

    # 1) Resumen de rutas
    df_rutas = pd.DataFrame(resultado["resumen_rutas"])
    ruta_csv_rutas = os.path.join(carpeta_salida, f"rutas_resumen_{version}.csv")
    df_rutas.to_csv(ruta_csv_rutas, index=False, encoding="utf-8-sig")

    # 2) Secuencia de visitas por ruta
    nodos_export = nodos.copy()
    nodos_export["node_id"] = nodos_export["node_id"].astype(int)

    columnas_nodos = [
        c for c in [
            "node_id",
            "tipo",
            "direccion_original",
            "direccion_normalizada",
            "lat",
            "lon"
        ] if c in nodos_export.columns
    ]

    filas_visitas = []

    for r in resultado["resumen_rutas"]:
        ruta_id = r["ruta_id"]
        ruta_nodos = r["ruta"]

        for orden_visita, node_id in enumerate(ruta_nodos):
            fila = {
                "ruta_id": ruta_id,
                "orden_visita": orden_visita,
                "node_id": int(node_id),
                "es_deposito": 1 if orden_visita == 0 or orden_visita == len(ruta_nodos) - 1 else 0,
            }

            match = nodos_export.loc[nodos_export["node_id"] == int(node_id), columnas_nodos]

            if not match.empty:
                row = match.iloc[0].to_dict()
                fila.update(row)

            filas_visitas.append(fila)

    df_visitas = pd.DataFrame(filas_visitas)
    ruta_csv_visitas = os.path.join(carpeta_salida, f"rutas_visitas_detalle_{version}.csv")
    df_visitas.to_csv(ruta_csv_visitas, index=False, encoding="utf-8-sig")

    print("\nArchivos exportados:")
    print(ruta_csv_rutas)
    print(ruta_csv_visitas)

if __name__ == "__main__":
    main()