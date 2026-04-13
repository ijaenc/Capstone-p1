import os
import re
import time
import pandas as pd

from src.build_solver_inputs_from_master import build_solver_nodes_and_matrix_from_master
from src.solver_desde_matriz import resolver_desde_matriz, ConfiguracionSolver


def obtener_siguiente_version(carpeta_salida: str, prefijo: str) -> str:
    os.makedirs(carpeta_salida, exist_ok=True)
    patron = re.compile(rf"^{re.escape(prefijo)}_(\d+)\.csv$")
    versiones = []

    for nombre in os.listdir(carpeta_salida):
        match = patron.match(nombre)
        if match:
            versiones.append(int(match.group(1)))

    siguiente = 1 if not versiones else max(versiones) + 1
    return f"{siguiente:02d}"


def leer_input_ruteo(ruta_input: str) -> pd.DataFrame:
    if not os.path.exists(ruta_input):
        raise FileNotFoundError(f"No existe el archivo de input de ruteo: {ruta_input}")

    ext = os.path.splitext(ruta_input)[1].lower()

    if ext == ".csv":
        return pd.read_csv(ruta_input)
    if ext in [".xlsx", ".xls"]:
        return pd.read_excel(ruta_input)

    raise ValueError(f"Formato no soportado para input de ruteo: {ext}")


def correr_cluster(
    df_cluster: pd.DataFrame,
    ruta_nodos_csv: str,
    ruta_matriz: str,
    config: ConfiguracionSolver,
    aplicar_2opt: bool = True,
    time_limit_2opt_seg: float | None = 0.2,
):
    deposito_id = int(df_cluster["node_id_deposito"].iloc[0])

    clientes_dia = df_cluster.rename(columns={
        "volumen_total_nodo_dia_m3": "volumen_m3",
        "peso_total_nodo_dia_kg": "peso_kg",
    })[["node_id", "volumen_m3", "peso_kg"]].copy()

    clientes_dia["node_id"] = clientes_dia["node_id"].astype(int)
    clientes_dia["volumen_m3"] = clientes_dia["volumen_m3"].astype(float)
    clientes_dia["peso_kg"] = clientes_dia["peso_kg"].astype(float)

    node_ids_a_usar = [deposito_id] + clientes_dia["node_id"].tolist()

    nodos_solver, matriz_solver = build_solver_nodes_and_matrix_from_master(
        ruta_nodos=ruta_nodos_csv,
        ruta_matriz=ruta_matriz,
        node_ids_a_usar=node_ids_a_usar
    )

    resultado = resolver_desde_matriz(
        nodos_solver=nodos_solver,
        matriz_solver=matriz_solver,
        clientes_dia=clientes_dia,
        config=config,
        aplicar_2opt=aplicar_2opt,
        time_limit_2opt_seg=time_limit_2opt_seg,
    )

    return resultado, clientes_dia, nodos_solver, matriz_solver


def main():
    # =========================
    # RUTAS DE ARCHIVOS
    # =========================
    ruta_nodos_csv = r"C:\Users\ijaen\OneDrive\Documents\GitHub\Capstone\data\processed\nodos_master.csv"
    ruta_matriz = r"C:\Users\ijaen\OneDrive\Documents\GitHub\Capstone\matriz_distancias_nodos_master.pkl"
    ruta_input = r"C:\Users\ijaen\OneDrive\Documents\GitHub\Capstone\data\clusters\input_ruteo_companero.csv"

    print("Existe nodos:", os.path.exists(ruta_nodos_csv))
    print("Existe matriz:", os.path.exists(ruta_matriz))
    print("Existe input:", os.path.exists(ruta_input))

    # =========================
    # LEER ARCHIVOS
    # =========================
    df = leer_input_ruteo(ruta_input)
    nodos = pd.read_csv(ruta_nodos_csv)
    nodos["node_id"] = nodos["node_id"].astype(int)

    # =========================
    # NORMALIZACIÓN
    # =========================
    df["node_id"] = df["node_id"].astype(int)
    df["node_id_deposito"] = df["node_id_deposito"].astype(int)
    df["cluster_id_dia"] = df["cluster_id_dia"].astype(str)
    df["ventana"] = df["ventana"].astype(str).str.upper().str.strip()

    df["Fecha de despacho Solicitada"] = pd.to_datetime(
        df["Fecha de despacho Solicitada"],
        errors="coerce"
    ).dt.strftime("%Y-%m-%d")

    df = df.dropna(subset=[
        "Fecha de despacho Solicitada",
        "ventana",
        "cluster_id_dia",
        "node_id",
        "node_id_deposito",
        "volumen_total_nodo_dia_m3",
        "peso_total_nodo_dia_kg",
    ]).copy()

    if df.empty:
        raise ValueError("El input quedó vacío después de limpiar columnas clave.")

    print("\nShape input:", df.shape)
    print("Columnas input:")
    print(df.columns.tolist())

    # =========================
    # CONFIGURACIÓN SOLVER
    # =========================
    config = ConfiguracionSolver(
        capacidad_volumen_m3=9.0,
        capacidad_peso_kg=3000.0,
        jornada_max_horas=8.0,
        servicio_por_cliente_min=10.0,
        velocidad_promedio_kmh=25.0,
    )

    aplicar_2opt = True
    time_limit_2opt_seg = 1.0

    # =========================
    # SALIDAS
    # =========================
    carpeta_salida = "salidas"
    os.makedirs(carpeta_salida, exist_ok=True)
    version = obtener_siguiente_version(carpeta_salida, "clusters_resumen")

    resumen_global = []
    detalle_visitas_global = []
    resumen_rutas_global = []

    # =========================
    # ITERAR TODOS LOS GRUPOS
    # =========================
    grupos = df.groupby(
        ["Fecha de despacho Solicitada", "ventana", "cluster_id_dia"],
        dropna=False
    )

    t0_total = time.perf_counter()

    for (fecha, ventana, cluster_id), df_cluster in grupos:
        if df_cluster.empty:
            continue

        t0 = time.perf_counter()

        resultado, clientes_dia, nodos_solver, matriz_solver = correr_cluster(
            df_cluster=df_cluster,
            ruta_nodos_csv=ruta_nodos_csv,
            ruta_matriz=ruta_matriz,
            config=config,
            aplicar_2opt=aplicar_2opt,
            time_limit_2opt_seg=time_limit_2opt_seg,
        )

        t1 = time.perf_counter()

        volumen_cluster = round(clientes_dia["volumen_m3"].sum(), 3)
        peso_cluster = round(clientes_dia["peso_kg"].sum(), 3)

        print(f"\n=== FECHA {fecha} | VENTANA {ventana} | CLUSTER {cluster_id} ===")
        print("Nodos:", len(clientes_dia))
        print("Rutas:", resultado["num_rutas"])
        print("Km:", resultado["km_totales"])
        print("Tiempo solver (seg):", round(t1 - t0, 3))

        resumen_global.append({
            "fecha": fecha,
            "ventana": ventana,
            "cluster_id_dia": cluster_id,
            "n_nodos": len(clientes_dia),
            "volumen_total_cluster_m3": volumen_cluster,
            "peso_total_cluster_kg": peso_cluster,
            "num_rutas": resultado["num_rutas"],
            "km_totales": resultado["km_totales"],
            "tiempo_solver_seg": round(t1 - t0, 3),
        })

        for r in resultado["resumen_rutas"]:
            fila_resumen_ruta = {
                "fecha": fecha,
                "ventana": ventana,
                "cluster_id_dia": cluster_id,
                "n_nodos_cluster": len(clientes_dia),
                "volumen_total_cluster_m3": volumen_cluster,
                "peso_total_cluster_kg": peso_cluster,
                "num_rutas_cluster": resultado["num_rutas"],
                "km_totales_cluster": resultado["km_totales"],
                **r
            }
            resumen_rutas_global.append(fila_resumen_ruta)

        columnas_nodos = [
            c for c in [
                "node_id",
                "tipo",
                "direccion_original",
                "direccion_normalizada",
                "lat",
                "lon"
            ] if c in nodos.columns
        ]

        for r in resultado["resumen_rutas"]:
            ruta_id = r["ruta_id"]
            secuencia_nodos_ruta = r["ruta"]

            for orden_visita, node_id in enumerate(secuencia_nodos_ruta):
                fila = {
                    "fecha": fecha,
                    "ventana": ventana,
                    "cluster_id_dia": cluster_id,
                    "ruta_id": ruta_id,
                    "orden_visita": orden_visita,
                    "node_id": int(node_id),
                    "es_deposito": 1 if orden_visita == 0 or orden_visita == len(secuencia_nodos_ruta) - 1 else 0,
                    "km_ruta": r["km_ruta"],
                    "tiempo_total_h": r["tiempo_total_h"],
                    "volumen_ruta_m3": r["volumen_m3"],
                    "peso_ruta_kg": r["peso_kg"],
                }

                match = nodos.loc[nodos["node_id"] == int(node_id), columnas_nodos]
                if not match.empty:
                    fila.update(match.iloc[0].to_dict())

                detalle_visitas_global.append(fila)

    t1_total = time.perf_counter()

    # =========================
    # DATAFRAMES DE SALIDA
    # =========================
    df_resumen_clusters = pd.DataFrame(resumen_global)
    df_resumen_rutas = pd.DataFrame(resumen_rutas_global)
    df_detalle_visitas = pd.DataFrame(detalle_visitas_global)

    resumen_total = pd.DataFrame([{
        "grupos_corridos": len(df_resumen_clusters),
        "rutas_totales": int(df_resumen_rutas.shape[0]) if not df_resumen_rutas.empty else 0,
        "nodos_totales": int(df_resumen_clusters["n_nodos"].sum()) if not df_resumen_clusters.empty else 0,
        "km_totales_global": round(df_resumen_clusters["km_totales"].sum(), 3) if not df_resumen_clusters.empty else 0.0,
        "volumen_total_global_m3": round(df_resumen_clusters["volumen_total_cluster_m3"].sum(), 3) if not df_resumen_clusters.empty else 0.0,
        "peso_total_global_kg": round(df_resumen_clusters["peso_total_cluster_kg"].sum(), 3) if not df_resumen_clusters.empty else 0.0,
        "km_promedio_por_ruta": round(df_resumen_rutas["km_ruta"].mean(), 3) if not df_resumen_rutas.empty else 0.0,
        "clientes_promedio_por_ruta": round(df_resumen_rutas["num_clientes"].mean(), 3) if not df_resumen_rutas.empty else 0.0,
        "utilizacion_vol_promedio_pct": round(df_resumen_rutas["utilizacion_vol_pct"].mean(), 3) if not df_resumen_rutas.empty else 0.0,
        "utilizacion_peso_promedio_pct": round(df_resumen_rutas["utilizacion_peso_pct"].mean(), 3) if not df_resumen_rutas.empty else 0.0,
        "tiempo_total_promedio_h": round(df_resumen_rutas["tiempo_total_h"].mean(), 3) if not df_resumen_rutas.empty else 0.0,
        "tiempo_solver_total_seg": round(t1_total - t0_total, 3),
    }])

    # =========================
    # EXPORTAR CSV
    # =========================
    ruta_resumen_total = os.path.join(carpeta_salida, f"clusters_resumen_total_{version}.csv")
    ruta_resumen_clusters = os.path.join(carpeta_salida, f"clusters_resumen_{version}.csv")
    ruta_resumen_rutas = os.path.join(carpeta_salida, f"clusters_rutas_resumen_{version}.csv")
    ruta_detalle_visitas = os.path.join(carpeta_salida, f"clusters_visitas_detalle_{version}.csv")

    resumen_total.to_csv(ruta_resumen_total, index=False, encoding="utf-8-sig")
    df_resumen_clusters.to_csv(ruta_resumen_clusters, index=False, encoding="utf-8-sig")
    df_resumen_rutas.to_csv(ruta_resumen_rutas, index=False, encoding="utf-8-sig")
    df_detalle_visitas.to_csv(ruta_detalle_visitas, index=False, encoding="utf-8-sig")

    # =========================
    # RESUMEN FINAL
    # =========================
    print("\n=== RESUMEN GLOBAL TOTAL ===")
    print(resumen_total.to_string(index=False))

    print("\n=== RESULTADO GLOBAL ===")
    print("Grupos corridos:", len(df_resumen_clusters))
    print("Tiempo total (seg):", round(t1_total - t0_total, 3))
    print("Archivo resumen total:", ruta_resumen_total)
    print("Archivo resumen grupos:", ruta_resumen_clusters)
    print("Archivo resumen rutas:", ruta_resumen_rutas)
    print("Archivo detalle visitas:", ruta_detalle_visitas)


if __name__ == "__main__":
    main()