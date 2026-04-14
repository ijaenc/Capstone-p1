import os
import json
import requests
import pandas as pd
import folium


# =========================
# CONFIGURACIÓN
# =========================
ruta_visitas = r"C:\Users\ijaen\OneDrive\Documents\GitHub\Capstone\salidas\clusters_visitas_detalle_06.csv"
carpeta_salida = r"C:\Users\ijaen\OneDrive\Documents\GitHub\Capstone\salidas\mapas"
os.makedirs(carpeta_salida, exist_ok=True)

# Filtros opcionales
fecha_objetivo = "2026-12-10"      # ejemplo: "2026-12-02"
ventana_objetivo = "AM"    # ejemplo: "AM"
cluster_objetivo =  "1"    # ejemplo: "1"

# Si quieres solo una ruta específica, usa esto
ruta_objetivo = None        # ejemplo: 1


# =========================
# FUNCIONES
# =========================
def construir_url_osrm(coords):
    """
    coords: lista de tuplas (lon, lat)
    """
    coord_str = ";".join([f"{lon},{lat}" for lon, lat in coords])
    url = (
        f"http://router.project-osrm.org/route/v1/driving/{coord_str}"
        f"?overview=full&geometries=geojson"
    )
    return url


def obtener_geometria_osrm(coords):
    url = construir_url_osrm(coords)
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    data = resp.json()

    if data.get("code") != "Ok":
        raise ValueError(f"OSRM no devolvió Ok: {data}")

    ruta = data["routes"][0]
    geometry = ruta["geometry"]["coordinates"]   # lista [lon, lat]
    distance_m = ruta["distance"]
    duration_s = ruta["duration"]

    return geometry, distance_m, duration_s


def crear_mapa_ruta(df_ruta, geometry, titulo):
    """
    df_ruta: visitas de una ruta ordenadas
    geometry: coordenadas devueltas por OSRM
    """
    lat_centro = df_ruta["lat"].mean()
    lon_centro = df_ruta["lon"].mean()

    m = folium.Map(location=[lat_centro, lon_centro], zoom_start=11)

    # Línea de ruta OSRM
    folium.PolyLine(
        locations=[(lat, lon) for lon, lat in geometry],
        weight=5,
        opacity=0.8,
        tooltip=titulo
    ).add_to(m)

    # Marcadores de visitas
    for _, row in df_ruta.iterrows():
        texto = (
            f"Ruta: {row['ruta_id']}<br>"
            f"Orden: {row['orden_visita']}<br>"
            f"Node ID: {row['node_id']}<br>"
            f"Tipo: {row.get('tipo', '')}<br>"
            f"Dirección: {row.get('direccion_normalizada', '')}"
        )

        color = "red" if row["es_deposito"] == 1 else "blue"

        folium.Marker(
            location=[row["lat"], row["lon"]],
            popup=folium.Popup(texto, max_width=300),
            icon=folium.Icon(color=color)
        ).add_to(m)

    return m


# =========================
# MAIN
# =========================
def main():
    df = pd.read_csv(ruta_visitas)

    # Limpiar tipos
    df["ruta_id"] = df["ruta_id"].astype(int)
    df["orden_visita"] = df["orden_visita"].astype(int)
    df["node_id"] = df["node_id"].astype(int)
    df["lat"] = df["lat"].astype(float)
    df["lon"] = df["lon"].astype(float)
    df["cluster_id_dia"] = df["cluster_id_dia"].astype(str)

    if "fecha" in df.columns:
        df["fecha"] = df["fecha"].astype(str)

    if "ventana" in df.columns:
        df["ventana"] = df["ventana"].astype(str).str.upper().str.strip()

    # Filtros opcionales
    if fecha_objetivo is not None:
        df = df[df["fecha"] == fecha_objetivo].copy()

    if ventana_objetivo is not None:
        df = df[df["ventana"] == ventana_objetivo].copy()

    if cluster_objetivo is not None:
        df = df[df["cluster_id_dia"] == str(cluster_objetivo)].copy()

    if ruta_objetivo is not None:
        df = df[df["ruta_id"] == int(ruta_objetivo)].copy()

    if df.empty:
        raise ValueError("No quedaron filas después de aplicar filtros.")

    grupos = df.groupby(["fecha", "ventana", "cluster_id_dia", "ruta_id"], dropna=False)

    resumen_osrm = []

    for (fecha, ventana, cluster_id, ruta_id), df_ruta in grupos:
        df_ruta = df_ruta.sort_values("orden_visita").copy()

        # Validar coordenadas
        if df_ruta[["lat", "lon"]].isna().any().any():
            print(f"Saltando ruta {ruta_id}: hay coordenadas faltantes.")
            continue

        coords = list(zip(df_ruta["lon"], df_ruta["lat"]))

        # OSRM necesita al menos 2 puntos
        if len(coords) < 2:
            print(f"Saltando ruta {ruta_id}: menos de 2 puntos.")
            continue

        try:
            geometry, distance_m, duration_s = obtener_geometria_osrm(coords)
        except Exception as e:
            print(f"Error en OSRM para ruta {ruta_id}: {e}")
            continue

        titulo = f"{fecha} | {ventana} | cluster {cluster_id} | ruta {ruta_id}"
        mapa = crear_mapa_ruta(df_ruta, geometry, titulo)

        nombre_html = f"mapa_{fecha}_{ventana}_cluster{cluster_id}_ruta{ruta_id}.html"
        nombre_html = nombre_html.replace(":", "-").replace("/", "-")
        ruta_html = os.path.join(carpeta_salida, nombre_html)

        mapa.save(ruta_html)

        resumen_osrm.append({
            "fecha": fecha,
            "ventana": ventana,
            "cluster_id_dia": cluster_id,
            "ruta_id": ruta_id,
            "n_visitas": len(df_ruta),
            "distancia_osrm_km": round(distance_m / 1000, 3),
            "duracion_osrm_h": round(duration_s / 3600, 3),
            "archivo_mapa": ruta_html,
        })

        print(f"Mapa guardado: {ruta_html}")

    if resumen_osrm:
        df_resumen_osrm = pd.DataFrame(resumen_osrm)
        ruta_resumen = os.path.join(carpeta_salida, "resumen_osrm_mapas.csv")
        df_resumen_osrm.to_csv(ruta_resumen, index=False, encoding="utf-8-sig")
        print("\nResumen OSRM guardado en:")
        print(ruta_resumen)
    else:
        print("\nNo se generaron mapas.")


if __name__ == "__main__":
    main()