import os
import requests
import pandas as pd
import folium


# =========================
# CONFIGURACIÓN
# =========================
ruta_visitas = r"C:\Users\ijaen\OneDrive\Documents\GitHub\Capstone\salidas\clusters_visitas_detalle_06.csv"
carpeta_salida = r"C:\Users\ijaen\OneDrive\Documents\GitHub\Capstone\salidas\mapas_por_dia"
os.makedirs(carpeta_salida, exist_ok=True)

# Día a visualizar
fecha_objetivo = "2026-12-10"


# =========================
# FUNCIONES
# =========================
def construir_url_osrm(coords):
    coord_str = ";".join([f"{lon},{lat}" for lon, lat in coords])
    return (
        f"http://router.project-osrm.org/route/v1/driving/{coord_str}"
        f"?overview=full&geometries=geojson"
    )


def obtener_geometria_osrm(coords):
    url = construir_url_osrm(coords)
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    data = resp.json()

    if data.get("code") != "Ok":
        raise ValueError(f"OSRM no devolvió Ok: {data}")

    ruta = data["routes"][0]
    geometry = ruta["geometry"]["coordinates"]
    distance_m = ruta["distance"]
    duration_s = ruta["duration"]

    return geometry, distance_m, duration_s


def color_por_ventana_y_indice(ventana, i):
    colores_am = [
        "blue", "green", "cadetblue", "darkblue", "lightblue", "lightgreen", "beige"
    ]
    colores_pm = [
        "purple", "orange", "darkred", "pink", "gray", "black", "lightred"
    ]

    if ventana == "AM":
        return colores_am[i % len(colores_am)]
    return colores_pm[i % len(colores_pm)]


# =========================
# MAIN
# =========================
def main():
    df = pd.read_csv(ruta_visitas)

    # Tipos
    df["ruta_id"] = df["ruta_id"].astype(int)
    df["orden_visita"] = df["orden_visita"].astype(int)
    df["node_id"] = df["node_id"].astype(int)
    df["lat"] = df["lat"].astype(float)
    df["lon"] = df["lon"].astype(float)
    df["cluster_id_dia"] = df["cluster_id_dia"].astype(str).str.strip()
    df["fecha"] = df["fecha"].astype(str).str.strip()
    df["ventana"] = df["ventana"].astype(str).str.upper().str.strip()

    df = df[df["fecha"] == fecha_objetivo].copy()

    if df.empty:
        raise ValueError(f"No hay datos para fecha={fecha_objetivo}")

    lat_centro = df["lat"].mean()
    lon_centro = df["lon"].mean()

    mapa = folium.Map(location=[lat_centro, lon_centro], zoom_start=11)

    # Grupo aparte para depósitos
    grupo_depositos = folium.FeatureGroup(name="Depósitos", show=True)
    depositos_agregados = set()

    grupos_ruta = df.groupby(["ventana", "cluster_id_dia", "ruta_id"], dropna=False)

    resumen = []
    idx_am = 0
    idx_pm = 0

    for (ventana, cluster_id, ruta_id), df_ruta in grupos_ruta:
        df_ruta = df_ruta.sort_values("orden_visita").copy()

        if df_ruta[["lat", "lon"]].isna().any().any():
            print(f"Saltando ruta {ruta_id}: coordenadas faltantes.")
            continue

        coords = list(zip(df_ruta["lon"], df_ruta["lat"]))

        if len(coords) < 2:
            print(f"Saltando ruta {ruta_id}: menos de 2 puntos.")
            continue

        try:
            geometry, distance_m, duration_s = obtener_geometria_osrm(coords)
        except Exception as e:
            print(f"Error OSRM en ruta {ruta_id}: {e}")
            continue

        if ventana == "AM":
            color = color_por_ventana_y_indice("AM", idx_am)
            idx_am += 1
        else:
            color = color_por_ventana_y_indice("PM", idx_pm)
            idx_pm += 1

        nombre_capa = f"{ventana} | Cluster {cluster_id} | Ruta {ruta_id}"

        # show=False para que el mapa abra limpio y tú actives las rutas que quieras
        capa_ruta = folium.FeatureGroup(name=nombre_capa, show=False)

        # Línea de la ruta
        folium.PolyLine(
            locations=[(lat, lon) for lon, lat in geometry],
            weight=4,
            opacity=0.85,
            color=color,
            tooltip=nombre_capa
        ).add_to(capa_ruta)

        # Puntos de visitas
        for _, row in df_ruta.iterrows():
            texto = (
                f"Fecha: {fecha_objetivo}<br>"
                f"Ventana: {ventana}<br>"
                f"Cluster: {cluster_id}<br>"
                f"Ruta: {ruta_id}<br>"
                f"Orden: {row['orden_visita']}<br>"
                f"Node ID: {row['node_id']}<br>"
                f"Dirección: {row.get('direccion_normalizada', '')}"
            )

            if row["es_deposito"] == 1:
                dep_key = (row["node_id"], row["lat"], row["lon"])

                if dep_key not in depositos_agregados:
                    folium.CircleMarker(
                        location=[row["lat"], row["lon"]],
                        radius=5,
                        color="red",
                        fill=True,
                        fill_color="red",
                        fill_opacity=1.0,
                        popup=folium.Popup(f"Depósito<br>Node ID: {row['node_id']}", max_width=250)
                    ).add_to(grupo_depositos)
                    depositos_agregados.add(dep_key)

                folium.CircleMarker(
                    location=[row["lat"], row["lon"]],
                    radius=5,
                    color="red",
                    fill=True,
                    fill_color="red",
                    fill_opacity=1.0,
                    popup=folium.Popup(texto, max_width=300)
                ).add_to(capa_ruta)

            else:
                folium.CircleMarker(
                    location=[row["lat"], row["lon"]],
                    radius=3,
                    color=color,
                    fill=True,
                    fill_color=color,
                    fill_opacity=0.9,
                    popup=folium.Popup(texto, max_width=300)
                ).add_to(capa_ruta)

        capa_ruta.add_to(mapa)

        resumen.append({
            "fecha": fecha_objetivo,
            "ventana": ventana,
            "cluster_id_dia": cluster_id,
            "ruta_id": ruta_id,
            "distancia_osrm_km": round(distance_m / 1000, 3),
            "duracion_osrm_h": round(duration_s / 3600, 3),
            "nombre_capa": nombre_capa,
        })

    grupo_depositos.add_to(mapa)

    # Selector de capas
    folium.LayerControl(collapsed=False).add_to(mapa)

    ruta_html = os.path.join(carpeta_salida, f"mapa_interactivo_{fecha_objetivo}.html")
    mapa.save(ruta_html)

    pd.DataFrame(resumen).to_csv(
        os.path.join(carpeta_salida, f"resumen_interactivo_{fecha_objetivo}.csv"),
        index=False,
        encoding="utf-8-sig"
    )

    print("Mapa interactivo guardado en:")
    print(ruta_html)


if __name__ == "__main__":
    main()