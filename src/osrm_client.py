import requests


OSRM_BASE_URL = "https://router.project-osrm.org"


def build_coordinates_string(df, lon_col="lon", lat_col="lat"):
    return ";".join(f"{lon},{lat}" for lon, lat in zip(df[lon_col], df[lat_col]))


def get_osrm_table(df, profile="driving", annotations="duration,distance"):
    coords = build_coordinates_string(df)

    url = f"{OSRM_BASE_URL}/table/v1/{profile}/{coords}"
    params = {
        "annotations": annotations
    }

    response = requests.get(url, params=params, timeout=60)
    response.raise_for_status()

    data = response.json()

    if data.get("code") != "Ok":
        raise ValueError(f"OSRM error: {data}")

    return data