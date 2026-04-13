from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Any, Tuple

import pandas as pd
import numpy as np
import math
import time

@dataclass
class ConfiguracionSolver:
    capacidad_volumen_m3: float = 45.0
    capacidad_peso_kg: float = 7500.0

    # Jornada diaria
    jornada_max_horas: float = 8.0

    # Descansos diarios: 1h almuerzo + 30 min AM + 30 min PM = 2h
    descanso_total_horas: float = 2.0

    # Tiempo de servicio por cliente
    servicio_por_cliente_min: float = 15.0

    # Conversión distancia -> tiempo si no tienes matriz de tiempos
    velocidad_promedio_kmh: float = 25.0

    # Columna identificadora del depósito en nodos_solver
    valor_tipo_deposito: str = "deposito"


def _validar_columnas(df: pd.DataFrame, columnas: List[str], nombre_df: str) -> None:
    faltantes = [c for c in columnas if c not in df.columns]
    if faltantes:
        raise ValueError(f"Faltan columnas en {nombre_df}: {faltantes}")


def _construir_demanda_por_nodo(
    clientes_dia: pd.DataFrame,
    servicio_por_cliente_min_default: float
) -> pd.DataFrame:
    """
    Espera un DataFrame con al menos:
    - node_id
    - volumen_m3
    - peso_kg

    Opcional:
    - servicio_min
    """
    _validar_columnas(clientes_dia, ["node_id", "volumen_m3", "peso_kg"], "clientes_dia")

    df = clientes_dia.copy()
    df["node_id"] = df["node_id"].astype(int)
    df["volumen_m3"] = df["volumen_m3"].astype(float)
    df["peso_kg"] = df["peso_kg"].astype(float)

    if "servicio_min" not in df.columns:
        df["servicio_min"] = servicio_por_cliente_min_default
    else:
        df["servicio_min"] = df["servicio_min"].fillna(servicio_por_cliente_min_default).astype(float)

    demanda = (
        df.groupby("node_id", as_index=False)
        .agg(
            volumen_m3=("volumen_m3", "sum"),
            peso_kg=("peso_kg", "sum"),
            servicio_min=("servicio_min", "sum"),
        )
    )

    return demanda


def _distancia_km(matriz_solver: pd.DataFrame, i: int, j: int) -> float:
    return float(matriz_solver.loc[i, j])

def calcular_descanso_horas(tiempo_manejo_h):
    """
    Cada 2 horas de manejo => 30 min descanso
    """
    bloques = math.floor(tiempo_manejo_h / 2)
    return bloques * 0.5

def _tiempo_viaje_horas_desde_distancia(
    matriz_solver: pd.DataFrame,
    i: int,
    j: int,
    velocidad_promedio_kmh: float
) -> float:
    dist_km = _distancia_km(matriz_solver, i, j)
    return dist_km / velocidad_promedio_kmh


def _evaluar_agregar_cliente(
    ruta_actual: List[int],
    cliente_candidato: int,
    deposito_id: int,
    matriz_solver: pd.DataFrame,
    demanda: Dict[int, Dict[str, float]],
    config: ConfiguracionSolver,
    volumen_actual: float,
    peso_actual: float,
    tiempo_viaje_actual_h: float,
    tiempo_servicio_actual_h: float,
) -> Tuple[bool, Dict[str, float]]:
    """
    Evalúa si agregar un cliente mantiene factibilidad.
    """

    vol_nuevo = volumen_actual + demanda[cliente_candidato]["volumen_m3"]
    peso_nuevo = peso_actual + demanda[cliente_candidato]["peso_kg"]

    if vol_nuevo > config.capacidad_volumen_m3:
        return False, {}

    if peso_nuevo > config.capacidad_peso_kg:
        return False, {}

    ultimo = ruta_actual[-1]

    # Tiempo adicional por ir al candidato
    add_viaje_h = _tiempo_viaje_horas_desde_distancia(
        matriz_solver,
        ultimo,
        cliente_candidato,
        config.velocidad_promedio_kmh
    )

    # Tiempo de servicio del candidato
    add_servicio_h = demanda[cliente_candidato]["servicio_min"] / 60.0

    tiempo_viaje_nuevo_h = tiempo_viaje_actual_h + add_viaje_h
    tiempo_servicio_nuevo_h = tiempo_servicio_actual_h + add_servicio_h

    # Importante: verificar que aún pueda volver al depósito
    tiempo_retorno_h = _tiempo_viaje_horas_desde_distancia(
        matriz_solver,
        cliente_candidato,
        deposito_id,
        config.velocidad_promedio_kmh
    )

    descanso_h = calcular_descanso_horas(tiempo_viaje_nuevo_h + tiempo_retorno_h)

    tiempo_total_con_retorno_h = (
        tiempo_viaje_nuevo_h
        + tiempo_servicio_nuevo_h
        + tiempo_retorno_h
        + descanso_h
    )

    if tiempo_total_con_retorno_h > config.jornada_max_horas:
        return False, {}

    return True, {
        "volumen_nuevo": vol_nuevo,
        "peso_nuevo": peso_nuevo,
        "tiempo_viaje_nuevo_h": tiempo_viaje_nuevo_h,
        "tiempo_servicio_nuevo_h": tiempo_servicio_nuevo_h,
        "tiempo_total_con_retorno_h": tiempo_total_con_retorno_h,
    }


def _seleccionar_siguiente_cliente(
    ruta_actual: List[int],
    candidatos: List[int],
    deposito_id: int,
    matriz_solver: pd.DataFrame,
    demanda: Dict[int, Dict[str, float]],
    config: ConfiguracionSolver,
    volumen_actual: float,
    peso_actual: float,
    tiempo_viaje_actual_h: float,
    tiempo_servicio_actual_h: float,
) -> Tuple[int | None, Dict[str, float] | None]:
    """
    Selecciona el cliente factible más cercano al último nodo.
    Esto tiende a minimizar distancia secundaria, mientras llena rutas
    hasta donde permitan capacidades y tiempo.
    """
    ultimo = ruta_actual[-1]

    mejor_cliente = None
    mejor_eval = None
    mejor_dist = float("inf")

    for cliente in candidatos:
        factible, info = _evaluar_agregar_cliente(
            ruta_actual=ruta_actual,
            cliente_candidato=cliente,
            deposito_id=deposito_id,
            matriz_solver=matriz_solver,
            demanda=demanda,
            config=config,
            volumen_actual=volumen_actual,
            peso_actual=peso_actual,
            tiempo_viaje_actual_h=tiempo_viaje_actual_h,
            tiempo_servicio_actual_h=tiempo_servicio_actual_h,
        )

        if not factible:
            continue

        dist = _distancia_km(matriz_solver, ultimo, cliente)

        if dist < mejor_dist:
            mejor_dist = dist
            mejor_cliente = cliente
            mejor_eval = info

    return mejor_cliente, mejor_eval


def _cerrar_ruta(
    ruta: List[int],
    deposito_id: int,
    matriz_solver: pd.DataFrame,
    config: ConfiguracionSolver,
    tiempo_viaje_actual_h: float,
) -> float:
    """
    Cierra ruta volviendo al depósito y retorna nuevo tiempo de viaje acumulado.
    """
    ultimo = ruta[-1]
    if ultimo != deposito_id:
        tiempo_retorno_h = _tiempo_viaje_horas_desde_distancia(
            matriz_solver,
            ultimo,
            deposito_id,
            config.velocidad_promedio_kmh
        )
        ruta.append(deposito_id)
        return tiempo_viaje_actual_h + tiempo_retorno_h
    return tiempo_viaje_actual_h


def _calcular_km_ruta(ruta: List[int], matriz_solver: pd.DataFrame) -> float:
    total = 0.0
    for a, b in zip(ruta[:-1], ruta[1:]):
        total += _distancia_km(matriz_solver, a, b)
    return total


def _dos_opt_ruta(ruta: List[int], matriz_solver: pd.DataFrame, time_limit_seg: float | None = None) -> List[int]:
    """
    Mejora simple de ruta manteniendo depósito al inicio y al final.
    Si time_limit_seg no es None, corta al llegar al límite.
    """
    if len(ruta) <= 4:
        return ruta[:]

    t0 = time.perf_counter()

    mejor = ruta[:]
    mejor_costo = _calcular_km_ruta(mejor, matriz_solver)
    mejora = True

    while mejora:
        if time_limit_seg is not None and (time.perf_counter() - t0) >= time_limit_seg:
            break

        mejora = False

        for i in range(1, len(mejor) - 2):
            if time_limit_seg is not None and (time.perf_counter() - t0) >= time_limit_seg:
                break

            for j in range(i + 1, len(mejor) - 1):
                if time_limit_seg is not None and (time.perf_counter() - t0) >= time_limit_seg:
                    break

                if j - i == 1:
                    continue

                candidata = mejor[:]
                candidata[i:j] = reversed(candidata[i:j])

                costo_candidato = _calcular_km_ruta(candidata, matriz_solver)

                if costo_candidato + 1e-9 < mejor_costo:
                    mejor = candidata
                    mejor_costo = costo_candidato
                    mejora = True
                    break

            if mejora:
                break

    return mejor

def resolver_desde_matriz(
    nodos_solver: pd.DataFrame,
    matriz_solver: pd.DataFrame,
    clientes_dia: pd.DataFrame,
    config: ConfiguracionSolver | None = None,
    aplicar_2opt: bool = True,
    time_limit_2opt_seg: float | None = None,
) -> Dict[str, Any]:
    """
    Resuelve una instancia diaria con heurística greedy + 2-opt opcional.

    Parámetros:
    - nodos_solver: DataFrame con node_id y tipo
    - matriz_solver: matriz cuadrada indexada por node_id
    - clientes_dia: DataFrame con columnas:
        node_id, volumen_m3, peso_kg
        opcional: servicio_min
    """

    if config is None:
        config = ConfiguracionSolver()

    _validar_columnas(nodos_solver, ["node_id", "tipo"], "nodos_solver")

    nodos_solver = nodos_solver.copy()
    nodos_solver["node_id"] = nodos_solver["node_id"].astype(int)

    matriz_solver = matriz_solver.copy()
    matriz_solver.index = matriz_solver.index.astype(int)
    matriz_solver.columns = matriz_solver.columns.astype(int)

    deposito_rows = nodos_solver[nodos_solver["tipo"] == config.valor_tipo_deposito]
    if deposito_rows.empty:
        raise ValueError("No se encontró depósito en nodos_solver.")

    deposito_id = int(deposito_rows["node_id"].iloc[0])

    demanda_df = _construir_demanda_por_nodo(
        clientes_dia=clientes_dia,
        servicio_por_cliente_min_default=config.servicio_por_cliente_min
    )

    # Excluir depósito de la demanda si por error viniera en clientes_dia
    demanda_df = demanda_df[demanda_df["node_id"] != deposito_id].copy()

    if demanda_df.empty:
        return {
            "num_rutas": 0,
            "rutas": [],
            "km_totales": 0.0,
            "resumen_rutas": [],
            "clientes_no_asignados": [],
        }

    node_ids_demanda = set(demanda_df["node_id"].tolist())
    node_ids_matriz = set(matriz_solver.index.tolist())

    faltantes_matriz = node_ids_demanda - node_ids_matriz
    if faltantes_matriz:
        raise ValueError(f"Hay clientes del día que no están en la matriz: {sorted(faltantes_matriz)}")

    demanda = {
        int(row["node_id"]): {
            "volumen_m3": float(row["volumen_m3"]),
            "peso_kg": float(row["peso_kg"]),
            "servicio_min": float(row["servicio_min"]),
        }
        for _, row in demanda_df.iterrows()
    }

    pendientes = set(demanda.keys())
    rutas = []
    resumen_rutas = []

    while pendientes:
        ruta = [deposito_id]
        volumen_actual = 0.0
        peso_actual = 0.0
        tiempo_viaje_actual_h = 0.0
        tiempo_servicio_actual_h = 0.0

        while True:
            candidatos = sorted(list(pendientes))

            siguiente, info = _seleccionar_siguiente_cliente(
                ruta_actual=ruta,
                candidatos=candidatos,
                deposito_id=deposito_id,
                matriz_solver=matriz_solver,
                demanda=demanda,
                config=config,
                volumen_actual=volumen_actual,
                peso_actual=peso_actual,
                tiempo_viaje_actual_h=tiempo_viaje_actual_h,
                tiempo_servicio_actual_h=tiempo_servicio_actual_h,
            )

            if siguiente is None:
                break

            ruta.append(siguiente)
            pendientes.remove(siguiente)

            volumen_actual = info["volumen_nuevo"]
            peso_actual = info["peso_nuevo"]
            tiempo_viaje_actual_h = info["tiempo_viaje_nuevo_h"]
            tiempo_servicio_actual_h = info["tiempo_servicio_nuevo_h"]

        # Si no pudo asignar ni un cliente en esta ruta, hay cliente infactible individualmente
        if len(ruta) == 1:
            cliente_infactible = min(list(pendientes))
            raise ValueError(
                f"El cliente node_id={cliente_infactible} no puede ser asignado ni siquiera "
                f"solo en una ruta. Revisa capacidad o tiempo máximo."
            )

        tiempo_viaje_actual_h = _cerrar_ruta(
            ruta=ruta,
            deposito_id=deposito_id,
            matriz_solver=matriz_solver,
            config=config,
            tiempo_viaje_actual_h=tiempo_viaje_actual_h,
        )

        if aplicar_2opt:
            ruta_mejorada = _dos_opt_ruta(
                ruta,
                matriz_solver,
                time_limit_seg=time_limit_2opt_seg
            )
            ruta = ruta_mejorada

        km_ruta = _calcular_km_ruta(ruta, matriz_solver)

        descanso_h = calcular_descanso_horas(tiempo_viaje_actual_h)

        tiempo_total_h = (
            tiempo_viaje_actual_h
            + tiempo_servicio_actual_h
            + descanso_h
        )

        rutas.append(ruta)
        resumen_rutas.append({
            "ruta_id": len(rutas),
            "ruta": ruta,
            "num_clientes": len(ruta) - 2,
            "volumen_m3": round(volumen_actual, 3),
            "peso_kg": round(peso_actual, 3),
            "km_ruta": round(km_ruta, 3),
            "tiempo_viaje_h": round(tiempo_viaje_actual_h, 3),
            "tiempo_servicio_h": round(tiempo_servicio_actual_h, 3),
            "descanso_h": round(descanso_h, 3),
            "tiempo_total_h": round(tiempo_total_h, 3),
            "utilizacion_vol_pct": round(100 * volumen_actual / config.capacidad_volumen_m3, 2),
            "utilizacion_peso_pct": round(100 * peso_actual / config.capacidad_peso_kg, 2),
        })

    km_totales = round(sum(r["km_ruta"] for r in resumen_rutas), 3)

    return {
        "num_rutas": len(rutas),
        "rutas": rutas,
        "km_totales": km_totales,
        "resumen_rutas": resumen_rutas,
        "clientes_no_asignados": sorted(list(pendientes)),
    }