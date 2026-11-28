#!/usr/bin/env python3
"""D3 — Prueba documentada de los filtros del dashboard.

Para cada filtro de `comun.Filtros` llama a las mismas funciones que usa la vista principal con un filtro base y con
el filtro cambiado, y anota si el resultado cambió. Una función cuyo resultado no cambia con un filtro que debería
afectarla es un error de conexión entre la UI y los datos. Las funciones sin datos (p. ej. Mongo vacío) se informan
como «sin datos», no como fallo.

  python scripts/prueba_filtros.py            # -> docs/resultados/prueba_filtros.md
"""
from __future__ import annotations

import math
import sys
import time
from dataclasses import replace
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ))
import conectores_mongo_spark as b  # noqa: E402
import conectores_postgres_hadoop as a  # noqa: E402
from comun import Filtros  # noqa: E402

SALIDA = RAIZ / "docs" / "resultados" / "prueba_filtros.md"
BASE = Filtros(fecha_inicio=date(2024, 1, 1), fecha_fin=date(2024, 12, 31))

# filtro -> (filtro cambiado, funciones que deben reaccionar)
CASOS = {
    "Rango de fechas": (replace(BASE, fecha_inicio=date(2024, 3, 1), fecha_fin=date(2024, 3, 31)),
                        ["kpis_movilidad", "kpis_energia", "lecturas_pg", "lecturas_trafico_energia",
                         "kpis_ambiente", "kpis_iot_mongo", "serie_temporal", "lecturas_mongo"]),
    "Perfil horario (hora pico)": (replace(BASE, perfil_horario="hora_pico"),
                                   ["kpis_movilidad", "kpis_energia", "lecturas_pg", "lecturas_trafico_energia",
                                    "kpis_ambiente", "serie_temporal", "lecturas_mongo"]),
    "Zona (Centro)": (replace(BASE, zonas=("Zona Centro",)),
                      ["kpis_movilidad", "kpis_energia", "lecturas_pg", "lecturas_trafico_energia", "kpis_iot",
                       "sensores", "kpis_ambiente", "comparativa_zonas", "lecturas_mongo"]),
    "Tipo de medición (trafico)": (replace(BASE, tipos_metrica=("trafico",)),
                                   ["lecturas_pg", "lecturas_trafico_energia", "kpis_iot", "sensores",
                                    "kpis_iot_mongo", "distribucion_tipos"]),
    "Tipo de medición (temperatura)": (replace(BASE, tipos_metrica=("temperatura",)),
                                       ["kpis_iot", "sensores", "kpis_iot_mongo", "distribucion_tipos",
                                        "lecturas_mongo", "lecturas_pg"]),
    "Sensor (T-003)": (replace(BASE, id_sensor="T-003"),
                       ["lecturas_pg", "lecturas_trafico_energia", "kpis_movilidad", "sensores"]),
    "Rango de valores (≥ 1.000)": (replace(BASE, valor_min=1000.0),
                                   ["lecturas_pg", "lecturas_trafico_energia"]),
}

FUNCIONES = {
    "kpis_movilidad": (b.kpis_movilidad, "Spark"), "kpis_energia": (b.kpis_energia, "Spark"),
    "lecturas_trafico_energia": (b.lecturas_trafico_energia, "Spark"),
    "kpis_ambiente": (b.kpis_ambiente, "MongoDB"), "kpis_iot_mongo": (b.kpis_iot_mongo, "MongoDB"),
    "serie_temporal": (lambda f: b.serie_temporal(f, "pm25", "day"), "MongoDB"),
    "comparativa_zonas": (lambda f: b.comparativa_zonas(f, "pm25"), "MongoDB"),
    "distribucion_tipos": (b.distribucion_tipos, "MongoDB"),
    "lecturas_mongo": (lambda f: b.lecturas_mongo(f, limite=5000), "MongoDB"),
    "lecturas_pg": (lambda f: a.lecturas_pg(f, limite=200_000), "PostgreSQL"),
    "kpis_iot": (a.kpis_iot, "PostgreSQL"), "sensores": (a.sensores, "PostgreSQL"),
}


def huella(res):
    """Resumen comparable del resultado: conteo y suma de lo numérico."""
    if isinstance(res, pd.DataFrame):
        if res.empty:
            return None
        num = res.select_dtypes("number")
        return (len(res), round(float(num.to_numpy().sum()), 4) if not num.empty else 0.0)
    if isinstance(res, dict):
        valores = tuple(round(float(v), 4) for k, v in sorted(res.items())
                        if isinstance(v, (int, float)) and not isinstance(v, bool) and not math.isnan(float(v)))
        return valores if any(valores) else None
    return res


def resumen(res):
    if isinstance(res, pd.DataFrame):
        return f"{len(res):,} filas".replace(",", ".")
    if isinstance(res, dict):
        claves = [k for k in ("vehiculos_detectados", "consumo_total_kwh", "sensores", "pm25_promedio",
                              "mediciones_recibidas") if k in res]
        return ", ".join(f"{k}={res[k]:,.0f}".replace(",", ".") for k in claves[:1]) or "dict"
    return str(res)[:40]


def main():
    cache = {}

    def llamar(nombre, filtro):
        clave = (nombre, filtro)
        if clave not in cache:
            funcion, _ = FUNCIONES[nombre]
            try:
                cache[clave] = funcion(filtro)
            except Exception as e:  # noqa: BLE001
                cache[clave] = e
        return cache[clave]

    filas, t0 = [], time.perf_counter()
    for filtro, (cambiado, funciones) in CASOS.items():
        for nombre in funciones:
            base, nuevo = llamar(nombre, BASE), llamar(nombre, cambiado)
            fuente = FUNCIONES[nombre][1]
            if isinstance(base, Exception) or isinstance(nuevo, Exception):
                err = base if isinstance(base, Exception) else nuevo
                estado, detalle = "⚠ error", f"{type(err).__name__}: {str(err)[:80]}"
            elif huella(base) is None and huella(nuevo) is None:
                estado, detalle = "— sin datos", "la fuente no tiene datos cargados todavía"
            elif huella(base) != huella(nuevo):
                estado, detalle = "✅ cambia", f"{resumen(base)} → {resumen(nuevo)}"
            else:
                estado, detalle = "❌ no cambia", f"{resumen(base)} = {resumen(nuevo)}"
            filas.append((filtro, nombre, fuente, estado, detalle))
            print(f"{filtro:<28} {nombre:<26} {estado}")
    ok = sum(f[3].startswith("✅") for f in filas)
    fallos = sum(f[3].startswith("❌") for f in filas)
    sin = sum(f[3].startswith("—") for f in filas)
    lineas = ["# Prueba de filtros del dashboard (D3)", "",
              f"Generado por `scripts/prueba_filtros.py` el {datetime.now(timezone.utc):%Y-%m-%d %H:%M} UTC en "
              f"{time.perf_counter() - t0:.0f} s. Filtro base: año 2024 completo, todo el día, todas las zonas.", "",
              f"**Resultado:** {ok} combinaciones cambian como se espera, {fallos} no cambian, {sin} sin datos.", "",
              "| Filtro cambiado | Función (la que usa el dashboard) | Fuente | Estado | Base → filtrado |",
              "|---|---|---|---|---|"]
    lineas += [f"| {f} | `{n}` | {s} | {e} | {d} |" for f, n, s, e, d in filas]
    lineas += ["", "Las filas «sin datos» corresponden a fuentes vacías en este entorno (por ejemplo, MongoDB antes de "
               "cargar las lecturas); al cargarlas se vuelve a ejecutar el script."]
    SALIDA.write_text("\n".join(lineas), encoding="utf-8")
    print(f"-> {SALIDA.relative_to(RAIZ).as_posix()}")


if __name__ == "__main__":
    main()
