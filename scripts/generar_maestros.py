#!/usr/bin/env python3
"""generar_maestros.py — F1.4 (catálogo maestro de sensores). Semilla fija 2026.

Genera data/maestros/sensores.csv a partir de config/zonas.yaml y config/estaciones_openaq.yaml.
Composición por zona: 1 estación real de aire (OpenAQ) + 1 sensor de clima (Open-Meteo, en el centroide)
+ 4 sensores virtuales de aire + 2 virtuales de clima  → 8 por zona, 40 en total.

Columnas del TODO: id_sensor, nombre, tipo_metrica, categoria, zona, lat, lon, fecha_instalacion,
estado, organismo. Extras que usa el generador sintético: origen, id_estacion_openaq, frecuencia_min,
envia_datos (false = nunca envía → PG-Q07 «nunca enviaron»), fecha_baja (inactivos: dejaron de enviar).

Uso (desde la raíz del repo):  python scripts/generar_maestros.py [--frecuencia-min 30]
"""
import argparse
import csv
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import yaml

RAIZ = Path(__file__).resolve().parents[1]
SEMILLA = 2026
VIRT_AIRE, VIRT_CLIMA = 4, 2
ORGANISMOS = ["Greater London Authority", "Defra", "Environment Agency", "Met Office",
              "Transport for London", "Imperial College ERG", "London Councils", "Public Health England"]
CAMPOS = ["id_sensor", "nombre", "tipo_metrica", "categoria", "zona", "lat", "lon", "fecha_instalacion",
          "estado", "organismo", "origen", "id_estacion_openaq", "frecuencia_min", "envia_datos", "fecha_baja"]


def fecha_aleatoria(rng, desde: date, hasta: date) -> date:
    return desde + timedelta(days=int(rng.integers(0, (hasta - desde).days + 1)))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--frecuencia-min", type=int, default=30, help="cadencia de los sensores virtuales (min)")
    ap.add_argument("--salida", default=str(RAIZ / "data/maestros/sensores.csv"))
    a = ap.parse_args()

    zonas = yaml.safe_load(open(RAIZ / "config/zonas.yaml", encoding="utf-8"))["zonas"]
    estaciones = {e["zona"]: e for e in yaml.safe_load(open(RAIZ / "config/estaciones_openaq.yaml", encoding="utf-8"))["estaciones"]}
    rng = np.random.default_rng(SEMILLA)
    filas = []

    def agregar(**kw):
        kw["id_sensor"] = f"S-{len(filas) + 1:03d}"
        filas.append({c: kw.get(c, "") for c in CAMPOS})

    # 1) reales de aire (OpenAQ)  2) reales de clima (Open-Meteo)  3) virtuales
    for z in zonas:
        e = estaciones[z["zona"]]
        agregar(nombre=f"OpenAQ {e['nombre']}", tipo_metrica="calidad_aire", categoria="Calidad del aire", zona=z["zona"],
                lat=e["lat"], lon=e["lon"], fecha_instalacion=fecha_aleatoria(rng, date(2012, 1, 1), date(2020, 12, 31)),
                estado="activo", organismo=ORGANISMOS[1 + z["id_zona"] % 2], origen="openaq", id_estacion_openaq=e["id"],
                frecuencia_min=60, envia_datos="true")
    for z in zonas:
        agregar(nombre=f"Open-Meteo {z['distrito']} (centroide)", tipo_metrica="clima", categoria="Clima", zona=z["zona"],
                lat=z["lat"], lon=z["lon"], fecha_instalacion=date(2022, 1, 1), estado="activo",
                organismo="Met Office", origen="open-meteo", frecuencia_min=60, envia_datos="true")
    virtuales = []
    for z in zonas:
        for tipo, cat, n, pref in (("calidad_aire", "Calidad del aire", VIRT_AIRE, "AIRE"), ("clima", "Clima", VIRT_CLIMA, "CLIMA")):
            for k in range(1, n + 1):
                lat = round(z["lat"] + rng.normal(0, 0.004), 6)
                lon = round(z["lon"] + rng.normal(0, 0.006), 6)
                # 6 de 30 se instalan dentro del período (datos desde esa fecha); el resto antes de 2023
                tardio = rng.random() < 0.2
                inst = fecha_aleatoria(rng, date(2023, 3, 1), date(2024, 6, 30)) if tardio else fecha_aleatoria(rng, date(2019, 1, 1), date(2022, 12, 31))
                agregar(nombre=f"Sensor virtual {z['distrito']} {pref}-{k:02d}", tipo_metrica=tipo, categoria=cat, zona=z["zona"],
                        lat=lat, lon=lon, fecha_instalacion=inst, estado="activo",
                        organismo=ORGANISMOS[int(rng.integers(0, len(ORGANISMOS)))], origen="sintetico",
                        frecuencia_min=a.frecuencia_min, envia_datos="true")
                virtuales.append(len(filas) - 1)

    # ~3 % nunca envían (2 de 30) y ~5 % inactivos que dejaron de enviar (2 de 30) — de los virtuales
    elegidos = rng.choice(virtuales, size=4, replace=False)
    for i in elegidos[:2]:
        filas[i]["envia_datos"] = "false"
    for i in elegidos[2:]:
        filas[i]["estado"] = "inactivo"
        filas[i]["fecha_baja"] = fecha_aleatoria(rng, date(2024, 3, 1), date(2024, 9, 30))

    Path(a.salida).parent.mkdir(parents=True, exist_ok=True)
    with open(a.salida, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CAMPOS, lineterminator="\n")
        w.writeheader()
        w.writerows(filas)
    print(f"{len(filas)} sensores → {a.salida}")
    print("nunca envían:", [f["id_sensor"] for f in filas if f["envia_datos"] == "false"])
    print("inactivos   :", [(f["id_sensor"], f["fecha_baja"]) for f in filas if f["estado"] == "inactivo"])


if __name__ == "__main__":
    main()
