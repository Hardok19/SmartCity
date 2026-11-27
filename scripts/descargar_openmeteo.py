#!/usr/bin/env python3
"""descargar_openmeteo.py — F2.2 (el TODO se la asigna a A; B puede correrla si A no la ha hecho).

Descarga clima horario histórico (UTC) por centroide de zona, en bloques de 1 año:
  temperature_2m, relative_humidity_2m, precipitation, wind_speed_10m
Guarda data/raw/openmeteo/zona_<norte|sur|centro|este|oeste>_<anio>.json y registra en data/raw/MANIFEST.json.
Idempotente: si el archivo existe, no se descarga de nuevo. Sin API key. Licencia de los datos: CC BY 4.0
(citar Open-Meteo.com). Uso:  python scripts/descargar_openmeteo.py [--anios 2023 2024]
"""
import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
import yaml

RAIZ = Path(__file__).resolve().parents[1]
URL = "https://archive-api.open-meteo.com/v1/archive"
HORARIAS = "temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m"


def slug(zona):
    return zona.lower().replace("zona ", "")


def pedir(params, reintentos=5):
    for k in range(reintentos):
        r = requests.get(URL, params=params, timeout=60)
        if r.status_code == 429:
            espera = 20 * (k + 1)
            print(f"   429 (límite de uso); espero {espera} s")
            time.sleep(espera)
            continue
        r.raise_for_status()
        return r.json()
    raise RuntimeError("Open-Meteo: demasiados reintentos")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--anios", nargs="+", type=int, default=[2023, 2024])
    a = ap.parse_args()
    with open(RAIZ / "config/zonas.yaml", encoding="utf-8") as f:
        zonas = yaml.safe_load(f)["zonas"]
    destino = RAIZ / "data/raw/openmeteo"
    destino.mkdir(parents=True, exist_ok=True)
    manifest_ruta = RAIZ / "data/raw/MANIFEST.json"
    manifest = json.loads(manifest_ruta.read_text(encoding="utf-8")) if manifest_ruta.exists() else {}

    for z in zonas:
        for anio in a.anios:
            ruta = destino / f"zona_{slug(z['zona'])}_{anio}.json"
            if ruta.exists():
                print(f"= {ruta.name} ya existe")
                continue
            params = {"latitude": z["lat"], "longitude": z["lon"], "start_date": f"{anio}-01-01", "end_date": f"{anio}-12-31",
                      "hourly": HORARIAS, "timezone": "UTC"}
            datos = pedir(params)
            horas = len(datos["hourly"]["time"])
            nulos = {k: sum(v is None for v in vals) for k, vals in datos["hourly"].items() if k != "time"}
            ruta.write_text(json.dumps(datos), encoding="utf-8")
            manifest[ruta.name] = {"url": requests.Request("GET", URL, params=params).prepare().url,
                                   "fecha_descarga": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                                   "bytes": ruta.stat().st_size, "horas": horas, "nulos": nulos,
                                   "licencia": "CC BY 4.0 — Open-Meteo.com"}
            print(f"+ {ruta.name}: {horas} horas, nulos {nulos}")
            time.sleep(1)
    manifest_ruta.write_text(json.dumps(manifest, indent=1, ensure_ascii=False), encoding="utf-8")


if __name__ == "__main__":
    main()
