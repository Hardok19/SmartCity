#!/usr/bin/env python3
"""descargar_openaq.py — F2.1. Mediciones horarias de OpenAQ (API v3) por estación de config/estaciones_openaq.yaml.

⚠ ESCRITO SIN PODER PROBARLO CONTRA LA API (el entorno donde se redactó no tiene salida a internet).
  Se basa en la estructura documentada de la API v3; si cambia algún campo, el script avisa con el JSON recibido.
  Primero prueben con UNA estación:  python scripts/descargar_openaq.py --estacion 154 --anios 2024

Requiere API key gratuita (https://explore.openaq.org → cuenta) en la variable OPENAQ_API_KEY (o .env).
Flujo: /v3/locations/{id} → sensores de la estación (pm25, pm10, no2, o3, co) → /v3/sensors/{id}/hours por año.
Guarda data/raw/openaq/<id_estacion>/<anio>.csv con columnas timestamp_utc,parameter,value,unit
(el formato que lee generar_sinteticos.py) y anota en data/raw/MANIFEST.json. Idempotente.
Límite de la API gratuita ~60 peticiones/min: el script espera y reintenta ante 429.
Alternativa sin key: archivo público S3 `openaq-data-archive` (CSV.gz por estación/día).
"""
import argparse
import csv
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
import yaml

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

RAIZ = Path(__file__).resolve().parents[1]
BASE = "https://api.openaq.org/v3"
PARAMS_OK = {"pm25", "pm10", "no2", "o3", "co"}


def get(ruta, params, clave):
    for k in range(6):
        try:
            r = requests.get(f"{BASE}{ruta}", params=params, headers={"X-API-Key": clave}, timeout=60)
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as error:
            espera = 5 * (k + 1)
            print(f"   Conexión interrumpida ({type(error).__name__}); reintento en {espera} s")
            time.sleep(espera)
            continue
        if r.status_code in (408, 429, 500, 502, 503, 504):
            espera = int(r.headers.get("Retry-After", 5 * (k + 1)))
            print(f"   HTTP {r.status_code}; reintento en {espera} s")
            time.sleep(espera)
            continue
        r.raise_for_status()
        time.sleep(1.1)
        return r.json()
    raise RuntimeError(f"OpenAQ: seis intentos fallidos en {ruta} con parámetros {params}")


def nombre_param(p):
    return str(p.get("name", "")).lower().replace(".", "").replace("_", "")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--anios", nargs="+", type=int, default=[2023, 2024])
    ap.add_argument("--estacion", type=int, help="solo esta estación (id OpenAQ)")
    a = ap.parse_args()
    clave = os.environ.get("OPENAQ_API_KEY")
    if not clave:
        raise SystemExit("Falta OPENAQ_API_KEY (variable de entorno o .env)")
    with open(RAIZ / "config/estaciones_openaq.yaml", encoding="utf-8") as f:
        estaciones = yaml.safe_load(f)["estaciones"]
    manifest_ruta = RAIZ / "data/raw/MANIFEST.json"
    manifest = json.loads(manifest_ruta.read_text(encoding="utf-8")) if manifest_ruta.exists() else {}

    for e in estaciones:
        if a.estacion and e["id"] != a.estacion:
            continue
        carpeta = RAIZ / "data/raw/openaq" / str(e["id"])
        carpeta.mkdir(parents=True, exist_ok=True)
        pendientes = [y for y in a.anios if not (carpeta / f"{y}.csv").exists()]
        if not pendientes:
            print(f"= {e['id']} {e['nombre']}: ya descargada")
            continue
        loc = get(f"/locations/{e['id']}", {}, clave)
        try:
            sensores = [(s["id"], nombre_param(s["parameter"]), s["parameter"].get("units", "")) for s in loc["results"][0]["sensors"]]
        except (KeyError, IndexError):
            raise SystemExit(f"Respuesta inesperada de /locations/{e['id']}: {json.dumps(loc)[:400]}")
        sensores = [s for s in sensores if s[1] in PARAMS_OK]
        print(f"* {e['id']} {e['nombre']}: parámetros disponibles {[s[1] for s in sensores]}")
        for anio in pendientes:
            filas = []
            for sid, param, unidad in sensores:
                for mes in range(1, 13):
                    inicio = f"{anio}-{mes:02d}-01T00:00:00Z"
                    fin = (f"{anio + 1}-01-01T00:00:00Z" if mes == 12
                           else f"{anio}-{mes + 1:02d}-01T00:00:00Z")
                    pagina = 1
                    while True:
                        js = get(f"/sensors/{sid}/hours", {
                            "datetime_from": inicio, "datetime_to": fin,
                            "limit": 1000, "page": pagina}, clave)
                        res = js.get("results", [])
                        for r in res:
                            try:
                                ts = r["period"]["datetimeFrom"]["utc"]
                            except (KeyError, TypeError):
                                raise SystemExit(f"Estructura inesperada en /hours: {json.dumps(r)[:400]}")
                            filas.append((ts, param, r.get("value"), unidad))
                        if len(res) < 1000:
                            break
                        pagina += 1
                    print(f"  {param} {anio}-{mes:02d}: completado")
            filas = list(dict.fromkeys(filas))
            ruta = carpeta / f"{anio}.csv"
            with open(ruta, "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f, lineterminator="\n")
                w.writerow(["timestamp_utc", "parameter", "value", "unit"])
                w.writerows(filas)
            nulos = sum(1 for r in filas if r[2] is None)
            manifest[f"openaq/{e['id']}/{anio}.csv"] = {
                "fecha_descarga": datetime.now(timezone.utc).isoformat(timespec="seconds"), "filas": len(filas),
                "pct_nulos": round(100 * nulos / max(len(filas), 1), 2), "bytes": ruta.stat().st_size,
                "fuente": "OpenAQ API v3 (/sensors/{id}/hours)", "licencia": "según proveedor de cada estación (ver OpenAQ)"}
            print(f"   {anio}: {len(filas):,} filas, {nulos} nulos")
    manifest_ruta.parent.mkdir(parents=True, exist_ok=True)
    manifest_ruta.write_text(json.dumps(manifest, indent=1, ensure_ascii=False), encoding="utf-8")


if __name__ == "__main__":
    main()
