#!/usr/bin/env python3
"""descargar_openaq_archivo.py — F2.1 sin API key: archivo público de OpenAQ en S3 (`openaq-data-archive`).

Alternativa a descargar_openaq.py (API v3, requiere OPENAQ_API_KEY). Mismo resultado y mismo formato:
data/raw/openaq/<id_estacion>/<anio>.csv con columnas timestamp_utc,parameter,value,unit (lo lee
generar_sinteticos.py) y una entrada por archivo en data/raw/MANIFEST.json. Idempotente.

Origen: https://openaq-data-archive.s3.amazonaws.com/records/csv.gz/locationid=<id>/year=<a>/month=<mm>/
        location-<id>-<aaaammdd>.csv.gz   (un archivo por estación y día local; bucket público, sin credenciales)
Columnas del archivo: location_id, sensors_id, location, datetime, lat, lon, parameter, units, value

Normalización (la misma convención que la API v3, `period.datetimeFrom`):
  * `datetime` viene en hora local con desfase y marca el FIN del promedio horario (01:00 … 24:00 de cada día);
    se pasa a UTC y se resta 1 h → `timestamp_utc` = inicio de la hora en UTC.
  * Si una estación tiene dos sensores del mismo parámetro en la misma hora, se promedian.
  * Solo pm25, pm10, no2, o3 y co (config/estaciones_openaq.yaml). Los valores se guardan tal cual (crudos:
    la limpieza de rangos es del ETL).

Uso (raíz del repo):  python scripts/descargar_openaq_archivo.py [--anios 2023 2024] [--estacion 154] [--hilos 16]
"""
import argparse
import csv
import gzip
import io
import json
import re
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
import yaml

RAIZ = Path(__file__).resolve().parents[1]
BUCKET = "https://openaq-data-archive.s3.amazonaws.com"
PARAMS_OK = {"pm25", "pm10", "no2", "o3", "co"}
UNA_HORA = timedelta(hours=1)
SESION = requests.Session()


def pedir(url, params=None, intentos=6):
    for k in range(intentos):
        try:
            r = SESION.get(url, params=params, timeout=60)
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
            time.sleep(3 * (k + 1))
            continue
        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(3 * (k + 1))
            continue
        return r
    raise RuntimeError(f"S3: {intentos} intentos fallidos en {url}")


def fecha_clave(clave):
    """'.../location-154-20240701.csv.gz' -> '20240701'"""
    return clave[-15:-7]


def listar(id_est, anio, meses=range(1, 13)):
    """Claves de los archivos diarios de la estación en esos meses del año (ListObjectsV2 por mes)."""
    claves = []
    for mes in meses:
        prefijo = f"records/csv.gz/locationid={id_est}/year={anio}/month={mes:02d}/"
        token = None
        while True:
            params = {"list-type": "2", "prefix": prefijo}
            if token:
                params["continuation-token"] = token
            r = pedir(BUCKET, params)
            r.raise_for_status()
            claves += re.findall(r"<Key>([^<]+\.csv\.gz)</Key>", r.text)
            m = re.search(r"<NextContinuationToken>([^<]+)</NextContinuationToken>", r.text)
            if "<IsTruncated>true</IsTruncated>" not in r.text or not m:
                break
            token = m.group(1)
    return claves


def unidad(texto):
    t = texto.lower()
    if "ppm" in t:
        return "ppm"
    if "ppb" in t:
        return "ppb"
    if "mg/m" in t:
        return "mg/m³"
    if "g/m" in t:
        return "µg/m³"
    return texto


def leer_dia(clave):
    """→ lista de (inicio_hora_utc, parametro, valor|None, unidad, sensors_id)"""
    r = pedir(f"{BUCKET}/{clave}")
    if r.status_code == 404:
        return []
    r.raise_for_status()
    crudo = gzip.decompress(r.content)
    try:
        texto = crudo.decode("utf-8")
    except UnicodeDecodeError:
        texto = crudo.decode("latin-1")
    filas = []
    for f in csv.DictReader(io.StringIO(texto)):
        p = str(f.get("parameter", "")).lower().replace(".", "").replace("_", "")
        if p not in PARAMS_OK:
            continue
        fin = datetime.fromisoformat(f["datetime"]).astimezone(timezone.utc)
        valor = f.get("value")
        filas.append((fin - UNA_HORA, p, float(valor) if valor not in ("", None) else None,
                      unidad(f.get("units", "")), f.get("sensors_id")))
    return filas


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--anios", nargs="+", type=int, default=[2023, 2024])
    ap.add_argument("--estacion", type=int, help="solo esta estación (id OpenAQ)")
    ap.add_argument("--hilos", type=int, default=16)
    a = ap.parse_args()
    with open(RAIZ / "config/estaciones_openaq.yaml", encoding="utf-8") as fh:
        estaciones = yaml.safe_load(fh)["estaciones"]
    manifest_ruta = RAIZ / "data/raw/MANIFEST.json"
    manifest = json.loads(manifest_ruta.read_text(encoding="utf-8")) if manifest_ruta.exists() else {}

    for e in estaciones:
        if a.estacion and e["id"] != a.estacion:
            continue
        carpeta = RAIZ / "data/raw/openaq" / str(e["id"])
        pendientes = [y for y in a.anios if not (carpeta / f"{y}.csv").exists()]
        if not pendientes:
            print(f"= {e['id']} {e['nombre']}: ya descargada")
            continue
        carpeta.mkdir(parents=True, exist_ok=True)
        # El día local D trae las horas que terminan entre D 01:00 y D+1 00:00: el inicio de año UTC cae en el
        # archivo del 1 de enero y la última hora del año en el del 31 de diciembre; se listan también los años
        # vecinos por si el proveedor reporta con otro desfase y se filtra por el inicio de hora en UTC.
        t0 = time.time()
        claves = [c for y in pendientes for c in listar(e["id"], y)]
        bordes = {f"{min(pendientes) - 1}1231", f"{max(pendientes) + 1}0101"}
        claves += [c for c in listar(e["id"], min(pendientes) - 1, [12]) + listar(e["id"], max(pendientes) + 1, [1])
                   if fecha_clave(c) in bordes]
        with ThreadPoolExecutor(a.hilos) as pool:
            dias = list(pool.map(leer_dia, claves))
        acumulado = defaultdict(list)            # (inicio, param) -> valores
        unidades, sensores = {}, defaultdict(set)
        for filas in dias:
            for inicio, p, v, u, sid in filas:
                acumulado[(inicio, p)].append(v)
                unidades.setdefault(p, u)
                sensores[p].add(sid)
        for anio in pendientes:
            filas = []
            for (inicio, p), valores in acumulado.items():
                if inicio.year != anio:
                    continue
                buenos = [v for v in valores if v is not None]
                filas.append((inicio.strftime("%Y-%m-%dT%H:%M:%SZ"), p,
                              round(sum(buenos) / len(buenos), 4) if buenos else None, unidades[p]))
            filas.sort()
            ruta = carpeta / f"{anio}.csv"
            with open(ruta, "w", newline="", encoding="utf-8") as fh:
                w = csv.writer(fh, lineterminator="\n")
                w.writerow(["timestamp_utc", "parameter", "value", "unit"])
                w.writerows(filas)
            nulos = sum(1 for f in filas if f[2] is None)
            por_param = defaultdict(int)
            for f in filas:
                por_param[f[1]] += 1
            n_arch = sum(1 for c in claves if fecha_clave(c).startswith(str(anio)))
            manifest[f"openaq/{e['id']}/{anio}.csv"] = {
                "fecha_descarga": datetime.now(timezone.utc).isoformat(timespec="seconds"), "filas": len(filas),
                "filas_por_parametro": dict(sorted(por_param.items())), "archivos_diarios": n_arch,
                "pct_nulos": round(100 * nulos / max(len(filas), 1), 2), "bytes": ruta.stat().st_size,
                "fuente": "OpenAQ archivo público S3 (openaq-data-archive/records/csv.gz); hora de inicio en UTC",
                "licencia": "según proveedor de cada estación (ver OpenAQ)"}
            print(f"+ {e['id']} {e['nombre']} {anio}: {len(filas):,} filas {dict(sorted(por_param.items()))}"
                  f" · {n_arch} archivos diarios")
        print(f"  sensores por parámetro: { {p: len(s) for p, s in sorted(sensores.items())} } · {time.time() - t0:.0f} s")
        manifest_ruta.parent.mkdir(parents=True, exist_ok=True)
        manifest_ruta.write_text(json.dumps(manifest, indent=1, ensure_ascii=False), encoding="utf-8")


if __name__ == "__main__":
    main()
