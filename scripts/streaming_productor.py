#!/usr/bin/env python3
"""Reto adicional (streaming): simula la llegada CONTINUA de lecturas de sensores IoT.

Cada lote (un archivo JSON Lines) trae, por zona, una lectura de PM2.5 de un sensor de aire y una de
tráfico del contador de la zona, con timestamp = ahora (UTC). Se escribe en HDFS
/smartcity/streaming/entrada/lote_<epoch_ms>.json con `hdfs dfs -put -` (sube a un archivo temporal
._COPYING_ y lo renombra al final, así Spark nunca ve un archivo a medio escribir).

Valores SIMULADOS (supuestos del generador, documentados en docs/datos/GENERACION_SINTETICA.md §7):
  PM2.5     = base_zona · (1 + 0,35 · hora_pico) · N(1; 0,10)  [· factor si --episodio ZONA]
  vehículos = capacidad · perfil_horario (mismo que generar_trafico_sintetico.py) · N(1; 0,08) [· factor si --congestion ZONA]

    python scripts/streaming_productor.py --lotes 30 --intervalo 5
    python scripts/streaming_productor.py --lotes 12 --intervalo 5 --episodio "Zona Centro"    # fuerza alertas de PM2.5
    python scripts/streaming_productor.py --lotes 12 --intervalo 5 --congestion "Zona Norte"   # fuerza congestión
"""
import argparse
import json
import math
import random
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

RAIZ = Path(__file__).resolve().parent
sys.path.insert(0, str(RAIZ))
from generar_trafico_sintetico import ZONAS as CONTADORES, perfil  # noqa: E402

CONTENEDOR_NAMENODE = "smartcity_namenode"
ENTRADA = "/smartcity/streaming/entrada"
# PM2.5 base por zona (µg/m³), igual que el modelo de generar_sinteticos.py; sensor de aire virtual por zona
BASE_PM25 = {"Zona Norte": 12.0, "Zona Sur": 10.0, "Zona Centro": 14.0, "Zona Este": 9.5, "Zona Oeste": 8.0}
SENSOR_AIRE = {"Zona Norte": "S-011", "Zona Sur": "S-017", "Zona Centro": "S-024", "Zona Este": "S-029",
               "Zona Oeste": "S-035"}   # sensores virtuales de aire activos de data/maestros/sensores.csv
LONDRES = ZoneInfo("Europe/London")


def generar_lote(ahora=None, episodio=None, congestion=None, factor=4.0, rng=None):
    """Lista de lecturas (dict) para un instante: PM2.5 y tráfico por zona."""
    rng = rng or random.Random()
    ahora = ahora or datetime.now(timezone.utc)
    local = ahora.astimezone(LONDRES)
    laborable = local.weekday() < 5
    h = local.hour + local.minute / 60
    rush = math.exp(-((h - 8) / 1.6) ** 2) + math.exp(-((h - 18) / 2) ** 2)
    ts = ahora.isoformat(timespec="seconds").replace("+00:00", "Z")
    lecturas = []
    for zona, id_contador, capacidad, _ in CONTADORES:
        pm25 = BASE_PM25[zona] * (1 + 0.35 * rush) * rng.gauss(1, 0.10)
        if episodio and zona == episodio:
            pm25 *= factor
        vehiculos = capacidad * perfil(local.hour, laborable) * rng.gauss(1, 0.08)
        if congestion and zona == congestion:
            vehiculos *= 1.8
        lecturas.append({"sensor_id": SENSOR_AIRE[zona], "tipo": "calidad_aire", "zona": zona, "timestamp": ts,
                         "variable": "pm25", "valor": round(max(pm25, 0.0), 1), "origen": "streaming_simulado"})
        lecturas.append({"sensor_id": id_contador, "tipo": "trafico", "zona": zona, "timestamp": ts,
                         "variable": "vehiculos_hora", "valor": float(max(round(vehiculos), 0)),
                         "origen": "streaming_simulado"})
    return lecturas


def enviar_lote(lecturas, contenedor=CONTENEDOR_NAMENODE):
    """Escribe el lote en HDFS. Devuelve la ruta creada."""
    ruta = f"{ENTRADA}/lote_{int(time.time() * 1000)}.json"
    datos = "".join(json.dumps(x, ensure_ascii=False) + "\n" for x in lecturas).encode("utf-8")
    r = subprocess.run(["docker", "exec", "-i", contenedor, "hdfs", "dfs", "-put", "-f", "-", ruta],
                       input=datos, capture_output=True, timeout=120)
    if r.returncode != 0:
        raise RuntimeError(r.stderr.decode("utf-8", "replace")[-500:])
    return ruta


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--lotes", type=int, default=30, help="cantidad de lotes (0 = sin fin)")
    ap.add_argument("--intervalo", type=float, default=5.0, help="segundos entre lotes")
    ap.add_argument("--episodio", choices=list(BASE_PM25), help="zona con episodio de contaminación (PM2.5 x factor)")
    ap.add_argument("--congestion", choices=list(BASE_PM25), help="zona con tráfico x1,8")
    ap.add_argument("--factor", type=float, default=4.0)
    a = ap.parse_args()
    rng = random.Random()
    n = 0
    while a.lotes == 0 or n < a.lotes:
        lote = generar_lote(episodio=a.episodio, congestion=a.congestion, factor=a.factor, rng=rng)
        ruta = enviar_lote(lote)
        n += 1
        pm = {x["zona"]: x["valor"] for x in lote if x["variable"] == "pm25"}
        print(f"SMARTCITY_STREAM_LOTE n={n} ruta={ruta} pm25={pm}", flush=True)
        time.sleep(a.intervalo)


if __name__ == "__main__":
    main()
