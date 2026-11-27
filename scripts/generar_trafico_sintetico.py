#!/usr/bin/env python3
"""Genera conteos horarios de trafico SIMULADO (no mediciones observadas).

Cinco contadores virtuales, uno por zona, 2023-2024 UTC. Usa la hora
civil de Londres para que las franjas de congestion reflejen BST/GMT.
La semilla fija permite regenerar la misma simulacion.
"""
import argparse
import csv
import math
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

ZONAS = [
    ("Zona Norte", "T-001", 1050, 54),
    ("Zona Sur", "T-002", 840, 48),
    ("Zona Centro", "T-003", 1500, 44),
    ("Zona Este", "T-004", 980, 51),
    ("Zona Oeste", "T-005", 780, 55),
]
CAMPOS = ["timestamp_utc", "zona", "id_sensor", "vehiculos_hora",
          "velocidad_promedio_kmh", "congestion_pct", "origen"]


def perfil(hora, laborable):
    """Perfil horario SUPUESTO, no derivado de un aforo real."""
    manana = math.exp(-0.5 * ((hora - 8) / 1.6) ** 2)
    tarde = math.exp(-0.5 * ((hora - 18) / 1.8) ** 2)
    mediodia = math.exp(-0.5 * ((hora - 13) / 3.5) ** 2)
    if laborable:
        return 0.12 + 0.63 * manana + 0.70 * tarde + 0.34 * mediodia
    return 0.15 + 0.52 * math.exp(-0.5 * ((hora - 14) / 4.2) ** 2)


def generar(anio, destino, semilla=2026):
    rng = random.Random(semilla + anio)
    londres = ZoneInfo("Europe/London")
    inicio = datetime(anio, 1, 1, tzinfo=timezone.utc)
    fin = datetime(anio + 1, 1, 1, tzinfo=timezone.utc)
    destino.parent.mkdir(parents=True, exist_ok=True)
    n = 0
    with destino.open("w", encoding="utf-8", newline="") as fh:
        salida = csv.DictWriter(fh, fieldnames=CAMPOS, lineterminator="\n")
        salida.writeheader()
        instante = inicio
        while instante < fin:
            local = instante.astimezone(londres)
            laboral = local.weekday() < 5
            estacional = 1 + 0.07 * math.cos((local.timetuple().tm_yday - 25) * 2 * math.pi / 365.25)
            for zona, id_sensor, capacidad, velocidad_libre in ZONAS:
                intensidad = max(0.01, perfil(local.hour, laboral) * estacional * rng.gauss(1, 0.08))
                vehiculos = max(0, round(capacidad * intensidad))
                ocupacion = min(1.30, vehiculos / capacidad)
                velocidad = max(7, min(velocidad_libre,
                    velocidad_libre * (1 - 0.63 * min(1, ocupacion)) + rng.gauss(0, 2.0)))
                congestion = max(0.0, min(100.0, 100 * (1 - velocidad / velocidad_libre)))
                salida.writerow({
                    "timestamp_utc": instante.isoformat(timespec="seconds").replace("+00:00", "Z"),
                    "zona": zona, "id_sensor": id_sensor,
                    "vehiculos_hora": vehiculos,
                    "velocidad_promedio_kmh": f"{velocidad:.2f}",
                    "congestion_pct": f"{congestion:.2f}",
                    "origen": "sintetico",
                })
                n += 1
            instante += timedelta(hours=1)
    return n


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--anios", nargs="+", type=int, default=[2023, 2024])
    parser.add_argument("--semilla", type=int, default=2026)
    parser.add_argument("--salida", default="data/raw/trafico")
    args = parser.parse_args()
    for anio in args.anios:
        if anio < 2000 or anio > 2100:
            parser.error("El anio debe estar entre 2000 y 2100")
        ruta = Path(args.salida) / f"trafico_{anio}.csv"
        cantidad = generar(anio, ruta, args.semilla)
        print(f"SMARTCITY_TRAFICO_GENERADO={cantidad} ruta={ruta} origen=sintetico", flush=True)


if __name__ == "__main__":
    main()
