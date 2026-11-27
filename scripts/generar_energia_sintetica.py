#!/usr/bin/env python3
"""B3.3 / D-03: genera consumo eléctrico horario SIMULADO (no mediciones observadas).

Cinco medidores virtuales agregados, uno por zona (E-001…E-005), 2023-2024 UTC, en kWh
(unidad de contrato.UNIDADES["energia"]). Mismo patrón que generar_trafico_sintetico.py:
hora civil de Londres para el perfil diario, semilla fija para regenerar la misma simulación.

Modelo (SUPUESTO del generador, documentado en docs/datos/GENERACION_SINTETICA.md §6):
    consumo_kwh = poblacion_zona * 0,5 kWh/persona·h * perfil(hora, laborable, tipo_zona)
                  * estacional(día del año) * N(1; 0,05)
  * perfil residencial: picos 07–09 h y 18–21 h; comercial: meseta 09–18 h en días laborables;
    industrial: meseta 06–22 h; mixta: promedio de residencial y comercial.
  * estacional = 1 + 0,18·cos(2π(doy − 15)/365,25)  (más consumo en invierno: calefacción/iluminación).
"""
import argparse
import csv
import math
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import yaml

RAIZ = Path(__file__).resolve().parents[1]
CAMPOS = ["timestamp_utc", "zona", "id_sensor", "consumo_kwh", "origen"]
KWH_POR_PERSONA_HORA = 0.5


def gauss(h, centro, ancho):
    return math.exp(-0.5 * ((h - centro) / ancho) ** 2)


def perfil(hora, laborable, tipo_zona):
    residencial = 0.55 + 0.35 * gauss(hora, 8, 1.3) + 0.55 * gauss(hora, 19.5, 1.8)
    if not laborable:
        residencial = 0.60 + 0.25 * gauss(hora, 10, 2.0) + 0.50 * gauss(hora, 19.5, 2.0)
    comercial = (0.45 + 0.75 * (1 if 9 <= hora <= 18 else 0)) if laborable else 0.55
    industrial = 0.50 + 0.60 * (1 if 6 <= hora <= 22 else 0) * (1 if laborable else 0.6)
    return {
        "residencial": residencial,
        "comercial": comercial,
        "industrial": industrial,
        "mixta": (residencial + comercial) / 2,
    }[tipo_zona]


def zonas():
    datos = yaml.safe_load((RAIZ / "config" / "zonas.yaml").read_text(encoding="utf-8"))
    return [(z["zona"], f"E-{z['id_zona']:03d}", z["tipo_zona"], z["poblacion_aprox"]) for z in datos["zonas"]]


def generar(anio, destino, semilla=2026):
    rng = random.Random(semilla + 100 + anio)
    londres = ZoneInfo("Europe/London")
    inicio = datetime(anio, 1, 1, tzinfo=timezone.utc)
    fin = datetime(anio + 1, 1, 1, tzinfo=timezone.utc)
    destino.parent.mkdir(parents=True, exist_ok=True)
    lista_zonas = zonas()
    n = 0
    with destino.open("w", encoding="utf-8", newline="") as fh:
        salida = csv.DictWriter(fh, fieldnames=CAMPOS, lineterminator="\n")
        salida.writeheader()
        instante = inicio
        while instante < fin:
            local = instante.astimezone(londres)
            laborable = local.weekday() < 5
            estacional = 1 + 0.18 * math.cos((local.timetuple().tm_yday - 15) * 2 * math.pi / 365.25)
            for zona, id_sensor, tipo_zona, poblacion in lista_zonas:
                consumo = (poblacion * KWH_POR_PERSONA_HORA * perfil(local.hour, laborable, tipo_zona)
                           * estacional * rng.gauss(1, 0.05))
                salida.writerow({
                    "timestamp_utc": instante.isoformat(timespec="seconds").replace("+00:00", "Z"),
                    "zona": zona, "id_sensor": id_sensor,
                    "consumo_kwh": f"{max(0.0, consumo):.1f}",
                    "origen": "sintetico",
                })
                n += 1
            instante += timedelta(hours=1)
    return n


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--anios", nargs="+", type=int, default=[2023, 2024])
    parser.add_argument("--semilla", type=int, default=2026)
    parser.add_argument("--salida", default="data/raw/energia")
    args = parser.parse_args()
    for anio in args.anios:
        if anio < 2000 or anio > 2100:
            parser.error("El anio debe estar entre 2000 y 2100")
        ruta = Path(args.salida) / f"energia_{anio}.csv"
        cantidad = generar(anio, ruta, args.semilla)
        print(f"SMARTCITY_ENERGIA_GENERADA={cantidad} ruta={ruta} origen=sintetico", flush=True)


if __name__ == "__main__":
    main()
