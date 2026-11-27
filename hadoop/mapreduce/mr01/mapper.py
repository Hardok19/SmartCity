#!/usr/bin/env python3
"""mr01 mapper — ¿Cuántas mediciones existen por sensor?
Entrada: export de la colección `lectura` (JSON Lines). Salida: "id_sensor\t1" por documento.
"""
import datetime  # noqa: F401  (usado por fecha_iso)
import json
import sys

def documentos(entrada):
    """Una línea JSON por documento (export de MongoDB). Las líneas inválidas se cuentan y se omiten."""
    for linea in entrada:
        linea = linea.strip()
        if not linea:
            continue
        try:
            yield json.loads(linea)
        except ValueError:
            sys.stderr.write("reporter:counter:SmartCity,lineas_invalidas,1\n")


def fecha_iso(valor):
    """timestamp de Mongo exportado: {"$date": "2024-01-01T00:00:00Z"} | {"$date": {"$numberLong": ms}} | texto."""
    if isinstance(valor, dict) and "$date" in valor:
        valor = valor["$date"]
        if isinstance(valor, dict) and "$numberLong" in valor:
            return datetime.datetime.utcfromtimestamp(int(valor["$numberLong"]) / 1000.0).strftime("%Y-%m-%dT%H:%M:%S")
    return str(valor) if valor else ""


def lectura_valida(doc):
    calidad = doc.get("calidad_dato") or {}
    return calidad.get("valido", True) is not False


def main():
    for doc in documentos(sys.stdin):
        sensor = doc.get("sensor_id")
        if sensor:
            print("%s\t1" % sensor)


if __name__ == "__main__":
    main()
