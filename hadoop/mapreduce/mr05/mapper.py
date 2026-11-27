#!/usr/bin/env python3
"""mr05 mapper — ¿Cuál es el comportamiento de los sensores durante un período?
Variables de entorno (-cmdenv): DESDE y HASTA (AAAA-MM-DD, inclusive). Para cada lectura válida del período
emite cada variable medida: "id_sensor|fecha|variable\tsuma\tminimo\tmaximo\tconteo".
"""
import datetime
import json
import os
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
    desde = os.environ.get("DESDE") or "0000-00-00"
    hasta = os.environ.get("HASTA") or "9999-99-99"
    for doc in documentos(sys.stdin):
        fecha = fecha_iso(doc.get("timestamp"))[:10]
        if not (desde <= fecha <= hasta) or not lectura_valida(doc):
            continue
        sensor = doc.get("sensor_id", "sin_sensor")
        for variable, valor in sorted((doc.get("mediciones") or {}).items()):
            if isinstance(valor, (int, float)) and not isinstance(valor, bool):
                v = float(valor)
                print("%s|%s|%s\t%r\t%r\t%r\t1" % (sensor, fecha, variable, v, v, v))


if __name__ == "__main__":
    main()
