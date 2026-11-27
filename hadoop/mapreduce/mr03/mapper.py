#!/usr/bin/env python3
"""mr03 mapper — ¿Cuál es la temperatura promedio histórica?
Entrada: export de `lectura`. Para cada lectura válida con temperatura emite SUMA y CONTEO (no promedios:
un promedio de promedios parciales sería incorrecto) en tres ámbitos: global, por zona y por zona-año.
Salida: "ambito|zona|anio\tsuma\tconteo".
"""
import datetime
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
        temperatura = (doc.get("mediciones") or {}).get("temperatura")
        if not isinstance(temperatura, (int, float)) or not lectura_valida(doc) or not -60 <= temperatura <= 60:
            continue
        zona = (doc.get("ubicacion") or {}).get("zona", "sin_zona")
        anio = fecha_iso(doc.get("timestamp"))[:4]
        for clave in ("global|-|-", "zona|%s|-" % zona, "zona_anio|%s|%s" % (zona, anio)):
            print("%s\t%r\t1" % (clave, float(temperatura)))


if __name__ == "__main__":
    main()
