#!/usr/bin/env python3
"""mr03 reducer (y combiner con --combinar): acumula suma y conteo por clave.
Salida final: "ambito\tzona\tanio\ttemperatura_promedio\tlecturas".
"""
import sys


def emitir(clave, suma, conteo, combinar):
    if combinar:
        print("%s\t%r\t%d" % (clave, suma, conteo))
    else:
        ambito, zona, anio = clave.split("|")
        print("%s\t%s\t%s\t%.3f\t%d" % (ambito, zona, anio, suma / conteo, conteo))


def main():
    combinar = "--combinar" in sys.argv
    actual, suma, conteo = None, 0.0, 0
    for linea in sys.stdin:
        partes = linea.rstrip("\n").split("\t")
        if len(partes) != 3:
            continue
        clave, s, c = partes[0], float(partes[1]), int(partes[2])
        if clave != actual:
            if actual is not None:
                emitir(actual, suma, conteo, combinar)
            actual, suma, conteo = clave, 0.0, 0
        suma += s
        conteo += c
    if actual is not None:
        emitir(actual, suma, conteo, combinar)


if __name__ == "__main__":
    main()
