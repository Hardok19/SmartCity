#!/usr/bin/env python3
"""mr05 reducer (y combiner con --combinar): suma, mínimo, máximo y conteo por sensor-día-variable.
Salida final: "id_sensor\tfecha\tvariable\tpromedio\tminimo\tmaximo\tlecturas".
"""
import sys


def emitir(clave, acc, combinar):
    suma, minimo, maximo, conteo = acc
    if combinar:
        print("%s\t%r\t%r\t%r\t%d" % (clave, suma, minimo, maximo, conteo))
    else:
        sensor, fecha, variable = clave.split("|")
        print("%s\t%s\t%s\t%.3f\t%.3f\t%.3f\t%d" % (sensor, fecha, variable, suma / conteo, minimo, maximo, conteo))


def main():
    combinar = "--combinar" in sys.argv
    actual, acc = None, None
    for linea in sys.stdin:
        partes = linea.rstrip("\n").split("\t")
        if len(partes) != 5:
            continue
        clave = partes[0]
        s, mn, mx, c = float(partes[1]), float(partes[2]), float(partes[3]), int(partes[4])
        if clave != actual:
            if actual is not None:
                emitir(actual, acc, combinar)
            actual, acc = clave, [0.0, mn, mx, 0]
        acc[0] += s
        acc[1] = min(acc[1], mn)
        acc[2] = max(acc[2], mx)
        acc[3] += c
    if actual is not None:
        emitir(actual, acc, combinar)


if __name__ == "__main__":
    main()
