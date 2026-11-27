#!/usr/bin/env python3
"""mr04 reducer (y combiner): suma los conteos de cada clave.
Entrada ordenada por clave: "clave\tconteo". Salida final: "ambito\tzona\ttipo_evento\teventos"
Con --combinar devuelve "clave\tconteo" (resultado parcial en el nodo del mapper).
"""
import sys


def emitir(clave, total, combinar):
    if combinar:
        print("%s\t%d" % (clave, total))
    else:
        print("%s\t%d" % (clave.replace("|", "\t"), total))


def main():
    combinar = "--combinar" in sys.argv
    actual, total = None, 0
    for linea in sys.stdin:
        clave, _, valor = linea.rstrip("\n").rpartition("\t")
        if not clave:
            continue
        if clave != actual:
            if actual is not None:
                emitir(actual, total, combinar)
            actual, total = clave, 0
        total += int(valor)
    if actual is not None:
        emitir(actual, total, combinar)


if __name__ == "__main__":
    main()
