#!/usr/bin/env python3
"""mr02 reducer — top N sensores por cantidad de mediciones (N = variable TOP_N, por defecto 10).
Salida: "posicion\tid_sensor\tmediciones". Requiere -numReduceTasks 1.
"""
import heapq
import os
import sys


def main():
    n = int(os.environ.get("TOP_N", "10") or 10)
    filas = []
    for linea in sys.stdin:
        partes = linea.rstrip("\n").split("\t")
        if len(partes) == 3:
            filas.append((int(partes[1]), partes[2]))
    for posicion, (total, sensor) in enumerate(heapq.nlargest(n, filas, key=lambda f: (f[0], f[1])), start=1):
        print("%d\t%s\t%d" % (posicion, sensor, total))


if __name__ == "__main__":
    main()
