#!/usr/bin/env python3
"""mr02 mapper — ¿Qué sensor produjo mayor cantidad de datos?
Entrada: salida de mr01 ("id_sensor\tmediciones"). Todas las filas van a la misma clave para que UN solo
reducer calcule el top N (el volumen ya es pequeño: una fila por sensor).
"""
import sys


def main():
    for linea in sys.stdin:
        partes = linea.strip().split("\t")
        if len(partes) == 2 and partes[1].isdigit():
            print("top\t%s\t%s" % (partes[1], partes[0]))


if __name__ == "__main__":
    main()
