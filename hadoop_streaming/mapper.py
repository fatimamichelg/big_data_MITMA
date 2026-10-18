#!/usr/bin/env python3
import sys

for line in sys.stdin:
    c = line.rstrip("\n").split("|")
    if len(c) < 15:
        continue
    try:
        v = float(c[13])          # columna viajes (la cabecera falla aquí y se salta)
    except ValueError:
        continue
    if v <= 0:
        continue
    print("{}\t{}".format(c[3], v))   # clave: destino, valor: viajes
