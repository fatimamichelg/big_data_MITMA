#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Descarga de los ficheros diarios de viajes de MITMA (nivel municipios).

Fuente : https://movilidad-opendata.mitma.es  (datos abiertos del Ministerio de
         Transportes y Movilidad Sostenible; no requiere cuenta ni clave).
Formato: un .csv.gz por día. Los .gz NO son divisibles (Spark los leería con una
         sola tarea), así que se DESCOMPRIMEN a .csv antes de subirlos a HDFS.

Uso:
    python pipeline/download_mitma.py --desde 2022-10-26 --hasta 2022-11-01 --dir data
    python pipeline/download_mitma.py --desde 2022-10-26 --hasta 2022-11-01 --dir data --dry-run
    python pipeline/download_mitma.py --dir data --zonificacion

Es idempotente: si el .csv del día ya existe, no se vuelve a descargar.
También lo usa run_pipeline.py cuando ingesta.descargar = true.
"""
import argparse
import datetime as dt
import gzip
import shutil
import sys
import urllib.error
import urllib.request
from pathlib import Path

URL_BASE = ("https://movilidad-opendata.mitma.es/estudios_basicos/por-municipios/"
            "viajes/ficheros-diarios")
URL_ZONIFICACION = ("https://movilidad-opendata.mitma.es/zonificacion/"
                    "zonificacion_municipios/nombres_municipios.csv")
PATRON = "{fecha}_Viajes_municipios.csv"


def url_dia(fecha, url_base=URL_BASE):
    """URL del .csv.gz de un día (los ficheros se agrupan por carpeta AAAA-MM)."""
    return f"{url_base}/{fecha:%Y-%m}/{fecha:%Y%m%d}_Viajes_municipios.csv.gz"


def rango_fechas(desde, hasta):
    d0, d1 = dt.date.fromisoformat(str(desde)), dt.date.fromisoformat(str(hasta))
    return [d0 + dt.timedelta(days=i) for i in range((d1 - d0).days + 1)]


def _bajar(url, destino):
    req = urllib.request.Request(url, headers={"User-Agent": "mitma-pipeline/1.0"})
    with urllib.request.urlopen(req, timeout=120) as r, open(destino, "wb") as f:
        shutil.copyfileobj(r, f, length=1024 * 1024)


def descargar_dia(fecha, destino_dir, url_base=URL_BASE, patron=PATRON, forzar=False):
    """Descarga y descomprime un día. Devuelve 'existia' o 'descargado'."""
    destino_dir = Path(destino_dir)
    destino_dir.mkdir(parents=True, exist_ok=True)
    csv = destino_dir / patron.format(fecha=f"{fecha:%Y%m%d}")
    if csv.exists() and not forzar:
        return "existia"

    gz_tmp = destino_dir / f".{csv.name}.gz.part"
    csv_tmp = destino_dir / f".{csv.name}.part"
    try:
        _bajar(url_dia(fecha, url_base), gz_tmp)
        with gzip.open(gz_tmp, "rb") as src, open(csv_tmp, "wb") as dst:
            shutil.copyfileobj(src, dst, length=1024 * 1024)
        csv_tmp.replace(csv)          # el fichero final solo aparece cuando está completo
    finally:
        for t in (gz_tmp, csv_tmp):
            if t.exists():
                t.unlink()
    return "descargado"


def descargar_dias(fechas, destino_dir, url_base=URL_BASE, patron=PATRON, forzar=False):
    for f in fechas:
        estado = descargar_dia(f, destino_dir, url_base, patron, forzar)
        print(f"  {f:%Y-%m-%d}: {estado}", flush=True)


def descargar_zonificacion(destino_dir):
    destino_dir = Path(destino_dir)
    destino_dir.mkdir(parents=True, exist_ok=True)
    destino = destino_dir / "nombres_municipios.csv"
    if not destino.exists():
        _bajar(URL_ZONIFICACION, destino)
    print(f"  zonificación: {destino}")
    return destino


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--desde")
    ap.add_argument("--hasta")
    ap.add_argument("--dir", default="data")
    ap.add_argument("--url-base", default=URL_BASE)
    ap.add_argument("--forzar", action="store_true", help="volver a descargar aunque exista")
    ap.add_argument("--dry-run", action="store_true", help="solo muestra las URLs")
    ap.add_argument("--zonificacion", action="store_true",
                    help="descarga también la tabla original nombres_municipios.csv")
    a = ap.parse_args()

    if a.zonificacion:
        if a.dry_run:
            print(URL_ZONIFICACION)
        else:
            descargar_zonificacion(a.dir)
    if a.desde and a.hasta:
        fechas = rango_fechas(a.desde, a.hasta)
        if a.dry_run:
            for f in fechas:
                print(url_dia(f, a.url_base))
        else:
            try:
                descargar_dias(fechas, a.dir, a.url_base, forzar=a.forzar)
            except urllib.error.URLError as e:
                sys.exit(f"Error de descarga: {e}")
    elif not a.zonificacion:
        ap.error("indica --desde y --hasta (o --zonificacion)")


if __name__ == "__main__":
    main()
