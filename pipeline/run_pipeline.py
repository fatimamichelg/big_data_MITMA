#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Pipeline MITMA — punto de entrada único. TODO el código Spark está en este archivo,
ordenado por apartados:

    A  Datos            ingesta -> hdfs (raw) -> etl (curated, Parquet) -> calidad -> formatos
    B  HDFS             fsck (run_pipeline.sh: hdfs fsck -files -blocks -locations)
                        + hdfs_estado: tamaños, bloques y réplicas de cada zona
    C  MapReduce        streaming (Hadoop Streaming en YARN, lo lanza run_pipeline.sh)
                        + mapreduce (RDD de Spark y comparación de resultados)
    D  Análisis         resultados (P1, P2, P3) + cache (con y sin cache())
    E  Machine Learning ml (MLlib: StringIndexer, OneHotEncoder, VectorAssembler, LinearRegression)
    F  Tolerancia       tolerancia (run_pipeline.sh para un DataNode) + f_job (el job de Spark)
    -  Informe          informe: informe/informe.html con todos los resultados

Se ejecuta DENTRO del contenedor mitma-spark; desde el Mac basta con:

    ./run_pipeline.sh                          # todo, de la A a la F, y el informe
    ./run_pipeline.sh --sin-reto               # todo menos el reto F
    ./run_pipeline.sh --etapas C               # un apartado entero (letra)
    ./run_pipeline.sh --etapas etl,calidad     # etapas concretas

Las etapas "streaming" y "tolerancia" usan Docker (hadoop jar, docker stop), por eso las
orquesta run_pipeline.sh; el resto vive aquí.
"""
import argparse
import base64
import csv
import datetime as dt
import html
import io
import json
import os
import re
import sys
import time
from pathlib import Path

os.environ.setdefault("HADOOP_USER_NAME", "root")
sys.path.insert(0, str(Path(__file__).resolve().parent))

from pyspark.ml import Pipeline                                   # noqa: E402
from pyspark.ml.evaluation import RegressionEvaluator              # noqa: E402
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler  # noqa: E402
from pyspark.ml.regression import LinearRegression                 # noqa: E402
from pyspark.sql import SparkSession, Window, functions as F       # noqa: E402
from pyspark.sql.types import (DateType, DoubleType, IntegerType, StringType,   # noqa: E402
                               StructField, StructType)
import yaml                                                        # noqa: E402

# ==========================================================================
# CONFIGURACIÓN: las rutas y los parámetros viven en config.yaml (en la raíz del proyecto);
# aquí solo se leen. Para cambiar una ruta, una fecha o un umbral se edita ese archivo.
# ==========================================================================
RAIZ = Path(__file__).resolve().parent.parent      # este archivo vive en <raiz>/pipeline/
with open(RAIZ / "config.yaml", encoding="utf-8") as _f:
    CFG = yaml.safe_load(_f)

HDFS = CFG["hdfs"]["uri"]
RAW = HDFS + CFG["hdfs"]["raw"]                    # zona raw: CSV tal cual
ZONIF = HDFS + CFG["hdfs"]["zonificacion"]         # tabla auxiliar de municipios
CURATED = HDFS + CFG["hdfs"]["curated"]            # zona curated: Parquet particionado
RESULTS = HDFS + CFG["hdfs"]["results"]            # resultados pequeños en CSV
F_MIN, F_MAX = str(CFG["datos"]["fecha_min"]), str(CFG["datos"]["fecha_max"])   # ventana de 7 días

DIR_LOCAL = RAIZ / CFG["datos"]["dir_local"]       # CSV diarios en tu ordenador (no van a GitHub)
TABLA_AUX = RAIZ / CFG["datos"]["tabla_auxiliar"]
PATRON = CFG["datos"]["patron_fichero"]            # {fecha} = AAAAMMDD
DESCARGAR = CFG["datos"]["descargar"]              # True: descarga los días que falten
URL_BASE = CFG["datos"]["url_base"]
COL_PARTICION = CFG["datos"]["columna_particion"]  # columna por la que se particiona el Parquet

# Esquema explícito de los CSV (no se infiere)
schema = StructType([
    StructField("fecha", DateType()),
    StructField("periodo", IntegerType()),
    StructField("origen", StringType()),
    StructField("destino", StringType()),
    StructField("distancia", StringType()),
    StructField("actividad_origen", StringType()),
    StructField("actividad_destino", StringType()),
    StructField("estudio_origen_posible", StringType()),
    StructField("estudio_destino_posible", StringType()),
    StructField("residencia", StringType()),
    StructField("renta", StringType()),
    StructField("edad", StringType()),
    StructField("sexo", StringType()),
    StructField("viajes", DoubleType()),
    StructField("viajes_km", DoubleType()),
])

TOP_DESTINOS = CFG["analisis"]["top_destinos"]     # P2: top N destinos por día
UMBRAL_Q3 = CFG["analisis"]["umbral_q3"]           # P3: ignora municipios con media diaria menor
TOP_N_Q3 = CFG["analisis"]["top_n_q3"]

ML = CFG["ml"]                                     # apartado E
N_ITER = CFG["reto_f"]["iteraciones"]              # iteraciones del job del reto F (se cambia con --iter)

# ==========================================================================
# SESIÓN DE SPARK (igual que la primera celda del notebook)
# ==========================================================================
S = CFG["spark"]
_builder = (SparkSession.builder.master(S["master"]).appName("mitma-pipeline")
            .config("spark.driver.memory", S["memoria_driver"])
            .config("spark.sql.shuffle.partitions", str(S["shuffle_partitions"]))
            .config("spark.hadoop.dfs.replication", str(CFG["hdfs"]["replicacion"]))
            .config("spark.executorEnv.HADOOP_USER_NAME", "root")
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic"))   # overwrite solo de las fechas escritas
if S["master"].startswith("spark://"):        # clúster Spark (master + worker del docker-compose)
    _builder = (_builder
                .config("spark.driver.host", S["driver_host"])          # cómo llega el worker al driver
                .config("spark.driver.bindAddress", "0.0.0.0")
                .config("spark.executor.memory", S["memoria_executor"])
                .config("spark.executor.cores", str(S["cores_executor"])))
spark = _builder.getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Acceso al sistema de ficheros de Hadoop (HDFS) desde Python, para subir ficheros y listar bloques
_jvm = spark._jvm
HPath = _jvm.org.apache.hadoop.fs.Path
fs = _jvm.org.apache.hadoop.fs.FileSystem.get(_jvm.java.net.URI(HDFS), spark._jsc.hadoopConfiguration())

# Orden de ejecución (apartados A -> F). f_job NO va en el orden por defecto: lo lanza
# run_pipeline.sh en segundo plano mientras para un DataNode.
ORDEN = ["ingesta", "hdfs", "etl", "calidad", "formatos",      # A
         "hdfs_estado",                                         # B
         "mapreduce",                                           # C (+ streaming en el .sh)
         "resultados", "cache",                                 # D
         "ml",                                                  # E
         "f_job",                                               # F (+ tolerancia en el .sh)
         "informe"]
ETAPAS = [e for e in ORDEN if e != "f_job"]
GRUPOS = {"A": ["ingesta", "hdfs", "etl", "calidad", "formatos"], "B": ["hdfs_estado"],
          "C": ["mapreduce"], "D": ["resultados", "cache"], "E": ["ml"], "F": ["f_job"]}

RUTA_STREAMING = CFG["mapreduce"]["salida_streaming"]   # salida del job de Hadoop Streaming (en results)


# ---------------------------------------------------------------- utilidades
def log(msg):
    print(f"[{dt.datetime.now():%H:%M:%S}] {msg}", flush=True)


def fechas_ventana():
    d0, d1 = dt.date.fromisoformat(F_MIN), dt.date.fromisoformat(F_MAX)
    return [d0 + dt.timedelta(days=i) for i in range((d1 - d0).days + 1)]


def nombre_fichero(fecha):
    return PATRON.format(fecha=f"{fecha:%Y%m%d}")


def existe(ruta):
    return fs.exists(HPath(ruta))


def guardar_csv(df, nombre):
    destino = f"{RESULTS}/{nombre}"
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(destino)
    log(f"  -> {destino}")


def leer_raw():
    return (spark.read.schema(schema)
            .option("header", True).option("sep", "|")
            .option("dateFormat", "yyyyMMdd")
            .csv(RAW))


def leer_curated():
    return spark.read.parquet(CURATED).withColumn("fecha", F.to_date("fecha"))


def extranjero(col):
    """Zonas fuera de España: Portugal (PT*), Francia (FR*) y 'externo'."""
    return F.col(col).rlike("^(PT|FR)") | (F.col(col) == "externo")


def retocar(df):
    """Copia en memoria con las zonas extranjeras marcadas y rellenas (no se escribe)."""
    return (df
            .withColumn("origen_extranjero", extranjero("origen"))
            .withColumn("destino_extranjero", extranjero("destino"))
            .withColumn("nombre_origen", F.coalesce("nombre_origen", F.lit("ZONA EXTRANJERA")))
            .withColumn("nombre_destino", F.coalesce("nombre_destino", F.lit("ZONA EXTRANJERA")))
            .withColumn("provincia_origen",
                        F.when(F.col("origen_extranjero"), F.lit("EXT"))
                         .otherwise(F.col("provincia_origen")))
            .withColumn("provincia_destino",
                        F.when(F.col("destino_extranjero"), F.lit("EXT"))
                         .otherwise(F.col("provincia_destino")))
            .withColumn("nombre_provincia_origen",
                        F.coalesce("nombre_provincia_origen", F.lit("Extranjero")))
            .withColumn("nombre_provincia_destino",
                        F.coalesce("nombre_provincia_destino", F.lit("Extranjero")))
            .withColumn("ccaa_origen", F.coalesce("ccaa_origen", F.lit("Extranjero")))
            .withColumn("ccaa_destino", F.coalesce("ccaa_destino", F.lit("Extranjero"))))


# ---------------------------------------------------------------- tiempos y evidencias
def evidence():
    d = RAIZ / "evidence"
    d.mkdir(parents=True, exist_ok=True)
    return d


def registrar_tiempos(tiempos):
    """Añade los tiempos de esta ejecución a evidence/tiempos.csv (lo lee el informe)."""
    f = evidence() / "tiempos.csv"
    nuevo = not f.exists()
    with open(f, "a", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        if nuevo:
            w.writerow(["ts", "etapa", "segundos", "estado"])
        ts = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        for etapa, seg, estado in tiempos:
            w.writerow([ts, etapa, seg, estado])


def _ultimo_tiempo_shell(etapa):
    f = evidence() / "tiempos.csv"
    if not f.exists():
        return None
    val = None
    with open(f, encoding="utf-8") as fh:
        for r in csv.DictReader(fh):
            if r["etapa"] == etapa and r["estado"] == "OK":
                val = float(r["segundos"])
    return val


def _legible(n):
    n = float(n)
    for u in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024 or u == "TB":
            return f"{n:.0f} {u}" if u == "B" else f"{n:.1f} {u}"
        n /= 1024


def _forzar(df):
    """Ejecuta el plan completo sin escribir nada (formato 'noop')."""
    df.write.format("noop").mode("overwrite").save()


def _cronometro(fn):
    t = time.perf_counter()
    fn()
    return round(time.perf_counter() - t, 2)


def mostrar(nombre, titulo, df, n=20):
    """Enseña el resultado de una pregunta (show) y su plan de ejecución (explain) en la consola.
    El plan también se guarda en evidence/<nombre>_explain.txt."""
    print(f"\n----- {nombre.upper()} · {titulo} -----", flush=True)
    df.show(n, truncate=False)
    print(f"----- {nombre.upper()} · plan de ejecución (explain) -----", flush=True)
    df.explain("formatted")
    try:
        plan = df._jdf.queryExecution().explainString(
            spark._jvm.org.apache.spark.sql.execution.ExplainMode.fromString("formatted"))
        (evidence() / f"{nombre}_explain.txt").write_text(plan, encoding="utf-8")
        log(f"  plan de {nombre.upper()} guardado en evidence/{nombre}_explain.txt")
    except Exception as ex:              # noqa: BLE001
        log(f"  (no se pudo guardar el plan de {nombre.upper()}: {type(ex).__name__})")


# ==========================================================================
# APARTADO A · Datos: ingesta, HDFS, ETL (curated), calidad y formatos
#   Ingesta de los CSV diarios, subida a HDFS (raw), limpieza y dos joins -> Parquet
#   particionado por fecha (curated), comprobaciones de calidad y CSV frente a Parquet.
# ==========================================================================
def etapa_ingesta():
    """Paso 1: comprueba que los CSV diarios y la tabla auxiliar están en local (o los descarga)."""
    def faltan():
        return [f for f in fechas_ventana() if not (DIR_LOCAL / nombre_fichero(f)).exists()]

    pendientes = faltan()
    if pendientes and DESCARGAR:
        from download_mitma import descargar_dias
        log(f"  descargando {len(pendientes)} día(s) que faltan en {DIR_LOCAL}")
        descargar_dias(pendientes, DIR_LOCAL, URL_BASE, PATRON)
        pendientes = faltan()
    if pendientes:
        raise FileNotFoundError(
            f"Faltan en {DIR_LOCAL}: {[nombre_fichero(f) for f in pendientes]}. "
            "Colócalos ahí o pon DESCARGAR = True arriba del archivo")
    if not TABLA_AUX.exists():
        raise FileNotFoundError(f"Falta la tabla auxiliar: {TABLA_AUX}")
    log(f"  {len(fechas_ventana())} ficheros diarios + tabla auxiliar disponibles en local")


def etapa_hdfs():
    """Sube los CSV a la zona raw de HDFS. Si ya están (mismo tamaño) no los vuelve a subir."""
    for carpeta in (RAW, ZONIF, CURATED, RESULTS):
        fs.mkdirs(HPath(carpeta))

    pares = [(DIR_LOCAL / nombre_fichero(f), f"{RAW}/{nombre_fichero(f)}") for f in fechas_ventana()]
    pares.append((TABLA_AUX, f"{ZONIF}/{TABLA_AUX.name}"))

    subidos = saltados = 0
    for local, destino in pares:
        ruta = HPath(destino)
        if fs.exists(ruta) and fs.getFileStatus(ruta).getLen() == local.stat().st_size:
            saltados += 1
            log(f"  = {local.name} ya está en HDFS (mismo tamaño), se salta")
        else:
            t = time.perf_counter()
            fs.copyFromLocalFile(False, True, HPath("file://" + str(local)), ruta)
            subidos += 1
            log(f"  + {local.name} subido ({time.perf_counter() - t:.0f} s)")
    log(f"  subidos: {subidos} | saltados: {saltados}")


def con_nombre(df, col, muni):
    """Join con la tabla de municipios: añade nombre, provincia y CCAA de la columna `col`."""
    m = muni.select(F.col("codigo").alias(col),
                    F.col("nombre").alias(f"nombre_{col}"),
                    F.col("provincia").alias(f"nombre_provincia_{col}"),
                    F.col("ccaa").alias(f"ccaa_{col}"))
    return df.join(F.broadcast(m), on=col, how="left")


def etapa_etl():
    """Pasos 2-4 del notebook: leer, limpiar, dos joins con la tabla de municipios, escribir Parquet."""
    viajes = leer_raw()
    muni = (spark.read.option("header", True).option("sep", "|")
            .csv(f"{ZONIF}/{TABLA_AUX.name}")
            .select(F.col("ID").alias("codigo"), F.col("name").alias("nombre"),
                    "provincia", "ccaa"))

    # Limpieza
    limpio = (viajes
        .dropna(subset=["fecha", "periodo", "origen", "destino", "viajes"])
        .filter(F.col("periodo").between(0, 23))
        .filter(F.col("viajes") > 0)
        .filter(F.col("viajes_km").isNull() | (F.col("viajes_km") >= 0))
        .filter(F.col("fecha").between(F_MIN, F_MAX)))

    # Join con la tabla auxiliar: uno para el origen y otro para el destino
    curated = (con_nombre(con_nombre(limpio, "origen", muni), "destino", muni)
               .withColumn("provincia_origen",  F.substring("origen", 1, 2))
               .withColumn("provincia_destino", F.substring("destino", 1, 2)))

    # Parquet particionado por fecha. partitionOverwriteMode=dynamic (en la sesión de Spark)
    # hace que "overwrite" reescriba solo los días escritos: repetirlo no duplica filas.
    # repartition(columna): todas las filas de un día caen en la misma tarea, y cada día queda en
    # 1 fichero Parquet de ~120 MB en vez de ~8 de ~15 MB (evita el problema de archivos pequeños).
    (curated.repartition(COL_PARTICION).write
            .mode("overwrite")
            .partitionBy(COL_PARTICION)
            .parquet(CURATED))
    log(f"  curated escrito en {CURATED} (partición: {COL_PARTICION})")


def etapa_calidad():
    """Comprobaciones del notebook (pasos 3 y 4 y celdas de calidad). Guarda una tabla de controles."""
    controles = []                       # (control, valor, esperado, estado)

    def chk(nombre, valor, ok, esperado):
        controles.append((nombre, str(valor), esperado, "OK" if ok else "REVISAR"))

    def info(nombre, valor):
        controles.append((nombre, str(valor), "informativo", "INFO"))

    # --- Datos de entrada (raw)
    viajes = leer_raw()
    sucias = viajes.agg(
        F.count("*").alias("filas"),
        F.sum(F.col("viajes").isNull().cast("int")).alias("viajes_nulo"),
        F.sum((F.col("viajes") <= 0).cast("int")).alias("viajes_no_positivo"),
        F.sum(F.col("viajes_km").isNull().cast("int")).alias("km_nulo"),
        F.sum((F.col("viajes_km") < 0).cast("int")).alias("km_negativo"),
        F.sum((~F.col("periodo").between(0, 23)).cast("int")).alias("periodo_fuera"),
        F.sum((~F.col("fecha").between(F_MIN, F_MAX)).cast("int")).alias("fecha_fuera"))
    sucias.show(vertical=True)
    r = sucias.first().asDict()
    chk("raw: filas leídas", r["filas"], r["filas"] > 0, "> 0")
    for k in ("viajes_nulo", "viajes_no_positivo", "km_nulo", "km_negativo", "periodo_fuera", "fecha_fuera"):
        info(f"raw: {k}", r[k] or 0)

    # --- Paso 3: qué valores tienen las columnas categóricas (último día de la ventana)
    perfil = []
    # Solo se lee el CSV de ese día (como en el notebook): basta para ver qué valores hay y es rápido.
    dia = (spark.read.schema(schema).option("header", True).option("sep", "|")
           .option("dateFormat", "yyyyMMdd")
           .csv(f"{RAW}/{nombre_fichero(fechas_ventana()[-1])}"))
    for col in ["distancia", "actividad_origen", "actividad_destino", "renta", "edad", "sexo"]:
        top = dia.groupBy(col).count().orderBy(F.desc("count")).limit(15).collect()
        log(f"  {col}: " + ", ".join(f"{x[col]}={x['count']:,}" for x in top[:5]) + " ...")
        perfil += [(col, str(x[col]), int(x["count"])) for x in top]
    guardar_csv(spark.createDataFrame(perfil, "columna STRING, valor STRING, filas LONG"),
                "perfil_columnas")

    # --- Paso 4: la tabla de municipios no puede tener códigos repetidos (si no, el join duplica filas)
    muni = (spark.read.option("header", True).option("sep", "|").csv(f"{ZONIF}/{TABLA_AUX.name}"))
    dup = muni.groupBy("ID").count().filter("count > 1").count()
    chk("tabla auxiliar: códigos de municipio duplicados", dup, dup == 0, "== 0")

    # --- Tabla curated tras el join
    c0 = leer_curated()
    n_filas = c0.count()
    n_fechas = c0.select("fecha").distinct().count()
    chk("curated: filas", n_filas, n_filas > 0, "> 0")
    chk("curated: días (particiones)", n_fechas, n_fechas == len(fechas_ventana()), f"== {len(fechas_ventana())}")

    # Idempotencia: cada ejecución apunta cuántas filas tiene curated; si se repite el pipeline,
    # el recuento tiene que ser el mismo (overwrite dinámico por partición: no duplica ni pierde filas).
    f_idem = evidence() / "idempotencia.csv"
    anterior = None
    if f_idem.exists():
        with open(f_idem, encoding="utf-8") as fh:
            previas = list(csv.DictReader(fh))
        if previas:
            anterior = int(previas[-1]["filas_curated"])
    if anterior is not None:
        chk("idempotencia: filas de curated iguales a la ejecución anterior",
            f"{n_filas} vs {anterior}", n_filas == anterior, "iguales")
    with open(f_idem, "a", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        if anterior is None and fh.tell() == 0:
            w.writerow(["ts", "filas_curated", "dias"])
        w.writerow([dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), n_filas, n_fechas])
    pct = round(100 * (r["filas"] - n_filas) / r["filas"], 2)
    info("limpieza: % filas descartadas", pct)

    sin_nombre = c0.filter(F.col("nombre_origen").isNull() | F.col("nombre_destino").isNull()).count()
    raros = c0.filter((F.col("nombre_origen").isNull() & ~extranjero("origen")) |
                      (F.col("nombre_destino").isNull() & ~extranjero("destino"))).count()
    ccaa_raros = c0.filter((F.col("ccaa_origen").isNull() & ~extranjero("origen")) |
                           (F.col("ccaa_destino").isNull() & ~extranjero("destino"))).count()
    n_ccaa = c0.filter(F.col("ccaa_origen").isNotNull()).select("ccaa_origen").distinct().count()
    log(f"  sin nombre: {sin_nombre} | sin nombre y no extranjero: {raros} | "
        f"sin CCAA y no extranjero: {ccaa_raros} | CCAA distintas (origen): {n_ccaa}")
    info("curated: filas sin nombre (PT, FR, externo)", sin_nombre)
    chk("curated: sin nombre y no extranjero", raros, raros == 0, "== 0")
    chk("curated: sin CCAA y no extranjero", ccaa_raros, ccaa_raros == 0, "== 0")
    info("curated: CCAA distintas (origen)", n_ccaa)

    # --- EXT como categoría más (celda 25): coincide con las zonas extranjeras y no hay provincias nulas
    c = retocar(c0)
    ext = c.agg(
        F.sum((F.col("provincia_origen") == "EXT").cast("int")).alias("ext_o"),
        F.sum(F.col("origen_extranjero").cast("int")).alias("extr_o"),
        F.sum((F.col("provincia_destino") == "EXT").cast("int")).alias("ext_d"),
        F.sum(F.col("destino_extranjero").cast("int")).alias("extr_d"),
        F.sum((F.col("provincia_origen").isNull() | F.col("provincia_destino").isNull()).cast("int")
              ).alias("prov_nulas")).first().asDict()
    chk("curated: EXT == zonas extranjeras (origen y destino)",
        f"{ext['ext_o']}={ext['extr_o']} | {ext['ext_d']}={ext['extr_d']}",
        ext["ext_o"] == ext["extr_o"] and ext["ext_d"] == ext["extr_d"], "iguales")
    chk("curated: provincias nulas", ext["prov_nulas"] or 0, (ext["prov_nulas"] or 0) == 0, "== 0")

    tabla = spark.createDataFrame(controles, "control STRING, valor STRING, esperado STRING, estado STRING")
    tabla.show(100, truncate=False)
    guardar_csv(tabla, "calidad_pipeline")
    # Acción ante un control fallido: se REGISTRA (estado REVISAR en calidad_pipeline e informe) y se AVISA aquí
    n_revisar = sum(1 for c in controles if c[3] == "REVISAR")
    if n_revisar:
        log(f"  AVISO: {n_revisar} control(es) de calidad en REVISAR (ver tabla de arriba)")
    else:
        log("  calidad: todos los controles en OK")


def etapa_formatos():

    def consulta(formato):
        df = leer_raw() if formato == "csv" else leer_curated()
        return df.groupBy("fecha").agg(F.sum("viajes").alias("viajes")).orderBy("fecha").collect()

    filas = []
    for formato, ruta in (("csv", RAW), ("parquet", CURATED)):
        tam = fs.getContentSummary(HPath(ruta)).getLength()
        t1 = _cronometro(lambda: consulta(formato))
        t2 = _cronometro(lambda: consulta(formato))
        log(f"  {formato}: {_legible(tam)} | 1ª ejecución {t1} s | 2ª {t2} s")
        filas.append((formato, int(tam), _legible(tam), t1, t2))
    guardar_csv(spark.createDataFrame(
        filas, "formato STRING, tamano_bytes LONG, tamano STRING, seg_1a DOUBLE, seg_2a DOUBLE"),
        "comparacion_formatos")


# ==========================================================================
# APARTADO B · HDFS: bloques, réplicas y tamaños
#   Estado del data lake (raw, curated, results).
# ==========================================================================
def etapa_hdfs_estado():
    zonas = {"raw": f"{HDFS}/raw", "curated": f"{HDFS}/curated", "results": RESULTS}
    filas = []
    for zona in zonas:
        ruta = zonas[zona]
        if not existe(ruta):
            continue
        n_arch = n_bloques = n_bytes = 0
        reps = set()
        it = fs.listFiles(HPath(ruta), True)
        while it.hasNext():
            st = it.next()
            n_arch += 1
            n_bytes += st.getLen()
            n_bloques += len(st.getBlockLocations())
            reps.add(int(st.getReplication()))
        filas.append((zona, zonas[zona], n_arch, int(n_bytes), n_bloques,
                      min(reps) if reps else 0, max(reps) if reps else 0))
        log(f"  {zona}: {n_arch} ficheros, {_legible(n_bytes)}, {n_bloques} bloques, "
              f"réplicas {min(reps) if reps else 0}-{max(reps) if reps else 0}")
    guardar_csv(spark.createDataFrame(
        filas, "zona STRING, ruta STRING, ficheros LONG, bytes LONG, bloques LONG, "
               "replicas_min INT, replicas_max INT"), "hdfs_estado")
    try:
        nodos = [(str(n.getHostName()), int(n.getDfsUsed())) for n in fs.getDataNodeStats()]
        log(f"  DataNodes vivos: {len(nodos)}")
        guardar_csv(spark.createDataFrame(nodos, "datanode STRING, bytes_usados LONG"),
                      "hdfs_datanodes")
    except Exception as ex:                                  # noqa: BLE001
        log(f"  (no se pudo listar DataNodes: {type(ex).__name__})")


# ==========================================================================
# APARTADO C · MapReduce: Hadoop Streaming (YARN) frente a Spark RDD
#   El job de Hadoop Streaming (hadoop_streaming/mapper.py y reducer.py) lo lanza
#   run_pipeline.sh (etapa 'streaming'). Aquí: la versión RDD y la comparación.
# ==========================================================================
def etapa_mapreduce():
    def parse(line):
        c = line.split("|")
        if len(c) < 15:
            return []
        try:
            v = float(c[13])          # la cabecera falla aquí y se descarta
        except ValueError:
            return []
        return [(c[3], v)] if v > 0 else []

    t = time.perf_counter()
    res = (spark.sparkContext.textFile(RAW)
           .flatMap(parse)
           .reduceByKey(lambda a, b: a + b)
           .collect())
    seg = round(time.perf_counter() - t, 2)
    log(f"  segundos: {seg} | destinos: {len(res)}")
    resultado = dict(res)

    filas = []
    t_hadoop = _ultimo_tiempo_shell("streaming")
    if t_hadoop is not None:
        filas.append(("hadoop_streaming_yarn", t_hadoop, ""))
    filas.append(("spark_rdd", seg, ""))

    # ¿El job de Hadoop Streaming y el RDD dan lo mismo?
    ruta_mr = f"{RESULTS}/{RUTA_STREAMING}"
    if existe(ruta_mr):
        mr = {}
        for fila in spark.read.text(ruta_mr).collect():
            k, _, v = fila[0].partition("\t")
            if k:
                mr[k] = float(v)
        mismas = set(mr) == set(resultado)
        maxdif = max((abs(mr[k] - resultado[k]) / max(1.0, abs(resultado[k]))
                      for k in mr if k in resultado), default=0.0)
        coinciden = mismas and maxdif < 1e-6
        log(f"  Hadoop Streaming vs RDD: {len(mr)} vs {len(resultado)} destinos | "
              f"diferencia relativa máx.: {maxdif:.2e} | coinciden: {coinciden}")
        filas.append(("resultados_coinciden", None,
                      f"{'sí' if coinciden else 'NO'}: {len(mr)} destinos; "
                      f"diferencia relativa máx. {maxdif:.2e}"))

        # Resultado legible: los 10 destinos con más viajes, con el nombre del municipio, según
        # Hadoop Streaming (lo que sale de hdfs dfs -cat) y según Spark RDD, lado a lado.
        nombres = {r["ID"]: r["name"] for r in
                   (spark.read.option("header", True).option("sep", "|")
                    .csv(f"{ZONIF}/{TABLA_AUX.name}").select("ID", "name").collect())}
        top10 = sorted(resultado.items(), key=lambda kv: -kv[1])[:10]
        comp = spark.createDataFrame(
            [(i, k, nombres.get(k, "(sin nombre)"),
              int(round(mr[k])) if k in mr else None, int(round(v)))
             for i, (k, v) in enumerate(top10, start=1)],
            "puesto INT, destino STRING, municipio STRING, viajes_hadoop LONG, viajes_spark_rdd LONG")
        print("\n----- C · top 10 destinos con más viajes en los 7 días "
              "(Hadoop Streaming vs Spark RDD) -----", flush=True)
        comp.show(truncate=False)
        guardar_csv(comp, "mr_top10_destinos")
    else:
        log(f"  (no existe {ruta_mr}: lanza antes la etapa 'streaming' para comparar resultados)")

    guardar_csv(spark.createDataFrame(
        filas, "implementacion STRING, segundos DOUBLE, nota STRING"), "comparacion_mr_spark")

    # Contadores del job de Hadoop (los saca del log que guarda run_pipeline.sh)
    log_mr = evidence() / "mr_streaming_pipeline.log"
    if log_mr.exists():
        txt = log_mr.read_text(encoding="utf-8", errors="replace")
        claves = ["Launched map tasks", "Launched reduce tasks", "Map input records",
                  "Map output records", "Combine input records", "Combine output records",
                  "Reduce input records", "Reduce shuffle bytes", "Spilled Records",
                  "Failed Shuffles", "Bytes Read"]
        cont = []
        for k in claves:
            m = re.search(rf"{re.escape(k)}=(\d+)", txt)
            if m:
                cont.append((k, int(m.group(1))))
        if cont:
            guardar_csv(spark.createDataFrame(cont, "contador STRING, valor LONG"),
                          "mr_contadores")


# ==========================================================================
# APARTADO D · Análisis: preguntas analíticas y optimización con cache
#   P1 agrupación, P2 ranking (ventana), P3 desviación (ventana) y el efecto de cache().
# ==========================================================================
def etapa_resultados():
    c = retocar(leer_curated())

    # P1 (agrupación): ¿cuáles son los flujos más grandes entre CCAA distintas y qué % del total suponen?
    nac = c.filter(~F.col("origen_extranjero") & ~F.col("destino_extranjero"))
    inter = nac.filter(F.col("ccaa_origen") != F.col("ccaa_destino"))
    total_inter = inter.agg(F.sum("viajes")).first()[0]
    q1 = (inter.groupBy("ccaa_origen", "ccaa_destino")
               .agg(F.sum("viajes").alias("viajes"))
               .withColumn("pct_interccaa", F.round(100 * F.col("viajes") / total_inter, 2))
               .orderBy(F.col("viajes").desc()))
    q1_out = q1.withColumn("viajes", F.round("viajes").cast("long"))
    guardar_csv(q1_out, "q1_flujos_ccaa")
    mostrar("q1", "flujos más grandes entre CCAA distintas (top 10)", q1_out, 10)

    # Agrupación por municipio de destino (la usan P2 y P3)
    diario_dest = (c.filter(~F.col("destino_extranjero"))
                    .groupBy("fecha", "destino", "nombre_destino")
                    .agg(F.sum("viajes").alias("viajes")))

    # P2 (ventana, ranking): los 3 municipios que más viajes reciben cada día
    w = Window.partitionBy("fecha").orderBy(F.col("viajes").desc())
    q2 = (diario_dest.withColumn("puesto", F.rank().over(w))
                     .filter(F.col("puesto") <= TOP_DESTINOS)
                     .orderBy("fecha", "puesto"))
    q2_out = q2.withColumn("viajes", F.round("viajes").cast("long"))
    guardar_csv(q2_out, "q2_top3_destinos")
    mostrar("q2", f"top {TOP_DESTINOS} municipios de destino de cada día (ventana rank)", q2_out, 50)

    # P3 (ventana, comparación con el grupo): desviación de cada día respecto a la media del municipio
    w2 = Window.partitionBy("destino")
    q3 = (diario_dest
          .withColumn("media_muni", F.avg("viajes").over(w2))
          .withColumn("desviacion", F.col("viajes") - F.col("media_muni"))
          .withColumn("desv_pct", F.round(100 * F.col("desviacion") / F.col("media_muni"), 1)))

    def a_enteros(df):
        return (df.withColumn("viajes",     F.round("viajes").cast("long"))
                  .withColumn("media_muni", F.round("media_muni").cast("long"))
                  .withColumn("desviacion", F.round("desviacion").cast("long")))

    guardar_csv(a_enteros(q3), "q3_desviacion_municipio")
    q3_top = a_enteros(q3.filter(F.col("media_muni") >= UMBRAL_Q3)
                         .orderBy(F.abs("desv_pct").desc())
                         .limit(TOP_N_Q3))
    guardar_csv(q3_top, "q3_top20_desviaciones")
    mostrar("q3", f"días que más se desvían de la media de su municipio (top {TOP_N_Q3})", q3_top, TOP_N_Q3)


def etapa_cache():
    top = TOP_DESTINOS

    def diario():
        c = retocar(leer_curated())
        return (c.filter(~F.col("destino_extranjero"))
                 .groupBy("fecha", "destino", "nombre_destino")
                 .agg(F.sum("viajes").alias("viajes")))

    def p2(d):
        w = Window.partitionBy("fecha").orderBy(F.col("viajes").desc())
        return d.withColumn("puesto", F.rank().over(w)).filter(F.col("puesto") <= top)

    def p3(d):
        w = Window.partitionBy("destino")
        return (d.withColumn("media_muni", F.avg("viajes").over(w))
                 .withColumn("desviacion", F.col("viajes") - F.col("media_muni")))

    d = diario()
    s2 = _cronometro(lambda: _forzar(p2(d)))
    s3 = _cronometro(lambda: _forzar(p3(d)))
    dc = diario().cache()
    crear = _cronometro(dc.count)
    c2 = _cronometro(lambda: _forzar(p2(dc)))
    c3 = _cronometro(lambda: _forzar(p3(dc)))
    dc.unpersist()
    filas = [("P2 sin cache", s2), ("P3 sin cache", s3), ("TOTAL sin cache", round(s2 + s3, 2)),
             ("Crear cache (count, una vez)", crear), ("P2 con cache", c2), ("P3 con cache", c3),
             ("TOTAL con cache (count + P2 + P3)", round(crear + c2 + c3, 2))]
    for n, s in filas:
        log(f"  {n}: {s} s")
    guardar_csv(spark.createDataFrame(filas, "escenario STRING, segundos DOUBLE"),
                  "comparacion_cache")


# ==========================================================================
# APARTADO E · Machine Learning (MLlib)
#   Regresión lineal sobre destino y franja de edad.
# ==========================================================================
def etapa_ml():
    m = ML
    if not m["activar"]:
        log("  ml.activar = false, se omite")
        return
    obj, feats = m["objetivo"], list(m["features_categoricas"])
    corte = F.lit(str(m["train_hasta"])).cast("date")

    # Unidad de análisis: viajes por (día, municipio de destino, franja de edad)
    c = retocar(leer_curated())
    base = (c.filter(~F.col("destino_extranjero"))
             .withColumn("edad", F.coalesce("edad", F.lit("NA")))   # "NA" = categoría explícita
             .groupBy("fecha", *feats).agg(F.sum(obj).alias(obj))).cache()
    if m.get("split", "aleatorio") == "cronologico":
        train = base.filter(F.col("fecha") <= corte)
        test = base.filter(F.col("fecha") > corte)
        txt = f"split cronológico, corte {m['train_hasta']}"
    else:   # randomSplit set seed
        prop = float(m["proporcion_train"])
        train, test = base.randomSplit([prop, 1 - prop], seed=int(m["semilla"]))
        txt = f"randomSplit {prop:.0%}/{1 - prop:.0%}, semilla {m['semilla']}"
    n_train, n_test = train.count(), test.count()
    log(f"  filas train: {n_train} | filas test: {n_test} ({txt})")
    if n_train == 0 or n_test == 0:
        raise ValueError("train o test vacío: revisa ml.train_hasta y la ventana de fechas")

    # Feature engineering, como tenemos muchos grupos de municipios, hacemos one hot encode y aplicamos VectroAssembler 
    #para tener nuestra columna de features
    idx = [f"{f}_idx" for f in feats]
    ohe = [f"{f}_ohe" for f in feats]
    pipe = Pipeline(stages=[
        StringIndexer(inputCols=feats, outputCols=idx, handleInvalid="keep"),
        OneHotEncoder(inputCols=idx, outputCols=ohe, handleInvalid="keep"),
        VectorAssembler(inputCols=ohe, outputCol="features"),
        LinearRegression(featuresCol="features", labelCol=obj, predictionCol="prediction",
                         regParam=float(m["reg_param"]), elasticNetParam=0.0),
    ])
    modelo = pipe.fit(train) #estimator
    pred = modelo.transform(test) #ya aqui es un transformer

    def metricas(df, nombre):
        ev = RegressionEvaluator(labelCol=obj, predictionCol="prediction")
        return (nombre,
                float(ev.setMetricName("rmse").evaluate(df)),
                float(ev.setMetricName("r2").evaluate(df)),
                int(n_train), int(n_test))

    # Línea base trivial: predecir siempre la media de viajes del conjunto de entrenamiento
    media_train = float(train.agg(F.avg(obj)).first()[0])
    base_pred = test.withColumn("prediction", F.lit(media_train))
    filas = [metricas(pred, "regresion_lineal"), metricas(base_pred, "linea_base_media")]
    log(f"  RMSE regresión lineal: {filas[0][1]:,.2f} | RMSE línea base (media): {filas[1][1]:,.2f}")
    tabla = spark.createDataFrame(
        filas, "modelo STRING, rmse DOUBLE, r2 DOUBLE, filas_train LONG, filas_test LONG")
    tabla.show(truncate=False)
    guardar_csv(tabla, "ml_metricas")

    # Coeficientes de las variables 
    lm = modelo.stages[-1]
    coefs = lm.coefficients.toArray()
    etiquetas = modelo.stages[0].labelsArray
    filas_coef, pos = [("intercepto", "", float(lm.intercept))], 0
    for f, labs in zip(feats, etiquetas):
        if len(labs) <= 50:
            filas_coef += [(f, str(lab), float(coefs[pos + i])) for i, lab in enumerate(labs)]
        pos += len(labs)
    guardar_csv(spark.createDataFrame(
        filas_coef, "variable STRING, categoria STRING, coeficiente DOUBLE"), "ml_coeficientes")
    base.unpersist()


# ==========================================================================
# APARTADO F · Tolerancia a fallos (HDFS + Spark)
#   run_pipeline.sh (etapa 'tolerancia') levanta un 3.er DataNode, lanza f_job y para un nodo.
#   f_job es la parte de Spark: un job largo que debe acabar con el mismo resultado.
# ==========================================================================
def etapa_f_job():
    """Parte de Spark del reto F. La lanza run_pipeline.sh en segundo plano y, mientras
    corre, para un DataNode con `docker stop`. Aquí solo se comprueba que el resultado
    del job es idéntico antes y después del fallo."""
    n_iter = N_ITER
    salida = evidence() / "reto_f"
    salida.mkdir(parents=True, exist_ok=True)
    curated = CURATED

    # 1) Linaje de Spark antes del fallo: el plan que Spark recalcularía si pierde una partición
    df = spark.read.parquet(curated)
    por_destino = (df.filter(~F.col("destino").rlike("^(PT|FR)") & (F.col("destino") != "externo"))
                     .groupBy("destino").agg(F.sum("viajes").alias("viajes")))
    linaje = por_destino.rdd.toDebugString()
    linaje = linaje.decode() if isinstance(linaje, bytes) else linaje
    plan = por_destino._jdf.queryExecution().explainString(
        spark._jvm.org.apache.spark.sql.execution.ExplainMode.fromString("simple"))
    (salida / "linaje.txt").write_text(
        "=== Plan (explain) ===\n" + plan + "\n=== Linaje del RDD (toDebugString) ===\n" + linaje,
        encoding="utf-8")

    # 2) Referencia antes del fallo
    ref_total = df.agg(F.sum("viajes")).first()[0]
    ref_filas = df.count()
    ref_dest = por_destino.count()
    log(f"  REFERENCIA filas={ref_filas:,} suma(viajes)={ref_total:,.0f} destinos={ref_dest:,}")

    # 3) Job largo: cada iteración vuelve a leer /curated/viajes desde HDFS
    filas, errores = [], 0
    for i in range(1, n_iter + 1):
        t0 = time.time()
        spark.sparkContext.setJobDescription(f"reto_F iteracion {i}")
        try:
            tot = (spark.read.parquet(curated).groupBy("fecha", "destino")
                        .agg(F.sum("viajes").alias("v")).agg(F.sum("v")).first()[0])
            estado = "OK" if abs(tot - ref_total) < 1 else "DIFERENTE"
        except Exception as ex:                                  # noqa: BLE001
            tot, estado = float("nan"), "ERROR"
            errores += 1
            log(f"  iteración {i:2d}: ERROR {type(ex).__name__}: {str(ex)[:200]}")
        dur = round(time.time() - t0, 1)
        filas.append((i, dt.datetime.now().strftime("%H:%M:%S"), dur, tot, estado))
        log(f"  iteración {i:2d}: {dur:5.1f} s | suma(viajes)={tot:,.0f} | {estado}")

    # 4) Comprobación final: mismo resultado que antes del fallo
    d = spark.read.parquet(curated)
    post_total, post_filas = d.agg(F.sum("viajes")).first()[0], d.count()
    identico = (ref_filas == post_filas and abs(ref_total - post_total) < 1
                and all(f[4] == "OK" for f in filas))
    log(f"  ANTES   filas={ref_filas:,} suma={ref_total:,.0f}")
    log(f"  DESPUÉS filas={post_filas:,} suma={post_total:,.0f}")
    log(f"  Resultado idéntico: {identico}")

    # Spark UI (notebook: "tareas reintentadas"): se lee del API REST de la propia UI
    tareas_fallidas = None
    try:
        import urllib.request
        sc = spark.sparkContext
        url = f"{sc.uiWebUrl}/api/v1/applications/{sc.applicationId}/stages"
        with urllib.request.urlopen(url, timeout=10) as r:
            etapas = json.load(r)
        tareas_fallidas = int(sum(e.get("numFailedTasks", 0) for e in etapas))
        log(f"  Spark UI: {len(etapas)} stages | tareas fallidas/reintentadas: {tareas_fallidas}")
    except Exception as ex:                                      # noqa: BLE001
        log(f"  (no se pudo leer la Spark UI: {type(ex).__name__})")

    with open(salida / "iteraciones.csv", "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["iteracion", "hora", "segundos", "suma_viajes", "estado"])
        w.writerows(filas)
    (salida / "resultado.json").write_text(json.dumps({
        "filas_antes": int(ref_filas), "filas_despues": int(post_filas),
        "suma_antes": float(ref_total), "suma_despues": float(post_total),
        "iteraciones": n_iter, "errores": errores, "identico": bool(identico),
        "destinos": int(ref_dest), "tareas_fallidas": tareas_fallidas}, indent=2),
        encoding="utf-8")
    guardar_csv(spark.createDataFrame(
        [(a, b, c, float(d_), e_) for a, b, c, d_, e_ in filas],
        "iteracion INT, hora STRING, segundos DOUBLE, suma_viajes DOUBLE, estado STRING"),
        "reto_f_iteraciones")
    if not identico:
        raise RuntimeError("el resultado tras el fallo no coincide con la referencia")


# ==========================================================================
# Informe final: genera informe/informe.html con las tablas y gráficos de todos los apartados
# ==========================================================================
CSS = """
body{font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;margin:0;background:#f6f7f9;color:#1f2430}
main{max-width:1000px;margin:0 auto;padding:24px 16px 64px}
h1{font-size:26px;margin:0 0 4px} h2{font-size:19px;margin:36px 0 8px;border-bottom:2px solid #d9dde5;padding-bottom:4px}
p.sub{color:#5b6475;margin:0 0 16px} p.nota{color:#5b6475;font-size:14px}
table{border-collapse:collapse;font-size:13px;margin:8px 0;background:#fff}
th,td{border:1px solid #d9dde5;padding:4px 8px;text-align:left} th{background:#eef1f6}
td.ok{color:#14753d;font-weight:600} td.mal{color:#b3261e;font-weight:600}
img{max-width:100%;background:#fff;border:1px solid #d9dde5;margin:8px 0}
pre{background:#fff;border:1px solid #d9dde5;padding:8px;font-size:12px;overflow:auto}
.kpis{display:flex;flex-wrap:wrap;gap:12px} .kpi{background:#fff;border:1px solid #d9dde5;padding:10px 14px;min-width:150px}
.kpi b{display:block;font-size:20px}
.aviso{background:#fff7e0;border:1px solid #f0d58a;padding:8px 12px;font-size:14px}
"""


def _png(fig):
    import matplotlib.pyplot as plt
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=110, bbox_inches="tight")
    plt.close(fig)
    return '<img alt="grafico" src="data:image/png;base64,%s">' % base64.b64encode(buf.getvalue()).decode()


def _tabla(df, max_filas=30, estado_col=None):
    if df is None or len(df) == 0:
        return "<p class='nota'>(sin datos)</p>"
    df = df.head(max_filas)
    cab = "".join(f"<th>{html.escape(str(c))}</th>" for c in df.columns)
    filas = []
    for _, r in df.iterrows():
        celdas = []
        for c in df.columns:
            v = r[c]
            if v is None or (isinstance(v, float) and v != v):
                v = ""
            elif isinstance(v, float):
                v = f"{v:,.4f}".rstrip("0").rstrip(".") if abs(v) < 1000 else f"{v:,.0f}"
            cls = ""
            if c == estado_col:
                cls = " class='ok'" if str(v) in ("OK", "INFO") else " class='mal'"
            celdas.append(f"<td{cls}>{html.escape(str(v))}</td>")
        filas.append("<tr>" + "".join(celdas) + "</tr>")
    return f"<table><tr>{cab}</tr>{''.join(filas)}</table>"


def etapa_informe():
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import pandas as pd

    ev = evidence()

    def res(nombre):
        try:
            return (spark.read.option("header", True).option("inferSchema", True)
                    .csv(f"{RESULTS}/{nombre}").toPandas())
        except Exception:                                    # noqa: BLE001
            return None

    def local_csv(ruta):
        try:
            return pd.read_csv(ruta) if Path(ruta).exists() else None
        except Exception:                                    # noqa: BLE001
            return None

    def falta(etapa):
        return f"<p class='aviso'>No hay datos todavía: ejecuta la etapa <code>{etapa}</code>.</p>"

    secciones = []

    def seccion(titulo, cuerpo, nota=""):
        n = f"<p class='nota'>{nota}</p>" if nota else ""
        secciones.append(f"<h2>{html.escape(titulo)}</h2>{n}{cuerpo}")

    # ---- resumen y tiempos
    t = local_csv(ev / "tiempos.csv")
    kpis = []
    cal = res("calidad_pipeline")
    if cal is not None and len(cal):
        ok = int((cal["estado"].isin(["OK", "INFO"])).sum())
        kpis.append(("Controles de calidad OK", f"{ok}/{len(cal)}"))
        fila = cal[cal["control"] == "curated: filas"]
        if len(fila):
            kpis.append(("Filas en curated", f"{int(float(fila.iloc[0]['valor'])):,}"))
    if t is not None and len(t):
        ult = t.groupby("etapa", sort=False).tail(1)
        total = ult[~ult["etapa"].isin(["TOTAL", "f_job"])]["segundos"].sum()
        kpis.append(("Tiempo de etapas (última ejec.)", f"{total/60:.1f} min"))
    kp = "".join(f"<div class='kpi'><b>{html.escape(v)}</b>{html.escape(k)}</div>" for k, v in kpis)
    seccion("Resumen", f"<div class='kpis'>{kp}</div>" if kp else falta("calidad"))

    if t is not None and len(t):
        ult = t[~t["etapa"].isin(["TOTAL", "f_job"])].groupby("etapa", sort=False).tail(1)
        fig, ax = plt.subplots(figsize=(8, 0.4 * len(ult) + 1))
        ax.barh(ult["etapa"][::-1], ult["segundos"][::-1], color="#4c78a8")
        ax.set_xlabel("segundos")
        ax.set_title("Tiempo por etapa del pipeline")
        seccion("Tiempos del pipeline", _png(fig) + _tabla(ult[["etapa", "segundos", "estado"]],
                                                           estado_col="estado"))

    # ---- calidad
    seccion("Calidad de datos", _tabla(cal, 40, "estado") if cal is not None else falta("calidad"),
            "Comprobaciones sobre la tabla curada (nulos, joins, zonas extranjeras...). REVISAR = mirar ese dato.")

    # ---- HDFS
    est = res("hdfs_estado")
    fsck_html = ""
    f_fsck = ev / "fsck_raw.txt"
    if f_fsck.exists():
        lineas = f_fsck.read_text(encoding="utf-8", errors="replace").splitlines()
        fsck_html = ("<p class='nota'>Salida de <code>hdfs fsck 20221101_Viajes_municipios.csv "
                     "-files -blocks -locations</code> (primeras líneas; completa en evidence/fsck_raw.txt):</p>"
                     f"<pre>{html.escape(chr(10).join(lineas[:25]))}</pre>")
    seccion("HDFS: zonas, bloques y réplicas",
            (_tabla(est) if est is not None else falta("hdfs_estado")) + fsck_html,
            "Estado del data lake (raw, curated, results) con la replicación configurada.")

    # ---- analítica
    q1 = res("q1_flujos_ccaa")
    if q1 is not None and len(q1):
        piv = q1.pivot_table(index="ccaa_origen", columns="ccaa_destino", values="viajes",
                             aggfunc="sum", fill_value=0)
        fig, ax = plt.subplots(figsize=(8, 6.5))
        import numpy as np
        im = ax.imshow(np.log10(piv.values + 1), cmap="Blues")
        ax.set_xticks(range(len(piv.columns)))
        ax.set_xticklabels(piv.columns, rotation=90, fontsize=7)
        ax.set_yticks(range(len(piv.index)))
        ax.set_yticklabels(piv.index, fontsize=7)
        ax.set_xlabel("Destino")
        ax.set_ylabel("Origen")
        ax.set_title("Viajes entre comunidades autónomas distintas")
        fig.colorbar(im, ax=ax, shrink=0.7, label="log10(viajes)")
        seccion("P1 · Flujos entre comunidades autónomas",
                _png(fig) + _tabla(q1, 10), "Diez mayores flujos; el mapa de calor usa escala logarítmica.")
    else:
        seccion("P1 · Flujos entre comunidades autónomas", falta("resultados"))
    q2 = res("q2_top3_destinos")
    seccion("P2 · Top destinos de cada día",
            _tabla(q2, 30) if q2 is not None else falta("resultados"))
    q3 = res("q3_top20_desviaciones")
    seccion("P3 · Mayores desviaciones respecto a la media del municipio",
            _tabla(q3, 20) if q3 is not None else falta("resultados"))

    # ---- ML
    ml = res("ml_metricas")
    coef = res("ml_coeficientes")
    cuerpo = _tabla(ml) if ml is not None else falta("ml")
    if coef is not None:
        cuerpo += "<p class='nota'>Coeficientes de las variables con pocas categorías:</p>" + _tabla(coef, 20)
    seccion("Regresión lineal (MLlib)", cuerpo,
            "Pipeline de MLlib: StringIndexer, OneHotEncoder, VectorAssembler y LinearRegression "
            "sobre destino y franja de edad. RMSE y R² se calculan sobre el 20 % de test.")

    # ---- MapReduce vs Spark
    mr = res("comparacion_mr_spark")
    if mr is not None and len(mr):
        g = mr[mr["segundos"].notna()]
        fig, ax = plt.subplots(figsize=(7, 3))
        ax.bar(g["implementacion"], g["segundos"], color="#f58518")
        ax.set_ylabel("segundos")
        ax.set_title("Mismo cálculo: viajes por destino")
        plt.setp(ax.get_xticklabels(), rotation=15, ha="right")
        cuerpo = _png(fig) + _tabla(mr)
        top = res("mr_top10_destinos")
        if top is not None and len(top):
            top["destino"] = top["destino"].astype(str).str.zfill(5)      # 08019, no 8019
            cuerpo += "<p class='nota'>Los 10 destinos con más viajes (7 días):</p>" + _tabla(top, 10)
        cont = res("mr_contadores")
        if cont is not None:
            cuerpo += "<p class='nota'>Contadores del job de Hadoop Streaming:</p>" + _tabla(cont)
    else:
        cuerpo = falta("mapreduce")
    seccion("MapReduce (Hadoop Streaming) frente a Spark RDD", cuerpo,
            "El job de Hadoop corre en YARN con mapper, combiner y reducer en Python; la versión de "
            "Spark usa textFile, flatMap y reduceByKey. Los dos resultados se comparan destino a destino.")

    # ---- formatos y cache
    fm = res("comparacion_formatos")
    if fm is not None and len(fm):
        fig, axs = plt.subplots(1, 2, figsize=(8, 3))
        axs[0].bar(fm["formato"], fm["tamano_bytes"] / 1e9, color="#54a24b")
        axs[0].set_title("Tamaño en HDFS (GB)")
        axs[1].bar(fm["formato"], fm["seg_2a"], color="#e45756")
        axs[1].set_title("Consulta, 2ª ejecución (s)")
        cuerpo = _png(fig) + _tabla(fm)
    else:
        cuerpo = falta("formatos")
    seccion("CSV frente a Parquet", cuerpo)

    ca = res("comparacion_cache")
    if ca is not None and len(ca):
        fig, ax = plt.subplots(figsize=(8, 3.2))
        ax.barh(ca["escenario"][::-1], ca["segundos"][::-1], color="#72b7b2")
        ax.set_xlabel("segundos")
        cuerpo = _png(fig) + _tabla(ca)
    else:
        cuerpo = falta("cache")
    seccion("Efecto de cache()", cuerpo)

    # ---- reto F
    rf = ev / "reto_f"
    it = local_csv(rf / "iteraciones.csv")
    lt = local_csv(rf / "linea_tiempo.csv")
    rj = None
    if (rf / "resultado.json").exists():
        try:
            rj = json.loads((rf / "resultado.json").read_text(encoding="utf-8"))
        except Exception:                                    # noqa: BLE001
            rj = None
    if it is not None and len(it):
        fig, ax = plt.subplots(figsize=(8, 3.2))
        ax.plot(it["iteracion"], it["segundos"], marker="o", color="#4c78a8")
        if lt is not None:
            par = lt[lt["evento"].astype(str).str.contains("docker stop datanode", na=False)]
            if len(par):
                hora_stop = str(par.iloc[0]["hora"])
                despues = it[it["hora"].astype(str) >= hora_stop]
                if len(despues):
                    ax.axvline(despues.iloc[0]["iteracion"], color="#e45756", ls="--")
                    ax.text(despues.iloc[0]["iteracion"], ax.get_ylim()[1], " DataNode parado",
                            color="#e45756", va="top", fontsize=9)
        from matplotlib.ticker import MaxNLocator
        ax.xaxis.set_major_locator(MaxNLocator(integer=True))
        ax.set_xlabel("iteración")
        ax.set_ylabel("segundos")
        ax.set_title("Reto F: duración de cada iteración del job")
        cuerpo = _png(fig)
        if rj:
            cuerpo += ("<p><b>Resultado idéntico antes y después del fallo:</b> "
                       f"{'sí' if rj.get('identico') else 'NO'} "
                       f"(filas {rj.get('filas_antes'):,} → {rj.get('filas_despues'):,}; "
                       f"suma de viajes {rj.get('suma_antes'):,.0f} → {rj.get('suma_despues'):,.0f}"
                       + (f"; tareas de Spark fallidas/reintentadas: {rj['tareas_fallidas']}"
                          if rj.get("tareas_fallidas") is not None else "") + ").</p>")
        if lt is not None:
            cuerpo += "<p class='nota'>Línea de tiempo:</p>" + _tabla(lt, 40)
    else:
        cuerpo = falta("tolerancia")
    seccion("Reto F · Tolerancia a fallos", cuerpo,
            "Se para un DataNode mientras corre un job de Spark: HDFS re-replica los bloques "
            "y Spark termina con el mismo resultado.")

    pagina = (f"<!doctype html><html lang='es'><head><meta charset='utf-8'>"
              f"<meta name='viewport' content='width=device-width,initial-scale=1'>"
              f"<title>Informe del pipeline MITMA</title><style>{CSS}</style></head><body><main>"
              f"<h1>Informe del pipeline MITMA</h1>"
              f"<p class='sub'>Generado el {dt.datetime.now():%Y-%m-%d %H:%M} · ventana "
              f"{F_MIN} a {F_MAX}</p>"
              + "".join(secciones) + "</main></body></html>")
    salida = RAIZ / "informe"
    salida.mkdir(exist_ok=True)
    (salida / "informe.html").write_text(pagina, encoding="utf-8")
    log(f"  informe escrito en {salida / 'informe.html'}")


FUNCIONES = {"ingesta": etapa_ingesta, "hdfs": etapa_hdfs, "etl": etapa_etl,
             "calidad": etapa_calidad, "formatos": etapa_formatos,
             "hdfs_estado": etapa_hdfs_estado, "mapreduce": etapa_mapreduce,
             "resultados": etapa_resultados, "cache": etapa_cache, "ml": etapa_ml,
             "f_job": etapa_f_job, "informe": etapa_informe}


# ---------------------------------------------------------------------- main
def main():
    global N_ITER
    ap = argparse.ArgumentParser(description="Pipeline MITMA")
    ap.add_argument("--etapas", default=",".join(ETAPAS),
                    help=f"lista separada por comas de etapas ({', '.join(ORDEN)}) "
                         f"o de apartados ({', '.join(GRUPOS)})")
    ap.add_argument("--iter", type=int, default=N_ITER, help="iteraciones del job de f_job (reto F)")
    args = ap.parse_args()
    N_ITER = args.iter

    pedidas = []
    for e in (x.strip() for x in args.etapas.split(",") if x.strip()):
        pedidas += GRUPOS[e.upper()] if e.upper() in GRUPOS else [e]
    desconocidas = [e for e in pedidas if e not in FUNCIONES]
    if desconocidas:
        ap.error(f"etapas desconocidas: {desconocidas}")
    etapas = [e for e in ORDEN if e in pedidas]       # siempre en el orden del flujo
    log(f"Etapas: {', '.join(etapas)}")

    tiempos, codigo = [], 0
    t_total = time.perf_counter()
    for e in etapas:
        log(f"=== {e.upper()} ===")
        t = time.perf_counter()
        estado = "OK"
        try:
            FUNCIONES[e]()
        except Exception as ex:                          # noqa: BLE001
            estado, codigo = "ERROR", 1
            log(f"  ERROR en {e}: {type(ex).__name__}: {ex}")
        tiempos.append((e, round(time.perf_counter() - t, 1), estado))
        log(f"  {e}: {tiempos[-1][1]} s [{estado}]")
        if codigo:
            break

    tiempos.append(("TOTAL", round(time.perf_counter() - t_total, 1), "OK" if not codigo else "ERROR"))
    registrar_tiempos(tiempos)
    try:
        tabla = spark.createDataFrame(tiempos, "etapa STRING, segundos DOUBLE, estado STRING")
        tabla.show(truncate=False)
        guardar_csv(tabla, "pipeline_tiempos")
    except Exception as ex:                              # noqa: BLE001
        log(f"No se pudo guardar pipeline_tiempos: {ex}")
    pausa = int(S.get("ui_pausa_segundos", 0))
    if pausa > 0:                    # deja viva la Spark UI para hacer capturas (http://localhost:4040)
        log(f"  Spark UI abierta {pausa} s en http://localhost:4040 (haz las capturas ahora)")
        time.sleep(pausa)
    spark.stop()
    sys.exit(codigo)


if __name__ == "__main__":
    main()
