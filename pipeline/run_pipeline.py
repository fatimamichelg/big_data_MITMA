#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Pipeline MITMA — punto de entrada único.

    ingesta -> HDFS (raw) -> ETL Spark (curated) -> calidad(nulls, rangos, duplciates)-> resultados -> ML

Se ejecuta DENTRO del contenedor mitma-spark; desde el Mac basta con:

    ./run_pipeline.sh                       # todas las etapas
    ./run_pipeline.sh --etapas etl,calidad  # solo algunas

Todo lo configurable (rutas, fechas, columna de partición, umbrales, split del
modelo...) está en pipeline/config.yaml; aquí no hay rutas escritas.

Idempotencia (ejecutarlo dos veces no duplica ni corrompe nada):
  * hdfs      : un fichero ya subido con el mismo tamaño se salta; la subida va a un
                nombre temporal oculto y se renombra al terminar.
  * etl       : Parquet con partitionOverwriteMode=dynamic -> se reescriben las
                particiones (días) de la ventana, nunca se añaden filas encima.
  * resultados: siempre mode("overwrite") sobre la misma carpeta.
  * calidad   : guarda una "huella" (filas, suma de viajes) y la compara con la de
                la ejecución anterior.
"""
import argparse
import datetime as dt
import os
import sys
import time
from pathlib import Path

import yaml

os.environ.setdefault("HADOOP_USER_NAME", "root")
sys.path.insert(0, str(Path(__file__).resolve().parent))

from pyspark.ml import Pipeline                                   # noqa: E402
from pyspark.ml.evaluation import RegressionEvaluator              # noqa: E402
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler  # noqa: E402
from pyspark.ml.regression import LinearRegression                 # noqa: E402
from pyspark.sql import SparkSession, Window, functions as F       # noqa: E402

ETAPAS = ["ingesta", "hdfs", "etl", "calidad", "resultados", "ml"]


class CalidadError(Exception):
    """Algún control de calidad de severidad 'error' ha fallado."""


# ---------------------------------------------------------------- utilidades
def log(msg):
    print(f"[{dt.datetime.now():%H:%M:%S}] {msg}", flush=True)


def cargar_config(ruta):
    ruta = Path(ruta).resolve()
    cfg = yaml.safe_load(ruta.read_text(encoding="utf-8"))
    cfg["_raiz"] = ruta.parent.parent          # el config vive en <raiz>/pipeline/
    return cfg


def p_local(cfg, rel):
    p = Path(rel)
    return p if p.is_absolute() else cfg["_raiz"] / p


def p_hdfs(cfg, zona, *partes):
    base = cfg["hdfs"]["uri"].rstrip("/") + cfg["hdfs"]["zonas"][zona]
    return "/".join([base, *partes])


def fechas_ventana(cfg):
    d0 = dt.date.fromisoformat(str(cfg["ventana"]["fecha_min"]))
    d1 = dt.date.fromisoformat(str(cfg["ventana"]["fecha_max"]))
    return [d0 + dt.timedelta(days=i) for i in range((d1 - d0).days + 1)]


def nombre_fichero(cfg, fecha):
    return cfg["datos"]["patron_fichero"].format(fecha=f"{fecha:%Y%m%d}")


def crear_spark(cfg):
    s = cfg["spark"]
    spark = (SparkSession.builder.master(s["master"]).appName("mitma-pipeline")
             .config("spark.driver.memory", s["memoria_driver"])
             .config("spark.sql.shuffle.partitions", str(s["shuffle_partitions"]))
             .config("spark.hadoop.dfs.replication", str(cfg["hdfs"]["replicacion"]))
             .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    return spark


class Hdfs:
    """Operaciones sobre el sistema de ficheros de Hadoop """

    def __init__(self, spark, uri):
        jvm = spark._jvm
        self.Path = jvm.org.apache.hadoop.fs.Path
        self.fs = jvm.org.apache.hadoop.fs.FileSystem.get(
            jvm.java.net.URI(uri), spark._jsc.hadoopConfiguration())

    def existe(self, ruta):
        return self.fs.exists(self.Path(ruta))

    def tam(self, ruta):
        return self.fs.getFileStatus(self.Path(ruta)).getLen()

    def mkdirs(self, ruta):
        self.fs.mkdirs(self.Path(ruta))

    def subir(self, local, destino):
        """Copia a un temporal oculto y renombra: nunca queda un fichero a medias."""
        carpeta, nombre = destino.rsplit("/", 1)
        tmp = self.Path(f"{carpeta}/.{nombre}.tmp")
        self.fs.copyFromLocalFile(False, True, self.Path("file://" + str(local)), tmp)
        dst = self.Path(destino)
        if self.fs.exists(dst):
            self.fs.delete(dst, False)
        if not self.fs.rename(tmp, dst):
            raise RuntimeError(f"No se pudo renombrar {tmp} -> {dst}")


def guardar_csv(cfg, df, nombre):
    destino = p_hdfs(cfg, "results", nombre)
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(destino)
    log(f"  -> {destino}")


def leer_raw(cfg, spark):
    return (spark.read.schema(cfg["esquema"])
            .option("header", True).option("sep", "|")
            .option("dateFormat", "yyyyMMdd")
            .csv(p_hdfs(cfg, "raw", cfg["hdfs"]["subcarpeta_viajes"])))


def leer_curated(cfg, spark):
    df = spark.read.parquet(p_hdfs(cfg, "curated", cfg["hdfs"]["subcarpeta_viajes"]))
    return df.withColumn("fecha", F.to_date("fecha"))


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


# -------------------------------------------------------------------- etapas
def etapa_ingesta(cfg, spark, ctx):
    d = p_local(cfg, cfg["datos"]["dir_local"])
    fechas = fechas_ventana(cfg)

    def faltan():
        return [f for f in fechas if not (d / nombre_fichero(cfg, f)).exists()]

    pendientes = faltan()
    if pendientes and cfg["ingesta"]["descargar"]:
        from download_mitma import descargar_dias
        log(f"  descargando {len(pendientes)} día(s) que faltan en {d}")
        descargar_dias(pendientes, d, cfg["ingesta"]["url_base"], cfg["datos"]["patron_fichero"])
        pendientes = faltan()
    if pendientes:
        raise FileNotFoundError(
            f"Faltan en {d}: {[nombre_fichero(cfg, f) for f in pendientes]}. "
            "Colócalos ahí o pon ingesta.descargar: true en config.yaml")
    aux = p_local(cfg, cfg["datos"]["tabla_aux_local"])
    if not aux.exists():
        raise FileNotFoundError(f"Falta la tabla auxiliar: {aux}")
    log(f"  {len(fechas)} ficheros diarios + tabla auxiliar disponibles en local")


def etapa_hdfs(cfg, spark, ctx):
    h = ctx["hdfs"]
    d = p_local(cfg, cfg["datos"]["dir_local"])
    dir_viajes = p_hdfs(cfg, "raw", cfg["hdfs"]["subcarpeta_viajes"])
    dir_zonif = p_hdfs(cfg, "raw", cfg["hdfs"]["subcarpeta_zonificacion"])
    for z in ("raw", "curated", "results"):
        h.mkdirs(p_hdfs(cfg, z))
    h.mkdirs(dir_viajes)
    h.mkdirs(dir_zonif)

    subidos = saltados = 0
    pares = [(d / nombre_fichero(cfg, f), f"{dir_viajes}/{nombre_fichero(cfg, f)}")
             for f in fechas_ventana(cfg)]
    aux = p_local(cfg, cfg["datos"]["tabla_aux_local"])
    pares.append((aux, f"{dir_zonif}/{aux.name}"))

    for local, destino in pares:
        if h.existe(destino) and h.tam(destino) == local.stat().st_size:
            saltados += 1
            log(f"  = {local.name} ya está en HDFS (mismo tamaño), se salta")
        else:
            t = time.perf_counter()
            h.subir(local, destino)
            subidos += 1
            log(f"  + {local.name} subido ({time.perf_counter() - t:.0f} s)")
    log(f"  subidos: {subidos} | saltados: {saltados}")


def etapa_etl(cfg, spark, ctx):
    l = cfg["limpieza"]
    fmin, fmax = str(cfg["ventana"]["fecha_min"]), str(cfg["ventana"]["fecha_max"])

    # EXTRACT: esquema explícito, nada de inferSchema
    viajes = leer_raw(cfg, spark)

    # TRANSFORM: limpieza
    limpio = (viajes
              .dropna(subset=["fecha", "periodo", "origen", "destino", "viajes"])
              .filter(F.col("periodo").between(l["periodo_min"], l["periodo_max"]))
              .filter(F.col("viajes") > 0)
              .filter(F.col("viajes_km").isNull() | (F.col("viajes_km") >= 0))
              .filter(F.col("fecha").between(fmin, fmax)))

    # TRANSFORM: dos joins (origen y destino) con la misma tabla auxiliar
    aux = p_hdfs(cfg, "raw", cfg["hdfs"]["subcarpeta_zonificacion"],
                 p_local(cfg, cfg["datos"]["tabla_aux_local"]).name)
    muni = (spark.read.option("header", True).option("sep", "|").csv(aux)
            .select(F.col("ID").alias("codigo"), F.col("name").alias("nombre"),
                    "provincia", "ccaa"))

    def con_nombre(df, col):
        m = muni.select(F.col("codigo").alias(col),
                        F.col("nombre").alias(f"nombre_{col}"),
                        F.col("provincia").alias(f"nombre_provincia_{col}"),
                        F.col("ccaa").alias(f"ccaa_{col}"))
        return df.join(F.broadcast(m), on=col, how="left")

    curated = (con_nombre(con_nombre(limpio, "origen"), "destino")
               .withColumn("provincia_origen", F.substring("origen", 1, 2))
               .withColumn("provincia_destino", F.substring("destino", 1, 2)))

    # LOAD: Parquet particionado; dynamic overwrite = idempotente
    destino = p_hdfs(cfg, "curated", cfg["hdfs"]["subcarpeta_viajes"])
    (curated.write.mode("overwrite")
            .partitionBy(cfg["particion"]["columna"])
            .parquet(destino))
    log(f"  curated escrito en {destino} (partición: {cfg['particion']['columna']})")


def etapa_calidad(cfg, spark, ctx):
    q, l = cfg["calidad"], cfg["limpieza"]
    fmin, fmax = str(cfg["ventana"]["fecha_min"]), str(cfg["ventana"]["fecha_max"])
    pmin, pmax = l["periodo_min"], l["periodo_max"]
    claves = ["fecha", "periodo", "origen", "destino", "viajes"]
    controles = []                       # (control, valor, esperado, severidad, estado)

    def chk(nombre, valor, ok, esperado, severidad="error"):
        if severidad == "info":
            estado = "INFO"
        else:
            estado = "OK" if ok else ("FALLO" if severidad == "error" else "AVISO")
        controles.append((nombre, str(valor), esperado, severidad, estado))

    # --- 1) Raw
    raw = leer_raw(cfg, spark)
    r = raw.agg(
        F.count(F.lit(1)).alias("filas"),
        *[F.sum(F.col(c).isNull().cast("int")).alias(f"nulos_{c}") for c in claves],
        F.sum((F.col("viajes") <= 0).cast("int")).alias("viajes_no_positivo"),
        F.sum((~F.col("periodo").between(pmin, pmax)).cast("int")).alias("periodo_fuera"),
        F.sum((F.col("viajes_km") < 0).cast("int")).alias("km_negativo"),
        F.sum((~F.col("fecha").between(fmin, fmax)).cast("int")).alias("fecha_fuera"),
    ).first().asDict()
    filas_raw = r["filas"]
    chk("raw: filas leídas", filas_raw, filas_raw > 0, "> 0", "error")
    for c in claves:
        chk(f"raw: nulos en {c}", r[f"nulos_{c}"] or 0, True, "informativo", "info")
    for k in ("viajes_no_positivo", "periodo_fuera", "km_negativo", "fecha_fuera"):
        chk(f"raw: {k}", r[k] or 0, True, "informativo", "info")

    # --- 2) Zona curated: tras la limpieza no debe quedar nada raro so solo vemos si esta bien
    cur = leer_curated(cfg, spark)
    a = cur.agg(
        F.count(F.lit(1)).alias("filas"),
        F.countDistinct("fecha").alias("fechas"),
        F.sum("viajes").alias("suma_viajes"),
        *[F.sum(F.col(c).isNull().cast("int")).alias(f"nulos_{c}") for c in claves],
        F.sum((F.col("viajes") <= 0).cast("int")).alias("viajes_no_positivo"),
        F.sum((~F.col("periodo").between(pmin, pmax)).cast("int")).alias("periodo_fuera"),
        F.sum((F.col("viajes_km") < 0).cast("int")).alias("km_negativo"),
        F.sum((~F.col("fecha").between(fmin, fmax)).cast("int")).alias("fecha_fuera"),
        F.sum((((F.col("nombre_origen").isNull()) & ~extranjero("origen")) |
               ((F.col("nombre_destino").isNull()) & ~extranjero("destino"))).cast("int")
              ).alias("sin_nombre_no_extranjero"),
        F.sum((((F.col("ccaa_origen").isNull()) & ~extranjero("origen")) |
               ((F.col("ccaa_destino").isNull()) & ~extranjero("destino"))).cast("int")
              ).alias("sin_ccaa_no_extranjero"),
    ).first().asDict()
    filas, suma = a["filas"], a["suma_viajes"] or 0.0
    n_esperadas = len(fechas_ventana(cfg))

    chk("curated: filas", filas, filas > 0, "> 0")
    chk("curated: días (particiones)", a["fechas"], a["fechas"] == n_esperadas, f"== {n_esperadas}")
    for c in claves:
        chk(f"curated: nulos en {c}", a[f"nulos_{c}"] or 0, (a[f"nulos_{c}"] or 0) == 0, "== 0")
    for k in ("viajes_no_positivo", "periodo_fuera", "km_negativo", "fecha_fuera",
              "sin_nombre_no_extranjero", "sin_ccaa_no_extranjero"):
        chk(f"curated: {k}", a[k] or 0, (a[k] or 0) == 0, "== 0")

    pct = round(100.0 * (filas_raw - filas) / filas_raw, 3) if filas_raw else 0.0
    chk("limpieza: % filas descartadas", pct, pct <= q["max_pct_descartadas"],
        f"<= {q['max_pct_descartadas']}")

    # --- 3) Duplicados 
    d = q["duplicados"]
    if d["activar"]:
        dias = [str(x) for x in d["fechas"]]
        dup = (cur.filter(F.col("fecha").cast("string").isin(dias))
                  .groupBy(*d["clave"]).count().filter("count > 1").count())
        chk(f"duplicados en clave completa ({', '.join(dias)})", dup, dup == 0, "== 0",
            d["severidad"])

    # --- 4)Comprobar la idempotencia 
    ruta_huella = p_hdfs(cfg, "results", "pipeline_huella")
    huella_ant = None
    if ctx["hdfs"].existe(ruta_huella):          # en la primera ejecución aún no existe
        h = spark.read.option("header", True).csv(ruta_huella).first()
        huella_ant = (int(h["filas"]), float(h["suma_viajes"]))
    if huella_ant is None:
        chk("huella respecto a la ejecución anterior", "primera ejecución", True, "-", "info")
    else:
        igual = (huella_ant[0] == filas and
                 abs(huella_ant[1] - suma) <= 1e-9 * max(1.0, abs(suma)))
        chk("huella respecto a la ejecución anterior",
            "idéntica" if igual else f"DISTINTA (antes {huella_ant[0]} filas)",
            igual, "idéntica", "aviso")
    huella = spark.createDataFrame([(int(filas), float(suma), int(a["fechas"]))],
                                   "filas LONG, suma_viajes DOUBLE, fechas INT")
    huella.coalesce(1).write.mode("overwrite").option("header", True).csv(ruta_huella)

    tabla = spark.createDataFrame(
        controles, "control STRING, valor STRING, esperado STRING, severidad STRING, estado STRING")
    tabla.show(200, truncate=False)
    guardar_csv(cfg, tabla, "calidad_pipeline")

    fallos = [c for c in controles if c[4] == "FALLO"]
    avisos = [c for c in controles if c[4] == "AVISO"]
    log(f"  controles: {len(controles)} | fallos: {len(fallos)} | avisos: {len(avisos)}")
    if fallos and q["abortar_si_error"]:
        raise CalidadError("Controles fallidos: " + "; ".join(c[0] for c in fallos))


def etapa_resultados(cfg, spark, ctx):
    a = cfg["analitica"]
    c = retocar(leer_curated(cfg, spark))
    nac = c.filter(~F.col("origen_extranjero") & ~F.col("destino_extranjero"))

    # P1 (agrupación): flujos entre comunidades autónomas distintas
    inter = nac.filter(F.col("ccaa_origen") != F.col("ccaa_destino"))
    total_inter = inter.agg(F.sum("viajes")).first()[0]
    q1 = (inter.groupBy("ccaa_origen", "ccaa_destino").agg(F.sum("viajes").alias("viajes"))
               .withColumn("pct_interccaa", F.round(100 * F.col("viajes") / total_inter, 2))
               .withColumn("viajes", F.round("viajes").cast("long"))
               .orderBy(F.col("viajes").desc()))
    guardar_csv(cfg, q1, "q1_flujos_ccaa")

    # Agregado diario por municipio de destino: lo comparten P2 y P3 -> cache()
    diario = (c.filter(~F.col("destino_extranjero"))
               .groupBy("fecha", "destino", "nombre_destino")
               .agg(F.sum("viajes").alias("viajes"))).cache()
    diario.count()                       # materializa la caché (unas 18.000 filas)

    # P2 (ventana, ranking): top N destinos de cada día
    w = Window.partitionBy("fecha").orderBy(F.col("viajes").desc())
    q2 = (diario.withColumn("puesto", F.rank().over(w))
                .filter(F.col("puesto") <= a["top_destinos"])
                .withColumn("viajes", F.round("viajes").cast("long"))
                .orderBy("fecha", "puesto"))
    guardar_csv(cfg, q2, "q2_top3_destinos")

    # P3 (ventana, comparación con el grupo): desviación respecto a la media del municipio
    w2 = Window.partitionBy("destino")
    q3 = (diario.withColumn("media_muni", F.avg("viajes").over(w2))
                .withColumn("desviacion", F.col("viajes") - F.col("media_muni"))
                .withColumn("desv_pct", F.round(100 * F.col("desviacion") / F.col("media_muni"), 1)))

    def a_enteros(df):
        return (df.withColumn("viajes", F.round("viajes").cast("long"))
                  .withColumn("media_muni", F.round("media_muni").cast("long"))
                  .withColumn("desviacion", F.round("desviacion").cast("long")))

    guardar_csv(cfg, a_enteros(q3), "q3_desviacion_municipio")
    top = (q3.filter(F.col("media_muni") >= a["umbral_media_q3"])
             .orderBy(F.abs("desv_pct").desc()).limit(a["top_n_q3"]))
    guardar_csv(cfg, a_enteros(top), "q3_top20_desviaciones")
    diario.unpersist()


def etapa_ml(cfg, spark, ctx):
    m = cfg["ml"]
    if not m["activar"]:
        log("  ml.activar = false, se omite")
        return
    obj, feats = m["objetivo"], list(m["features_categoricas"])
    corte = F.lit(str(m["train_hasta"])).cast("date")

    # Unidad de análisis: viajes por (día, municipio de destino, franja de edad)
    c = retocar(leer_curated(cfg, spark))
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

    media_glob = train.agg(F.avg(obj)).first()[0]
    media_seg = train.groupBy(*feats).agg(F.avg(obj).alias("pred_seg"))
    base_seg = (test.join(media_seg, on=feats, how="left")
                    .withColumn("prediction", F.coalesce("pred_seg", F.lit(media_glob))))
    base_glob = test.withColumn("prediction", F.lit(float(media_glob)))

    def metricas(df, nombre):
        ev = RegressionEvaluator(labelCol=obj, predictionCol="prediction")
        return (nombre,
                float(ev.setMetricName("rmse").evaluate(df)),
                float(ev.setMetricName("mae").evaluate(df)),
                float(ev.setMetricName("r2").evaluate(df)),
                int(n_train), int(n_test))

    filas = [metricas(pred, "regresion_lineal"),
             metricas(base_seg, "baseline_media_" + "_".join(feats)),
             metricas(base_glob, "baseline_media_global")]
    tabla = spark.createDataFrame(
        filas, "modelo STRING, rmse DOUBLE, mae DOUBLE, r2 DOUBLE, filas_train LONG, filas_test LONG")
    tabla.show(truncate=False)
    guardar_csv(cfg, tabla, "ml_metricas")

    # Coeficientes de las variables 
    lm = modelo.stages[-1]
    coefs = lm.coefficients.toArray()
    etiquetas = modelo.stages[0].labelsArray
    filas_coef, pos = [("intercepto", "", float(lm.intercept))], 0
    for f, labs in zip(feats, etiquetas):
        if len(labs) <= 50:
            filas_coef += [(f, str(lab), float(coefs[pos + i])) for i, lab in enumerate(labs)]
        pos += len(labs)
    guardar_csv(cfg, spark.createDataFrame(
        filas_coef, "variable STRING, categoria STRING, coeficiente DOUBLE"), "ml_coeficientes")
    base.unpersist()


FUNCIONES = {"ingesta": etapa_ingesta, "hdfs": etapa_hdfs, "etl": etapa_etl,
             "calidad": etapa_calidad, "resultados": etapa_resultados, "ml": etapa_ml}


# ---------------------------------------------------------------------- main
def main():
    ap = argparse.ArgumentParser(description="Pipeline MITMA")
    ap.add_argument("--config", default=str(Path(__file__).resolve().parent / "config.yaml"))
    ap.add_argument("--etapas", default=",".join(ETAPAS),
                    help=f"lista separada por comas de: {', '.join(ETAPAS)}")
    args = ap.parse_args()

    etapas = [e.strip() for e in args.etapas.split(",") if e.strip()]
    desconocidas = [e for e in etapas if e not in FUNCIONES]
    if desconocidas:
        ap.error(f"etapas desconocidas: {desconocidas}")
    etapas = [e for e in ETAPAS if e in etapas]       # siempre en el orden del flujo

    cfg = cargar_config(args.config)
    log(f"Config: {args.config} | etapas: {', '.join(etapas)}")
    spark = crear_spark(cfg)
    ctx = {"hdfs": Hdfs(spark, cfg["hdfs"]["uri"])}

    tiempos, codigo = [], 0
    t_total = time.perf_counter()
    for e in etapas:
        log(f"=== {e.upper()} ===")
        t = time.perf_counter()
        estado = "OK"
        try:
            FUNCIONES[e](cfg, spark, ctx)
        except CalidadError as ex:
            estado, codigo = "FALLO_CALIDAD", 1
            log(f"  {ex}")
        except Exception as ex:                          # noqa: BLE001
            estado, codigo = "ERROR", 1
            log(f"  ERROR en {e}: {type(ex).__name__}: {ex}")
        tiempos.append((e, round(time.perf_counter() - t, 1), estado))
        log(f"  {e}: {tiempos[-1][1]} s [{estado}]")
        if codigo:
            break

    tiempos.append(("TOTAL", round(time.perf_counter() - t_total, 1), "OK" if not codigo else "ERROR"))
    try:
        tabla = spark.createDataFrame(tiempos, "etapa STRING, segundos DOUBLE, estado STRING")
        tabla.show(truncate=False)
        guardar_csv(cfg, tabla, "pipeline_tiempos")
    except Exception as ex:                              # noqa: BLE001
        log(f"No se pudo guardar pipeline_tiempos: {ex}")
    spark.stop()
    sys.exit(codigo)


if __name__ == "__main__":
    main()
