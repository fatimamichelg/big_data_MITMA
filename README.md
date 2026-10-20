# Movilidad en España con Big Data (MITMA) — Proyecto Big Data & Cloud Computing
Fatima, Telmo y Aimar

Pipeline de datos sobre los viajes diarios entre municipios publicados por el Ministerio de
Transportes (MITMA). 

## Estructura

```
.
├── run_pipeline.sh              # punto de entrada: (A → F + informe)
├── config.yaml                  # rutas y parámetros; lo leen run_pipeline.sh y run_pipeline.py
├── docker-compose.yml           # clúster Hadoop + Spark 
├── Dockerfile.spark             # imagen de Spark / Jupyter (spark-master, spark-worker, mitma-spark)
├── Dockerfile.nodemanager       # imagen del NodeManager de YARN
├── pipeline/
│   ├── run_pipeline.py          # todo el código Spark, ordenado por apartados A–F
│   └── download_mitma.py        # descarga y descomprime los CSV de MITMA
├── hadoop_streaming/
│   ├── mapper.py                # MapReduce con Hadoop Streaming (Python)
│   └── reducer.py               # también se usa como combiner
├── aux_tables/                  # tabla auxiliar de municipios
├── data/raw/                    # CSV diarios (no se suben a GitHub; los descarga el pipeline)
├── notebooks/                   # exploración previa (1_setup_A, 6_reto_F_tolerancia)
├── evidence/                    # evidencias generadas al ejecutar (tiempos, fsck, logs, reto F)
│   └── capturas/                # capturas de pantalla (Spark UI, HDFS UI, master)
└── informe/
    └── informe.html             # informe generado con tablas y gráficos
```

## Cómo ejecutar paso a paso

**1. Clonar el repositorio y levantar el clúster**

```bash
git clone https://github.com/fatimamichelg/big_data_MITMA
cd big_data_MITMA
docker compose up -d
docker compose ps
```

Espera 1 o 2 minutos hasta que todos los servicios estén en marcha.

**2. Lanzar el pipeline** (unos 35–40 minutos)

| Sistema | Qué usar |
|---|---|
| macOS / Linux | La terminal normal: `bash run_pipeline.sh` |
| Windows | Docker Desktop abierto y **Git para Windows** instalado. Desde PowerShell, dentro de la carpeta del repositorio: `& "C:\Program Files\Git\bin\bash.exe" run_pipeline.sh` (probado en Windows). También funciona desde WSL2 con Docker Desktop |
```

Al terminar se puede abrir el informe (opcional, ya que sus resultados están incluidos en la memoria):

```bash
open informe/informe.html
```

## Otras variantes

```bash
bash run_pipeline.sh --sin-reto
bash run_pipeline.sh --etapas C
bash run_pipeline.sh --etapas etl,calidad
```
## Archivos del repositorio

| Archivo | Descripción |
|---|---|
| `README.md` | Presentación del proyecto y cómo ejecutarlo |
| `run_pipeline.sh` | Punto de entrada. Lanza todas las etapas en orden y orquesta las que usan Docker (fsck, Hadoop Streaming y reto F). Lee sus parámetros de `config.yaml` |
| `config.yaml` | Configuración única del pipeline: rutas de HDFS, ventana de fechas, parámetros de Spark (master, memoria, cores), análisis, ML, MapReduce y reto F. Lo leen `run_pipeline.sh` y `pipeline/run_pipeline.py`, así que ninguno de los dos lleva rutas escritas |
| `pipeline/run_pipeline.py` | Todo el código de Spark ordenado por apartados A–F: ingesta, ETL con esquema `StructType`, calidad (controles con acción definida e idempotencia), formatos, estado de HDFS, MapReduce con RDD, preguntas P1–P3 con `explain()`, cache, ML con línea base, job del reto F e informe |
| `pipeline/download_mitma.py` | Descarga y descomprime los CSV de MITMA (reproduce la obtención de los datos) |
| `hadoop_streaming/mapper.py` | Fase *map* del job de Hadoop Streaming: emite destino y número de viajes |
| `hadoop_streaming/reducer.py` | Fase *reduce* (y combiner): suma los viajes por destino |
| `aux_tables/nombres_municipios_ampliado.csv` | Tabla auxiliar para el join: código de municipio, nombre, provincia y CCAA |
| `docker-compose.yml` | Define el clúster: NameNode, 2 DataNodes, YARN, HistoryServer, clúster Spark standalone (`spark-master` y `spark-worker`), `mitma-spark` (driver con Jupyter) y el `datanode3` opcional (perfil `reto`) |
| `hadoop.env` | Configuración de Hadoop que leen los contenedores (replicación, tiempos de latido...) |
| `Dockerfile.spark` | Imagen compartida por `spark-master`, `spark-worker` y `mitma-spark` (Spark, PySpark, Jupyter y PyYAML) |
| `Dockerfile.nodemanager` | Imagen del NodeManager con Python 3, necesario para ejecutar `mapper.py` y `reducer.py` en YARN |
| `.gitignore` | Archivos que no se suben al repositorio (CSV de `data/`, copias `*.bak`, cachés...) |
| `notebooks/1_setup_A.ipynb` | Exploración previa: lectura, limpieza, joins, Parquet, P1–P3 y cache |
| `notebooks/6_reto_F_tolerancia.ipynb` | Reto F paso a paso |
| `evidence/` | Evidencias generadas al ejecutar: tiempos, `fsck` y listado de HDFS, logs de Hadoop Streaming, planes de ejecución de P1–P3, control de idempotencia y carpeta `reto_f/` |
| `evidence/capturas/` | Capturas de pantalla: Spark master (worker ALIVE y aplicación completada), Spark UI y HDFS UI |
| `informe/informe.html` | Informe final con tablas y gráficos |
