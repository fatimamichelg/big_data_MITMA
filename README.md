# Movilidad en España con Big Data (MITMA) — Proyecto Big Data & Cloud Computing
Fatima, Telmo y Aimar

Pipeline de datos sobre los viajes diarios entre municipios publicados por el Ministerio de
Transportes (MITMA). 

## Estructura

```
.
├── run_pipeline.sh            # punto de entrada: orquesta todo (A → F + informe)
├── docker-compose.yml         # clúster Hadoop + Spark (perfil "reto" → datanode3)
├── Dockerfile.spark / Dockerfile.nodemanager
├── pipeline/
│   ├── run_pipeline.py        # TODO el código Spark, ordenado por apartados A–F
│   └── download_mitma.py      # descarga y descomprime los CSV de MITMA
├── hadoop_streaming/
│   ├── mapper.py              # MapReduce con Hadoop Streaming (Python)
│   └── reducer.py
├── aux_tables/                # tabla auxiliar de municipios
├── data/raw/                  # CSV diarios (no se suben a GitHub; los descarga el pipeline)
├── notebooks/                 # exploración previa (1_setup_A, 6_reto_F_tolerancia)
├── evidence/                  # evidencias generadas al ejecutar (tiempos, fsck, logs, reto F)
└── informe/informe.html       # informe generado con tablas y gráficos
```

## Cómo ejecutar paso a paso

```bash
git clone https://github.com/fatimamichelg/big_data_MITMA
cd big_data_MITMA
docker compose up -d        # levanta el clúster
chmod +x run_pipeline.sh    # solo la primera vez
./run_pipeline.sh           # ejecuta todo (apartados A a F + informe)
```

Al terminar se puede abrir el informe (opcional, ya que sus resultados están incluidos en la memoria):

```bash
open informe/informe.html
```

## Otras variantes

```bash
./run_pipeline.sh --sin-reto            # todo menos el reto F (más rápido)
./run_pipeline.sh --etapas C            # un apartado entero (letra A..F)
./run_pipeline.sh --etapas etl,calidad  # etapas concretas
./run_pipeline.sh --help
```
## Archivos del repositorio

| Archivo | Descripción |
|---|---|
| `README.md` | Presentación del proyecto y cómo ejecutarlo |
| `run_pipeline.sh` | Punto de entrada. Lanza todas las etapas en orden y orquesta las que usan Docker (fsck, Hadoop Streaming y reto F) |
| `pipeline/run_pipeline.py` | Todo el código de Spark ordenado por apartados A–F: ingesta, ETL, calidad, formatos, estado de HDFS, MapReduce con RDD, preguntas P1–P3, cache, ML, job del reto F e informe |
| `pipeline/download_mitma.py` | Descarga y descomprime los CSV de MITMA (reproduce la obtención de los datos) |
| `hadoop_streaming/mapper.py` | Fase *map* del job de Hadoop Streaming: emite destino y número de viajes |
| `hadoop_streaming/reducer.py` | Fase *reduce* (y combiner): suma los viajes por destino |
| `aux_tables/nombres_municipios_ampliado.csv` | Tabla auxiliar para el join: código de municipio, nombre, provincia y CCAA |
| `docker-compose.yml` | Define el clúster: NameNode, 2 DataNodes, YARN, HistoryServer, `mitma-spark` y el `datanode3` opcional (perfil `reto`) |
| `hadoop.env` | Configuración de Hadoop que leen los contenedores (replicación, tiempos de latido...) |
| `Dockerfile.spark` | Imagen del contenedor `mitma-spark` (Jupyter con PySpark) |
| `Dockerfile.nodemanager` | Imagen del NodeManager con Python 3, necesario para ejecutar `mapper.py` y `reducer.py` en YARN |
| `.gitignore` | Archivos que no se suben al repositorio (CSV de `data/`, cachés...) |
| `notebooks/1_setup_A.ipynb` | Exploración previa: lectura, limpieza, joins, Parquet, P1–P3 y cache |
| `notebooks/6_reto_F_tolerancia.ipynb` | Reto F paso a paso |
| `evidence/` | Evidencias generadas al ejecutar: tiempos, `fsck`, logs de Hadoop Streaming, plan de P2 y carpeta `reto_f/` |
| `informe/informe.html` | Informe final con tablas y gráficos |
