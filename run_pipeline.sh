#!/usr/bin/env bash
# Punto de entrada del pipeline MITMA.
# Ejecuta todo el flujo dentro del contenedor mitma-spark:
#   ingesta -> HDFS -> ETL Spark -> calidad -> resultados -> ML
#
# Uso (desde la raíz del proyecto, con `docker compose up -d` hecho):
#   ./run_pipeline.sh                        # todas las etapas
#   ./run_pipeline.sh --etapas etl,calidad   # solo algunas
set -euo pipefail
cd "$(dirname "$0")"

# `docker exec` no hereda el PYTHONPATH que usa el kernel de Jupyter, así que
# lo fijamos aquí apuntando al pyspark que trae Spark en el contenedor.
docker exec mitma-spark bash -c '
  export PYTHONPATH="$SPARK_HOME/python:$(ls "$SPARK_HOME"/python/lib/py4j-*-src.zip 2>/dev/null | head -1)"
  exec python /home/jovyan/work/pipeline/run_pipeline.py \
       --config /home/jovyan/work/pipeline/config.yaml "$@"
' _ "$@"
