#!/usr/bin/env bash
# Punto de entrada único del pipeline MITMA. Con `docker compose up -d` hecho:
#
#   ./run_pipeline.sh                      # TODO (apartados A a F) + informe
#   ./run_pipeline.sh --sin-reto           # todo menos el reto F (pruebas rápidas)
#   ./run_pipeline.sh --etapas C           # un apartado entero (letra A..F)
#   ./run_pipeline.sh --etapas etl,calidad # etapas concretas
#
# Apartados y etapas, en este orden:
#   A  ingesta hdfs etl calidad formatos     (Spark, dentro del contenedor mitma-spark)
#   B  fsck                                  (este script: hdfs fsck -files -blocks -locations)
#      hdfs_estado                           (Spark)
#   C  streaming                             (Hadoop Streaming en YARN, lo lanza este script)
#      mapreduce                             (Spark RDD + comparación con Streaming)
#   D  resultados cache                      (Spark)
#   E  ml                                    (Spark MLlib)
#   F  tolerancia                            (este script: para un DataNode a mitad de un job;
#                                             el job de Spark es la etapa f_job de run_pipeline.py)
#   -  informe                               (genera informe/informe.html)
#
# `fsck`, `streaming` y `tolerancia` usan `docker` (hdfs fsck, hadoop jar, docker stop), por eso se
# orquestan desde aquí, en el Mac; todo lo demás está en pipeline/run_pipeline.py.
set -euo pipefail
cd "$(dirname "$0")"

TODAS="ingesta hdfs etl calidad formatos fsck hdfs_estado streaming mapreduce resultados cache ml tolerancia informe"
PYTHON_STAGES=" ingesta hdfs etl calidad formatos hdfs_estado mapreduce resultados cache ml informe "

# Rutas y parámetros: todo sale de config.yaml (aquí no hay rutas escritas)
cfg() {         # cfg <clave>  -> valor de esa clave en config.yaml
  local v
  v=$(sed -n "s/^[[:space:]]*$1:[[:space:]]*//p" config.yaml | head -1 \
      | sed -e 's/[[:space:]]*#.*$//' -e 's/^"//' -e 's/"$//')
  [ -n "$v" ] || { echo "Falta la clave '$1' en config.yaml" >&2; exit 2; }
  echo "$v"
}
[ -f config.yaml ] || { echo "No encuentro config.yaml junto a run_pipeline.sh" >&2; exit 2; }

RAW="$(cfg raw)"                              # /raw/viajes
CURATED="$(cfg curated)"                      # /curated/viajes
DATANODE_A_PARAR="$(cfg datanode_a_parar)"    # nodo que se para en el reto F
DATANODE_EXTRA="$(cfg datanode_extra)"        # tercer nodo (perfil "reto" de docker-compose.yml)
ITER_RETO="$(cfg iteraciones)"                # iteraciones del job del reto F
ITER_PARAR="$(cfg iteracion_parada)"          # se para el nodo cuando acaba esta iteración
SALIDA_STREAMING="$(cfg results)/$(cfg salida_streaming)"
EVID="evidence"
RF="$EVID/reto_f"

usage() { sed -n '2,23p' "$0" | sed 's/^# \{0,1\}//'; }

ETAPAS=""; SIN_RETO=0
while [ $# -gt 0 ]; do
  case "$1" in
    --etapas)   [ $# -ge 2 ] || { echo "--etapas necesita un valor"; exit 2; }; ETAPAS="$2"; shift 2;;
    --sin-reto) SIN_RETO=1; shift;;
    -h|--help)  usage; exit 0;;
    *) echo "argumento desconocido: $1 (usa --help)"; exit 2;;
  esac
done

# --- qué etapas toca ejecutar
SELECCION=""
if [ -z "$ETAPAS" ]; then
  FULL=1
  for e in $TODAS; do
    if [ "$e" = "tolerancia" ] && [ "$SIN_RETO" = 1 ]; then continue; fi
    SELECCION="$SELECCION $e"
  done
else
  FULL=0
  PEDIDAS=""
  for x in $(echo "$ETAPAS" | tr ',' ' '); do
    case "$(echo "$x" | tr 'a-z' 'A-Z')" in
      A) PEDIDAS="$PEDIDAS ingesta hdfs etl calidad formatos";;
      B) PEDIDAS="$PEDIDAS fsck hdfs_estado";;
      C) PEDIDAS="$PEDIDAS streaming mapreduce";;
      D) PEDIDAS="$PEDIDAS resultados cache";;
      E) PEDIDAS="$PEDIDAS ml";;
      F) PEDIDAS="$PEDIDAS tolerancia";;
      *) PEDIDAS="$PEDIDAS $x";;
    esac
  done
  for e in $PEDIDAS; do
    case " $TODAS " in *" $e "*) ;; *) echo "etapa desconocida: $e (válidas: $TODAS)"; exit 2;; esac
  done
  for e in $TODAS; do
    case " $PEDIDAS " in *" $e "*) SELECCION="$SELECCION $e";; esac
  done
fi
echo ">> etapas:$SELECCION"

mkdir -p "$EVID"
if [ "$FULL" = 1 ]; then rm -f "$EVID/tiempos.csv"; fi

# --- utilidades
registrar() {   # registrar <etapa> <segundos> <estado>
  [ -f "$EVID/tiempos.csv" ] || echo "ts,etapa,segundos,estado" > "$EVID/tiempos.csv"
  echo "$(date '+%Y-%m-%d %H:%M:%S'),$1,$2,$3" >> "$EVID/tiempos.csv"
}

py_exec() {     # py_exec <script.py> [args...]   (dentro del contenedor mitma-spark)
  docker exec mitma-spark bash -c '
    export PYTHONPATH="$SPARK_HOME/python:$(ls "$SPARK_HOME"/python/lib/py4j-*-src.zip 2>/dev/null | head -1)"
    script="$1"; shift
    exec python "/home/jovyan/work/pipeline/$script" "$@"
  ' _ "$@"
}

LOTE=""
vaciar_lote() {
  [ -n "$LOTE" ] || return 0
  local lista; lista=$(echo "$LOTE" | sed 's/^ //; s/ /,/g')
  LOTE=""
  echo ">> [python] etapas: $lista"
  py_exec run_pipeline.py --etapas "$lista"
}

# --- Evidencia de bloques y réplicas (hdfs fsck)
FSCK_RAW="$(cfg fsck_fichero)"
FSCK_CURATED="$CURATED"
etapa_fsck() {
  # Estructura de zonas del data lake (raw / curated / results), como en el paso 7 de clase
  echo ">> [fsck] hdfs dfs -ls -R de las zonas (salida en $EVID/hdfs_ls.txt)"
  docker exec namenode hdfs dfs -ls -R "${RAW%/*}" "${CURATED%/*}" "$(cfg results)" > "$EVID/hdfs_ls.txt" 2>&1 || true
  head -30 "$EVID/hdfs_ls.txt" | sed 's/^/   /'
  echo ">> [fsck] hdfs fsck $FSCK_RAW -files -blocks -locations"
  docker exec namenode hdfs fsck "$FSCK_RAW" -files -blocks -locations 2>&1 | tee "$EVID/fsck_raw.txt"
  echo ">> [fsck] hdfs fsck $FSCK_CURATED (salida completa en $EVID/fsck_curated.txt)"
  docker exec namenode hdfs fsck "$FSCK_CURATED" -files -blocks -locations > "$EVID/fsck_curated.txt" 2>&1 || true
  # Copia de seguridad de la ubicación de bloques de TODOS los ficheros de /raw (los 7 CSV y la
  # tabla auxiliar). /curated ya queda completo en fsck_curated.txt (todos sus Parquet).
  echo ">> [fsck] hdfs fsck ${RAW%/*} -files -blocks -locations (todos los ficheros; salida en $EVID/fsck_raw_todos.txt)"
  docker exec namenode hdfs fsck "${RAW%/*}" -files -blocks -locations > "$EVID/fsck_raw_todos.txt" 2>&1 || true
  grep -E "Status:|Total size|Total files|Total blocks|Average block replication|Under-replicated|Missing|Corrupt" \
    "$EVID/fsck_raw_todos.txt" | sed 's/^ */   /' || true
  grep -E "Status:|Total blocks|Average block replication|Under-replicated|Over-replicated|Number of data-nodes" \
    "$EVID/fsck_curated.txt" | sed 's/^ */   /' || true
}

# --- Hadoop Streaming (MapReduce en YARN)
etapa_streaming() {
  local c="" x
  for x in nodemanager resourcemanager namenode historyserver; do
    if docker exec "$x" test -f /streaming/mapper.py >/dev/null 2>&1; then c="$x"; break; fi
  done
  if [ -z "$c" ]; then
    echo "No encuentro /streaming/mapper.py en ningún contenedor Hadoop."
    echo "Revisa que docker-compose.yml monte ./hadoop_streaming en /streaming."
    return 1
  fi
  echo ">> [streaming] hadoop jar desde el contenedor '$c' (mapper + combiner + reducer en Python)"
  docker exec "$c" bash -c '
    JAR=$(ls /opt/hadoop-*/share/hadoop/tools/lib/hadoop-streaming-*.jar | head -1)
    hdfs dfs -rm -r -f -skipTrash '"$SALIDA_STREAMING"' >/dev/null 2>&1 || true
    exec hadoop jar "$JAR" \
      -D mapreduce.job.name=mitma_streaming_destino \
      -files /streaming/mapper.py,/streaming/reducer.py \
      -mapper "python3 mapper.py" -combiner "python3 reducer.py" -reducer "python3 reducer.py" \
      -input '"$RAW"' -output '"$SALIDA_STREAMING"'
  ' 2>&1 | tee "$EVID/mr_streaming_pipeline.log"

  # Como el paso 14 de clase (hdfs dfs -cat): ver el resultado del job en la terminal
  echo ">> [streaming] resultado: los 10 destinos con más viajes (hdfs dfs -cat $SALIDA_STREAMING/part-*)"
  { docker exec "$c" hdfs dfs -cat "$SALIDA_STREAMING"/part-* \
      | sort -t "$(printf '\t')" -k2 -gr | head -10 | tee "$EVID/mr_streaming_top10.txt"; } || true
}

# --- Reto F
vivos()   { docker exec namenode hdfs dfsadmin -report 2>/dev/null | grep -E '^Live datanodes' | sed -E 's/[^0-9]*([0-9]+).*/\1/' | head -1; }
bajo_rep(){ docker exec namenode hdfs fsck "$CURATED" 2>/dev/null | grep -i 'Under-replicated blocks' | sed -E 's/[^0-9]*([0-9]+).*/\1/' | head -1; }
sobre_rep(){ docker exec namenode hdfs fsck "$CURATED" 2>/dev/null | grep -i 'Over-replicated blocks' | sed -E 's/[^0-9]*([0-9]+).*/\1/' | head -1; }

esperar() {     # esperar <descripción> <timeout_s> <comando que devuelve 0 cuando se cumple>
  local desc="$1" lim="$2" ini; shift 2
  ini=$(date +%s)
  while ! "$@"; do
    if [ $(( $(date +%s) - ini )) -ge "$lim" ]; then echo "   (tiempo agotado esperando: $desc)"; return 1; fi
    sleep 3
  done
}
cond_vivos()  { [ "$(vivos || true)" = "$1" ]; }
cond_sano()   { [ "$(bajo_rep || true)" = "0" ] && [ "$(sobre_rep || true)" = "0" ]; }
cond_bajo0()  { [ "$(bajo_rep || true)" = "0" ]; }

ev() { echo "$(date +%H:%M:%S),$1" >> "$RF/linea_tiempo.csv"; echo "   [reto F] $1"; }

restaurar_reto() {
  docker start "$DATANODE_A_PARAR" >/dev/null 2>&1 || true
  docker stop "$DATANODE_EXTRA" >/dev/null 2>&1 || true
}

etapa_tolerancia() {
  mkdir -p "$RF"
  echo "hora,evento" > "$RF/linea_tiempo.csv"
  if ! docker ps --format '{{.Names}}' | grep -qx "$DATANODE_A_PARAR"; then
    echo "$DATANODE_A_PARAR no está en marcha: haz 'docker compose up -d' antes."; return 1
  fi
  trap restaurar_reto EXIT

  echo ">> [tolerancia] 1/5 levantando $DATANODE_EXTRA (tercer DataNode, siempre vacío)"
  # El tercer nodo empieza SIEMPRE vacío: se borra su contenedor y su volumen, para que no arrastre
  # bloques de ejecuciones anteriores (con restos viejos la re-replicación puede quedarse atascada).
  docker rm -f "$DATANODE_EXTRA" >/dev/null 2>&1 || true
  for v in $(docker volume ls -q | grep "hadoop_${DATANODE_EXTRA}$" || true); do
    docker volume rm "$v" >/dev/null 2>&1 || true
  done
  # --force-recreate: si el contenedor se creó con una red de Docker que ya no existe
  # ("network ... not found"), se vuelve a crear en la red actual.
  docker compose --profile reto up -d --force-recreate "$DATANODE_EXTRA" >/dev/null
  if ! esperar "3 DataNodes vivos" 180 cond_vivos 3; then
    echo "No hay 3 DataNodes vivos: $DATANODE_EXTRA no ha arrancado. Sin él el reto F no tiene sentido"
    echo "(al parar $DATANODE_A_PARAR solo quedaría 1 nodo y HDFS no podría mantener 2 réplicas)."
    echo "Prueba: docker rm -f $DATANODE_EXTRA && docker compose --profile reto up -d $DATANODE_EXTRA"
    return 1
  fi
  ev "estado inicial: $(vivos || echo '?') DataNodes vivos, $(bajo_rep || echo '?') bloques under-replicated"
  { echo "dfs.namenode.heartbeat.recheck-interval (ms): $(docker exec namenode hdfs getconf -confKey dfs.namenode.heartbeat.recheck-interval 2>&1 || true)"
    echo "dfs.heartbeat.interval (s): $(docker exec namenode hdfs getconf -confKey dfs.heartbeat.interval 2>&1 || true)"
  } > "$RF/config_latidos.txt"
  sed 's/^/   [reto F] /' "$RF/config_latidos.txt"
  docker exec namenode hdfs fsck "$CURATED" -files -blocks -locations > "$RF/fsck_inicial.txt" 2>&1 || true
  docker exec namenode hdfs dfsadmin -report > "$RF/report_inicial.txt" 2>&1 || true

  echo ">> [tolerancia] 2/5 lanzando el job de Spark (etapa f_job, $ITER_RETO iteraciones)"
  : > "$RF/reto_f.log"
  py_exec run_pipeline.py --etapas f_job --iter "$ITER_RETO" > "$RF/reto_f.log" 2>&1 &
  local pid=$! rc=0
  ev "job de Spark en marcha"

  local ini; ini=$(date +%s)
  while ! grep -Eq "iteración +$ITER_PARAR:" "$RF/reto_f.log"; do
    if ! kill -0 "$pid" 2>/dev/null; then echo "El job terminó antes de la iteración $ITER_PARAR:"; tail -20 "$RF/reto_f.log"; return 1; fi
    if [ $(( $(date +%s) - ini )) -ge 900 ]; then echo "Tiempo agotado esperando la iteración $ITER_PARAR"; kill "$pid" 2>/dev/null || true; return 1; fi
    sleep 1
  done

  echo ">> [tolerancia] 3/5 parando $DATANODE_A_PARAR a mitad del job"
  docker stop "$DATANODE_A_PARAR" >/dev/null
  local t_stop; t_stop=$(date +%s)
  ev "docker stop $DATANODE_A_PARAR (tras la iteración $ITER_PARAR)"
  esperar "NameNode da el nodo por muerto" 180 cond_vivos 2 || true
  ev "NameNode da el nodo por muerto a los $(( $(date +%s) - t_stop )) s: $(vivos || echo '?') DataNodes vivos, $(bajo_rep || echo '?') bloques under-replicated"

  wait "$pid" || rc=$?
  ev "el job de Spark termina (código $rc; 0 = resultado idéntico)"
  tail -4 "$RF/reto_f.log" | sed 's/^/   /'

  echo ">> [tolerancia] 4/5 esperando a que HDFS vuelva a 2 réplicas"
  esperar "0 bloques under-replicated" 900 cond_bajo0 || true
  ev "re-replicación terminada a los $(( $(date +%s) - t_stop )) s del stop: $(bajo_rep || echo '?') bloques under-replicated"
  docker exec namenode hdfs fsck "$CURATED" -files -blocks -locations > "$RF/fsck_tras_fallo.txt" 2>&1 || true
  docker exec namenode hdfs dfsadmin -report > "$RF/report_tras_fallo.txt" 2>&1 || true

  echo ">> [tolerancia] 5/5 restaurando el clúster"
  docker start "$DATANODE_A_PARAR" >/dev/null
  esperar "3 DataNodes vivos" 180 cond_vivos 3 || true
  ev "$DATANODE_A_PARAR vuelve: $(vivos || echo '?') DataNodes vivos"
  esperar "sin bloques sobre-replicados" 300 cond_sano || true
  docker stop "$DATANODE_EXTRA" >/dev/null
  esperar "2 DataNodes vivos" 180 cond_vivos 2 || true
  # Al parar $DATANODE_EXTRA, los bloques que tenían una réplica allí vuelven a quedar con 1 sola y
  # HDFS tarda unos minutos en copiarlos (igual que en el paso 4): se espera hasta 15 min.
  sleep 5
  esperar "0 bloques under-replicated" 900 cond_bajo0 || true
  ev "estado final: $(vivos || echo '?') DataNodes vivos, $(bajo_rep || echo '?') under-replicated"
  docker exec namenode hdfs fsck "$CURATED" -files -blocks -locations > "$RF/fsck_final.txt" 2>&1 || true
  docker exec namenode hdfs dfsadmin -report > "$RF/report_final.txt" 2>&1 || true
  trap - EXIT
  return "$rc"
}

# --- ejecución
for e in $SELECCION; do
  case "$PYTHON_STAGES" in
    *" $e "*) LOTE="$LOTE $e"; continue;;
  esac
  vaciar_lote
  t0=$(date +%s)
  estado=OK
  case "$e" in
    fsck)       etapa_fsck       || estado=ERROR;;
    streaming)  etapa_streaming  || estado=ERROR;;
    tolerancia) etapa_tolerancia || estado=ERROR;;
  esac
  registrar "$e" $(( $(date +%s) - t0 )) "$estado"
  if [ "$estado" != OK ]; then echo "ERROR en la etapa $e"; exit 1; fi
done
vaciar_lote

if echo "$SELECCION" | grep -q informe; then
  echo ">> Informe: informe/informe.html  (ábrelo con: open informe/informe.html)"
fi
