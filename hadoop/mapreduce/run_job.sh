#!/usr/bin/env bash
# run_job.sh <job> <entrada_hdfs> <salida_hdfs> — A4.1. Se ejecuta DENTRO del NameNode (cliente de YARN).
# Lanza un job de Hadoop Streaming con el mapper/reducer Python de hadoop/mapreduce/<job>/ y borra la salida
# previa. Variables opcionales: DESDE, HASTA (mr05), TOP_N (mr02).
# Desde el anfitrión usar hadoop/mapreduce/ejecutar.sh (copia los archivos y mide el tiempo).
set -euo pipefail
JOB="$1"; ENTRADA="$2"; SALIDA="$3"
STREAMING=/opt/hadoop-3.2.1/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar
cd "$(dirname "$0")/$JOB"

REDUCTORES=2
COMBINER=(-combiner "python3 reducer.py --combinar")
case "$JOB" in
  mr01) COMBINER=(-combiner "python3 reducer.py --combinar") ;;   # combiner = reducer (suma)
  mr02) REDUCTORES=1; COMBINER=() ;;                              # top N global: un solo reducer
esac

hdfs dfs -rm -r -f -skipTrash "$SALIDA" > /dev/null 2>&1 || true
INICIO=$(date +%s%N)
hadoop jar "$STREAMING" \
  -D mapreduce.job.name="smartcity-$JOB" \
  -D mapreduce.job.reduces="$REDUCTORES" \
  -files mapper.py,reducer.py \
  -mapper "python3 mapper.py" \
  -reducer "python3 reducer.py" \
  "${COMBINER[@]}" \
  -cmdenv "PYTHONIOENCODING=utf-8" -cmdenv "DESDE=${DESDE:-}" -cmdenv "HASTA=${HASTA:-}" -cmdenv "TOP_N=${TOP_N:-10}" \
  -input "$ENTRADA" -output "$SALIDA"
FIN=$(date +%s%N)
echo "SMARTCITY_MR_TIEMPO job=$JOB segundos=$(( (FIN - INICIO) / 1000000 ))e-3 entrada=$ENTRADA salida=$SALIDA"
echo "SMARTCITY_MR_FILAS job=$JOB filas=$(hdfs dfs -cat "$SALIDA/part-*" | wc -l)"
hdfs dfs -cat "$SALIDA/part-*" | head -n 10
