#!/usr/bin/env bash
# ejecutar.sh <job> [entrada_hdfs] — A4.1, desde el ANFITRIÓN (Git Bash/Linux), en la raíz del repo.
# Copia run_job.sh y el job al NameNode, lo lanza en YARN (http://localhost:8088) y anota el tiempo en
# docs/resultados/mapreduce_tiempos.csv.   Ejemplos:
#   bash hadoop/mapreduce/ejecutar.sh mr01
#   DESDE=2024-03-01 HASTA=2024-03-31 bash hadoop/mapreduce/ejecutar.sh mr05
#   bash hadoop/mapreduce/ejecutar.sh mr01 /smartcity/sandbox/mr_muestra/lectura.jsonl   # entrada alternativa
set -euo pipefail
export MSYS_NO_PATHCONV=1
NN="${CONTENEDOR_NAMENODE:-smartcity_namenode}"
JOB="$1"
LECTURAS=/smartcity/raw/mongo_exports/lectura/*/lectura.jsonl
case "$JOB" in
  mr01|mr03|mr05) ENTRADA_DEF="$LECTURAS" ;;
  mr02) ENTRADA_DEF=/smartcity/mapreduce/mr01 ;;
  mr04) ENTRADA_DEF=/smartcity/raw/mongo_exports/evento/*/evento.jsonl ;;
  *) echo "Job desconocido: $JOB" >&2; exit 2 ;;
esac
ENTRADA="${2:-$ENTRADA_DEF}"
SALIDA="/smartcity/mapreduce/$JOB"
if [ -n "${2:-}" ]; then SALIDA="/smartcity/sandbox/mapreduce/$JOB"; fi   # entradas de prueba no pisan resultados oficiales

docker exec "$NN" mkdir -p /tmp/smartcity_mr
docker cp hadoop/mapreduce/run_job.sh "$NN:/tmp/smartcity_mr/"
docker cp "hadoop/mapreduce/$JOB" "$NN:/tmp/smartcity_mr/"
SALIDA_JOB=$(docker exec -e DESDE="${DESDE:-}" -e HASTA="${HASTA:-}" -e TOP_N="${TOP_N:-10}" "$NN" \
  bash /tmp/smartcity_mr/run_job.sh "$JOB" "$ENTRADA" "$SALIDA" 2>&1 | grep -vE " (INFO|WARN) " || true)
echo "$SALIDA_JOB"
LINEA=$(echo "$SALIDA_JOB" | grep SMARTCITY_MR_TIEMPO || true)
if [ -z "$LINEA" ]; then echo "El job $JOB falló" >&2; exit 1; fi
SEG=$(echo "$LINEA" | sed -E 's/.*segundos=([0-9]+)e-3.*/\1/')
FILAS=$(echo "$SALIDA_JOB" | grep SMARTCITY_MR_FILAS | sed -E 's/.*filas=([0-9]+).*/\1/')
CSV=docs/resultados/mapreduce_tiempos.csv
mkdir -p docs/resultados
[ -f "$CSV" ] || echo "fecha_utc,job,entrada,salida,segundos,filas_salida" > "$CSV"
echo "$(date -u +%FT%TZ),$JOB,$ENTRADA,$SALIDA,$(awk "BEGIN{print $SEG/1000}"),$FILAS" >> "$CSV"
