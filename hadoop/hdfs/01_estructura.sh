#!/usr/bin/env bash
# 01_estructura.sh — A3.1. Crea la estructura de HDFS del proyecto (idempotente).
# Uso (desde la raíz del repo, Git Bash o Linux):  bash hadoop/hdfs/01_estructura.sh
#
# /smartcity
# ├── raw/            datos tal como llegan (inmutables): openaq/, openmeteo/, trafico/, energia/,
# │                   mongo_exports/<coleccion>/fecha=AAAA-MM-DD/   (export de MongoDB, JSON Lines)
# ├── curated/        datos limpios en Parquet particionados anio=/mes= (salida de los ETL Spark)
# ├── analytics/      resultados de análisis (Spark B4.2) listos para el dashboard
# ├── mapreduce/      salidas de los jobs MapReduce mr01…mr05
# ├── streaming/      reto adicional: entrada/ (llegada continua), alertas/, checkpoints/
# └── sandbox/        pruebas y operaciones de la UI (único lugar donde la UI puede escribir o borrar)
set -euo pipefail
export MSYS_NO_PATHCONV=1
NN="${CONTENEDOR_NAMENODE:-smartcity_namenode}"

DIRS=(
  /smartcity/raw/openaq /smartcity/raw/openmeteo /smartcity/raw/trafico /smartcity/raw/energia
  /smartcity/raw/mongo_exports
  /smartcity/curated /smartcity/analytics /smartcity/mapreduce
  /smartcity/streaming/entrada /smartcity/streaming/alertas /smartcity/streaming/checkpoints
  /smartcity/sandbox
)
docker exec "$NN" hdfs dfs -mkdir -p "${DIRS[@]}"
docker exec "$NN" hdfs dfs -ls -R /smartcity | grep '^d' | awk '{print $NF}'
