#!/usr/bin/env bash
# 04_export.sh — alternativa demostrativa a `smartcity spark export-mongo` (F6.8): mongoexport + hdfs dfs -put.
# Se ejecuta desde el HOST en la raíz del repo (Git Bash/Linux):  bash mongo/04_export.sh [coleccion ...]
# Sin argumentos exporta evento, alerta, incidente y lectura (la que necesita el ETL de Spark, B3.2).
# Requiere que mongo y namenode tengan el repo montado en /opt/smartcity (volumen "../:/opt/smartcity").
# COMPOSE_FILE: ruta al compose si no se ejecuta desde docker/ (por defecto docker/docker-compose.yml).
# Deja un único JSON Lines por colección en /smartcity/raw/mongo_exports_mongoexport/<coleccion>/ (sin particionar:
# el job de Spark es la vía principal porque lee en paralelo y particiona por fecha).
set -euo pipefail
export MSYS_NO_PATHCONV=1   # Git Bash: no convertir rutas /opt/... a rutas de Windows

USUARIO="${MONGO_USER:-smartcity}"; CLAVE="${MONGO_PASSWORD:-smartcity_dev}"; BD="${MONGO_DB:-smartcity}"
LOCAL=/opt/smartcity/data/interim/mongoexport          # carpeta compartida por los contenedores (repo montado)
DESTINO=/smartcity/raw/mongo_exports_mongoexport
COMPOSE="docker compose -f ${COMPOSE_FILE:-docker/docker-compose.yml}"
COLECCIONES=("${@:-evento alerta incidente lectura}")

for c in ${COLECCIONES[@]}; do
  echo "== $c"
  $COMPOSE exec -T mongo bash -c "mkdir -p $LOCAL && mongoexport --quiet -u $USUARIO -p $CLAVE \
    --authenticationDatabase $BD -d $BD -c $c --type=json -o $LOCAL/$c.jsonl && wc -l < $LOCAL/$c.jsonl"
  $COMPOSE exec -T namenode bash -c "hdfs dfs -mkdir -p $DESTINO/$c && \
    hdfs dfs -put -f $LOCAL/$c.jsonl $DESTINO/$c/ && hdfs dfs -ls $DESTINO/$c"
done
