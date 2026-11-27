#!/usr/bin/env bash
# 02_cargar_raw.sh — A3.2. Carga los datos crudos en /smartcity/raw (idempotente; verifica bytes).
# Uso (raíz del repo, Git Bash/Linux):  bash hadoop/hdfs/02_cargar_raw.sh [AAAA-MM-DD]
#
# * openaq/<estacion>/<anio>.csv, openmeteo/zona_<zona>_<anio>.json, trafico/, energia/: tal como se descargan
#   o generan. Ya vienen consolidados por estación/zona y AÑO (no por día): pocos archivos grandes en vez de
#   miles de archivos pequeños, porque el NameNode guarda en memoria la metadata de cada archivo y bloque
#   (~150 bytes por objeto) y MapReduce/Spark lanzan una tarea por archivo pequeño.
# * mongo_exports/<coleccion>/fecha=<AAAA-MM-DD>/: export de MongoDB en JSON Lines (B2.6), con validación
#   de conteo Mongo = líneas en HDFS.
# * Los CSV/JSON se suben SIN gzip: gzip no es divisible, así que un archivo .gz grande lo procesaría
#   una sola tarea. Parquet (snappy) sí es divisible y se usa en curated/.
set -euo pipefail
export MSYS_NO_PATHCONV=1
FECHA="${1:-$(date -u +%F)}"
NN="${CONTENEDOR_NAMENODE:-smartcity_namenode}"
PY="${PYTHON:-python}"

bash "$(dirname "$0")/01_estructura.sh" > /dev/null
"$PY" scripts/subir_crudos_hdfs.py --fuentes openaq openmeteo trafico energia
MONGO_URI="${MONGO_URI:-mongodb://smartcity:smartcity123@localhost:27017/smartcity_db?authSource=admin}" \
  "$PY" scripts/subir_crudos_hdfs.py --fuentes mongo --colecciones lectura evento alerta incidente --fecha "$FECHA"

echo "== Aceptación: tamaño en HDFS (lógico / con réplicas) vs. local"
docker exec "$NN" hdfs dfs -du -h /smartcity/raw
for d in openaq openmeteo trafico energia; do
  [ -d "data/raw/$d" ] && printf "local data/raw/%-10s %s\n" "$d" "$(du -sh "data/raw/$d" | cut -f1)"
done
