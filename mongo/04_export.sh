#!/usr/bin/env bash
# 04_export.sh — B2.6, alternativa con mongoexport a `python scripts/subir_crudos_hdfs.py --fuentes mongo`.
# Se ejecuta desde el HOST en la raíz del repo (Git Bash/Linux):  bash mongo/04_export.sh [coleccion ...]
# Sin argumentos exporta evento, alerta, incidente y lectura.
# Destino (mismo que el script Python): /smartcity/raw/mongo_exports/<coleccion>/fecha=<AAAA-MM-DD>/<coleccion>.jsonl
# Valida que el número de documentos en Mongo sea igual al número de líneas en HDFS.
set -euo pipefail
export MSYS_NO_PATHCONV=1   # Git Bash: no convertir rutas /tmp/... a rutas de Windows

USUARIO="${MONGO_USER:-smartcity}"; CLAVE="${MONGO_PASSWORD:-smartcity123}"; BD="${MONGO_DB:-smartcity_db}"
MONGO="${CONTENEDOR_MONGO:-smartcity_mongo}"; NAMENODE="${CONTENEDOR_NAMENODE:-smartcity_namenode}"
FECHA="${FECHA:-$(date -u +%F)}"
DESTINO=/smartcity/raw/mongo_exports
TMP_HOST="$(mktemp -d)"
trap 'rm -rf "$TMP_HOST"' EXIT
if [ "$#" -gt 0 ]; then COLECCIONES=("$@"); else COLECCIONES=(evento alerta incidente lectura); fi

for c in "${COLECCIONES[@]}"; do
  echo "== $c"
  n_mongo=$(docker exec "$MONGO" mongosh --quiet -u "$USUARIO" -p "$CLAVE" --authenticationDatabase admin \
    "$BD" --eval "db.$c.countDocuments({})")
  docker exec "$MONGO" mongoexport --quiet -u "$USUARIO" -p "$CLAVE" --authenticationDatabase admin \
    -d "$BD" -c "$c" --type=json -o "/tmp/$c.jsonl"
  docker cp "$MONGO:/tmp/$c.jsonl" "$TMP_HOST/$c.jsonl"
  docker exec "$MONGO" rm -f "/tmp/$c.jsonl"
  docker cp "$TMP_HOST/$c.jsonl" "$NAMENODE:/tmp/$c.jsonl"
  docker exec "$NAMENODE" bash -c "hdfs dfs -mkdir -p $DESTINO/$c/fecha=$FECHA && \
    hdfs dfs -put -f /tmp/$c.jsonl $DESTINO/$c/fecha=$FECHA/$c.jsonl && rm -f /tmp/$c.jsonl"
  n_hdfs=$(docker exec "$NAMENODE" bash -c "hdfs dfs -cat $DESTINO/$c/fecha=$FECHA/$c.jsonl | wc -l")
  if [ "$n_mongo" -ne "$n_hdfs" ]; then
    echo "ERROR $c: Mongo=$n_mongo HDFS=$n_hdfs" >&2; exit 1
  fi
  echo "OK $c: Mongo=$n_mongo HDFS=$n_hdfs -> $DESTINO/$c/fecha=$FECHA/"
done
