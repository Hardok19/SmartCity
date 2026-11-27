#!/usr/bin/env bash
# 03_demo.sh — A3.3. Demostración de comandos HDFS, bloques y réplicas. Guarda la transcripción en
# docs/resultados/hdfs_demo.txt.   Uso: bash hadoop/hdfs/03_demo.sh [archivo_local] [tamano_bloque_bytes]
# Por defecto usa el export de lecturas (≈380 MB, bloques de 32 MB) si existe; si no, un CSV de tráfico
# (≈2,8 MB) con bloques de 1 MB, para que el archivo se reparta en varios bloques igualmente.
set -uo pipefail
export MSYS_NO_PATHCONV=1
NN="${CONTENEDOR_NAMENODE:-smartcity_namenode}"
if [ -n "${1:-}" ]; then ARCHIVO="$1"; BLOQUE="${2:-33554432}"
elif [ -f data/interim/mongo/lectura.jsonl ]; then ARCHIVO=data/interim/mongo/lectura.jsonl; BLOQUE=33554432
else ARCHIVO=data/raw/trafico/trafico_2024.csv; BLOQUE=1048576; fi
NOMBRE=$(basename "$ARCHIVO")
DEMO=/smartcity/sandbox/demo
SALIDA=docs/resultados/hdfs_demo.txt
mkdir -p docs/resultados

paso() {   # imprime y ejecuta un comando dentro del NameNode
  echo
  echo "\$ $*"
  docker exec -i "$NN" bash -c "$*" 2>&1 | grep -vE " (INFO|WARN) "
}

{
  echo "# Demostración HDFS — $(date -u '+%Y-%m-%d %H:%M UTC')"
  echo "# Archivo: $ARCHIVO ($(du -h "$ARCHIVO" | cut -f1)), bloque por defecto 128 MB, bloque de prueba $((BLOQUE / 1048576)) MB"
  paso "hdfs dfsadmin -report | grep -E 'Live datanodes|^Name:|DFS Used%'"
  paso "hdfs dfs -rm -r -f -skipTrash $DEMO > /dev/null; hdfs dfs -mkdir -p $DEMO/bloque_defecto $DEMO/bloque_pequeno"
  docker cp "$ARCHIVO" "$NN:/tmp/$NOMBRE"
  paso "hdfs dfs -put /tmp/$NOMBRE $DEMO/bloque_defecto/"
  paso "hdfs dfs -D dfs.blocksize=$BLOQUE -put /tmp/$NOMBRE $DEMO/bloque_pequeno/"
  paso "hdfs dfs -ls -R $DEMO"
  paso "hdfs dfs -cat $DEMO/bloque_defecto/$NOMBRE | head -n 5"
  paso "hdfs dfs -stat 'tamaño=%b bytes, bloque=%o bytes, réplicas=%r, modificado=%y' $DEMO/bloque_pequeno/$NOMBRE"
  paso "hdfs dfs -du -h $DEMO"
  echo
  echo "# Cada bloque y los DataNodes donde está cada una de sus réplicas (dfs.replication = 2):"
  paso "hdfs fsck $DEMO/bloque_defecto/$NOMBRE -files -blocks -locations | grep -E 'blk_|Total blocks|replication|Status'"
  paso "hdfs fsck $DEMO/bloque_pequeno/$NOMBRE -files -blocks -locations | grep -E 'blk_|Total blocks|replication|Status'"
  echo
  echo "# Cambiar el factor de replicación de un archivo (3 réplicas = una en cada DataNode):"
  paso "hdfs dfs -setrep -w 3 $DEMO/bloque_pequeno/$NOMBRE"
  paso "hdfs fsck $DEMO/bloque_pequeno/$NOMBRE -files -blocks -locations | grep -E 'Total blocks|Average block replication'"
  paso "hdfs dfs -setrep -w 2 $DEMO/bloque_pequeno/$NOMBRE"
  paso "rm -f /tmp/copia_$NOMBRE; hdfs dfs -get $DEMO/bloque_pequeno/$NOMBRE /tmp/copia_$NOMBRE && md5sum /tmp/$NOMBRE /tmp/copia_$NOMBRE"
  paso "hdfs dfs -rm -skipTrash $DEMO/bloque_defecto/$NOMBRE"
  paso "hdfs dfs -ls -R $DEMO"
  docker exec "$NN" rm -f "/tmp/$NOMBRE" "/tmp/copia_$NOMBRE"
} | tee "$SALIDA"
echo
echo "Transcripción guardada en $SALIDA"
