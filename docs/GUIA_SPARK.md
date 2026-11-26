# SmartCity — Cómo reproducir la parte Spark (pareja B)

## Qué se guarda en Git

Se versionan `spark/jobs/`, `spark/sql/`, scripts de generación y carga,
`data/maestros/` y configuración Docker. No se versionan `data/interim/`,
`data/raw/`, archivos `.jsonl`, Parquet de HDFS ni volúmenes Docker.

Por tanto, al clonar este repositorio **no aparecen automáticamente** las
1 068 777 lecturas de MongoDB ni los 4 389 705 registros del ETL.
Hay que preparar o restaurar los datasets antes de ejecutar los análisis.

## Requisitos

Docker Desktop con Docker Compose, Python para los scripts locales,
paquetes Python de dichos scripts (por ejemplo `pymongo`, `numpy`, `PyYAML`,
`requests`), acceso a las fuentes externas si se quiere regenerar la
versión basada en OpenAQ/Open-Meteo, y memoria suficiente para Hadoop/Spark.

## Contenedores Spark reproducibles

Los siguientes comandos se ejecutan desde la raíz del repositorio:

```bash
# Validar la configuración combinada

docker compose -f docker/docker-compose.yml -f docker/docker-compose.spark-ml.yml config -q

# Compilar la imagen que incorpora NumPy y SciPy

docker compose -f docker/docker-compose.yml -f docker/docker-compose.spark-ml.yml build spark-master spark-worker

# Si los servicios ya existen, recrear solamente Spark; no tocar bases de datos

docker compose -f docker/docker-compose.yml -f docker/docker-compose.spark-ml.yml up -d --no-deps --force-recreate spark-master spark-worker
```

La compilación se basa en `bitnamilegacy/spark:3.5` y verifica NumPy 1.26.4
y SciPy 1.13.1. Desde Git Bash en Windows anteponer `MSYS_NO_PATHCONV=1`
a comandos `docker exec` que utilicen rutas absolutas Linux.

```bash
for c in smartcity_spark_master smartcity_spark_worker; do
  MSYS_NO_PATHCONV=1 docker exec "$c" /opt/bitnami/python/bin/python -c \
    "import numpy, scipy; print(numpy.__version__, scipy.__version__)"
done
```

## Datos necesarios (no incluidos en Git)

1. Iniciar Docker Compose y crear las colecciones e índices MongoDB con
   `python scripts/mongo_admin.py todo`.
2. Generar `data/maestros/sensores.csv` con
   `python scripts/generar_maestros.py` si no se versiona ya el archivo.
3. Descargar o restaurar datos históricos OpenAQ y Open-Meteo para 2023-2024;
   los datos externos pueden cambiar o dejar de estar accesibles. Revisar
   la configuración de estaciones, claves y fechas de cada script.
4. Generar las colecciones MongoDB:
   `python scripts/generar_sinteticos.py --salida data/interim/mongo/final`.
5. Cargar MongoDB con `python scripts/mongo_cargar.py --dir data/interim/mongo/final --reiniciar --lote 10000`.
   **Advertencia:** `--reiniciar` vacía las colecciones afectadas; ejecutar
   solo al preparar una base de pruebas nueva, nunca sobre datos que deban
   conservarse.
6. Crear en HDFS `/smartcity/raw`, `/smartcity/sandbox` si no existen.

Los totales exactos no tienen por qué coincidir si las APIs devuelven
nuevos datos o cambian las semillas, los sensores o el período. Para replicar
exactamente una ejecución histórica se necesita un snapshot de las fuentes.

## Ejecutar el ETL (desde raíz, Git Bash)

Definir `MONGO_URI` mediante un mecanismo seguro (no registrar contraseñas en
Git). En el contenedor la URL debe apuntar al servicio `mongo:27017`, no al
MongoDB de Windows. Por ejemplo:

```bash
read -rsp "Contraseña MongoDB Docker: " MONGO_PASS; echo
export MONGO_URI="mongodb://smartcity:${MONGO_PASS}@mongo:27017/smartcity_db?authSource=admin"
unset MONGO_PASS

MSYS_NO_PATHCONV=1 docker exec --user 0:0 -e HOME=/tmp -e HADOOP_USER_NAME=root -e MONGO_URI \
  smartcity_spark_master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 --total-executor-cores 2 \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.mongodb.spark:mongo-spark-connector_2.12:10.4.1 \
  /opt/smartcity/spark/jobs/etl_ambiente.py \
  --salida hdfs://namenode:8020/smartcity/sandbox/etl_ambiente_completa
```

Es necesario que `docker/docker-compose.yml` monte el repositorio en
`/opt/smartcity` para Spark master y worker, por ejemplo mediante
`../:/opt/smartcity:ro` en ambos servicios (ya añadido en esta rama).

## Análisis y consultas

```bash
MSYS_NO_PATHCONV=1 docker exec --user 0:0 -e HOME=/tmp -e HADOOP_USER_NAME=root -e MONGO_URI \
  smartcity_spark_master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 --total-executor-cores 2 \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.mongodb.spark:mongo-spark-connector_2.12:10.4.1 \
  /opt/smartcity/spark/jobs/analitica.py --analisis \
  descriptivo temporal correlacional predictivo espacial movilidad anomalias comparativo
```

`ejecutar_consultas.py` ejecuta los archivos `spark/sql/SP-Q01.sql`…`SP-Q10.sql`.
Las consultas SP-Q02 y SP-Q07 requieren `MONGO_URI` y el conector MongoDB;
SP-Q08 requiere el catálogo `data/maestros/sensores.csv`. Los resultados se
almacenan principalmente en `/smartcity/sandbox` de HDFS.

## Limitaciones conocidas

- Las cifras de OpenAQ se distinguen de los datos sintéticos.
- PM2.5 histórico externo se obtuvo solo para Centro y Este en el período usado.
- El análisis de «movilidad» actual es un proxy ambiental de horarios pico, no
  datos de conteo vehicular ni congestión real.
- Los resultados del análisis predictivo deben interpretarse con sus valores
  RMSE, R² y baseline; no constituyen un pronóstico validado de tráfico/aire.
- Integración de resultados con PostgreSQL y dashboard aún pendiente.
