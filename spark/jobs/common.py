#!/usr/bin/env python3
"""B3.1: utilidades comunes de los jobs Spark de SmartCity (se ejecutan dentro de smartcity_spark_master).

* ``get_spark(app)``: SparkSession con master del clúster, HDFS por defecto y zona horaria UTC.
* ``RUTAS``: única fuente de verdad de las rutas HDFS que usan los jobs y el dashboard.
* ``leer_mongo(...)``: lectura de una colección con esquema explícito (conector MongoDB 10.4.x).

Las rutas se definen en rutas.py (compartido con el dashboard); aquí se les antepone hdfs://namenode:8020.
"""
import json
import os

from pyspark.sql import SparkSession

from rutas import RUTAS as _RUTAS

MASTER = os.environ.get("SPARK_MASTER_URL", "spark://spark-master:7077")
HDFS = "hdfs://namenode:8020"
BASE = HDFS + "/smartcity"
CATALOGO_SENSORES = "/opt/smartcity/data/maestros/sensores.csv"
DIRECTORIO_SQL = "/opt/smartcity/spark/sql"
MONGO_DB = os.environ.get("MONGO_DB", "smartcity_db")
PAQUETE_MONGO = "org.mongodb.spark:mongo-spark-connector_2.12:10.4.1"

RUTAS = {clave: HDFS + ruta for clave, ruta in _RUTAS.items()}

PARTICIONADOR_UNICO = "com.mongodb.spark.sql.connector.read.partitioner.SinglePartitionPartitioner"


def get_spark(app, shuffle_partitions=8, **conf):
    """SparkSession estándar del proyecto. ``conf`` agrega/reemplaza configuraciones."""
    builder = (SparkSession.builder
               .appName(app)
               .config("spark.sql.session.timeZone", "UTC")
               .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
               .config("spark.hadoop.fs.defaultFS", HDFS)
               .config("spark.hadoop.dfs.replication", "2"))   # igual que el clúster (3 DataNodes, D-05)
    # El master lo fija spark-submit (--master spark://spark-master:7077, ver scripts/spark_submit.ps1).
    for clave, valor in conf.items():
        builder = builder.config(clave.replace("__", "."), str(valor))
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def existe(spark, ruta):
    """True si la ruta existe en HDFS (sin lanzar un job)."""
    jvm = spark.sparkContext._jvm
    ruta_h = jvm.org.apache.hadoop.fs.Path(ruta)
    return ruta_h.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration()).exists(ruta_h)


def mongo_uri():
    uri = os.environ.get("MONGO_URI")
    if not uri:
        raise RuntimeError("Falta MONGO_URI en el contenedor Spark (host 'mongo', authSource=admin)")
    return uri


def leer_mongo(spark, coleccion, esquema, pipeline=None, particion_unica=True):
    """Lee una colección de MongoDB con esquema explícito; ``pipeline`` se ejecuta en Mongo."""
    lector = (spark.read.format("mongodb")
              .option("connection.uri", mongo_uri())
              .option("database", MONGO_DB)
              .option("collection", coleccion)
              .schema(esquema))
    if particion_unica:
        lector = lector.option("partitioner", PARTICIONADOR_UNICO)
    if pipeline:
        lector = lector.option("aggregation.pipeline", json.dumps(pipeline))
    return lector.load()


if __name__ == "__main__":
    # Prueba de humo de B3.1: el job aparece en la UI :8080 ejecutado por los workers.
    s = get_spark("SmartCity-B3.1-Prueba")
    print("SMARTCITY_B31_RANGE_COUNT=" + str(s.range(1_000_000).count()), flush=True)
    print("SMARTCITY_B31_PARALELISMO=" + str(s.sparkContext.defaultParallelism), flush=True)
    s.stop()
