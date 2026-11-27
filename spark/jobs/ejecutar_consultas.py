#!/usr/bin/env python3
"""Ejecuta los SP-Qxx.sql de SmartCity sobre el ETL Parquet en HDFS.

Comando en contenedor: spark-submit ... ejecutar_consultas.py --consultas SP-Q02 SP-Q05
Para SP-Q02 y SP-Q07 requiere MONGO_URI (no poner contraseñas en el archivo).

Este ejecutor lee datos. Solo escribe en HDFS si se pasa --exportar-reporte
(y únicamente en una ruta de sandbox predefinida).
"""
import argparse
import os
from pathlib import Path

from pyspark.sql import SparkSession, functions as F, types as T

DIRECTORIO_SQL = Path("/opt/smartcity/spark/sql")
HDFS_ENTRADA = "hdfs://namenode:8020/smartcity/sandbox/etl_ambiente_completa"
HDFS_REPORTE = "hdfs://namenode:8020/smartcity/sandbox/reporte_spark_q10"



def cargar_eventos(spark, ambiente):
    uri = os.getenv("MONGO_URI")
    if not uri:
        raise RuntimeError(
            "SP-Q02 necesita la variable MONGO_URI en el contenedor"
        )
    # Esquema explicito de los eventos almacenados en MongoDB
    esquema = T.StructType([
        T.StructField("tipo_evento", T.StringType()),
        T.StructField("severidad", T.StringType()),
        T.StructField("inicio", T.TimestampType()),
        T.StructField(
            "zona",
            T.StructType([
                T.StructField("nombre", T.StringType()),
                T.StructField(
                    "geo",
                    T.StructType([
                        T.StructField(
                            "coordinates",
                            T.ArrayType(T.DoubleType())
                        )
                    ])
                )
            ])
        )
    ])
    # Leer los eventos de MongoDB
    eventos = (
        spark.read
        .format("mongodb")
        .option("connection.uri", uri)
        .option("database", "smartcity_db")
        .option("collection", "evento")
        .option(
            "partitioner",
            "com.mongodb.spark.sql.connector.read.partitioner.SinglePartitionPartitioner"
        )
        .schema(esquema)
        .load()
        .select(
            F.col("zona.nombre").alias("zona"),
            F.col("tipo_evento"),
            F.col("severidad"),
            F.col("inicio"),
            F.col("zona.geo.coordinates").getItem(1).alias("latitud"),
            F.col("zona.geo.coordinates").getItem(0).alias("longitud")
        )
    )
    # Crear vista temporal de eventos para Spark SQL
    eventos.createOrReplaceTempView("eventos")
    # Crear vista temporal de zonas para realizar el JOIN
    ambiente.select("zona").distinct().createOrReplaceTempView("zonas")


def cargar_lecturas_crudas(spark):
    """Vista sin limpieza para SP-Q07: conserva flags de anomalías de MongoDB."""
    uri = os.getenv("MONGO_URI")
    if not uri:
        raise RuntimeError("SP-Q07 necesita MONGO_URI en el contenedor Spark")
    esquema = T.StructType([
        T.StructField("sensor_id", T.StringType()),
        T.StructField("tipo", T.StringType()),
        T.StructField("timestamp", T.TimestampType()),
        T.StructField("origen", T.StringType()),
        T.StructField("ubicacion", T.StructType([
            T.StructField("zona", T.StringType()),
        ])),
        T.StructField("mediciones", T.StructType([
            T.StructField(nombre, T.DoubleType())
            for nombre in ("pm25", "pm10", "no2", "co", "o3", "temperatura", "humedad")
        ])),
        T.StructField("calidad_dato", T.StructType([
            T.StructField("valido", T.BooleanType()),
            T.StructField("flags", T.ArrayType(T.StringType())),
        ])),
    ])
    (spark.read.format("mongodb")
        .option("connection.uri", uri)
        .option("database", "smartcity_db")
        .option("collection", "lectura")
        .option(
                "partitioner",
                "com.mongodb.spark.sql.connector.read.partitioner.SinglePartitionPartitioner"
                )
        .schema(esquema)
        .load()
        .createOrReplaceTempView("lecturas_crudas"))


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--consultas", nargs="+", help="IDs como SP-Q02 SP-Q05; por defecto, todos los SQL disponibles")
    p.add_argument("--exportar-reporte", action="store_true", help="Guarda el resultado de SP-Q10 en HDFS sandbox")
    args = p.parse_args()

    disponibles = {archivo.stem: archivo for archivo in DIRECTORIO_SQL.glob("SP-Q*.sql")}
    ids = args.consultas or sorted(disponibles)
    faltantes = [ident for ident in ids if ident not in disponibles]
    if faltantes:
        p.error("No existen archivos para: " + ", ".join(faltantes))
    if args.exportar_reporte and "SP-Q10" not in ids:
        p.error("--exportar-reporte requiere incluir SP-Q10")

    spark = (SparkSession.builder
        .appName("SmartCity-B4-Spark-SQL")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    fallidas = []
    try:
        ambiente = spark.read.parquet(HDFS_ENTRADA)
        ambiente.createOrReplaceTempView("ambiente")
        if "SP-Q08" in ids:
            catalogo = (
                spark.read
                .option("header", True)
                .csv("/opt/smartcity/data/maestros/sensores.csv")
                .select("id_sensor", "categoria", "zona")
                .dropDuplicates(["id_sensor"])
            )
            catalogo.createOrReplaceTempView("catalogo_sensores")
        if "SP-Q02" in ids:
            cargar_eventos(spark, ambiente)
        if "SP-Q07" in ids:
            cargar_lecturas_crudas(spark)

        for ident in ids:
            print(f"\n=== {ident} ===", flush=True)
            consulta = disponibles[ident].read_text(encoding="utf-8")
            try:
                resultado = spark.sql(consulta)
                resultado.show(20, truncate=False)
                if ident == "SP-Q07":
                    print("=== RESUMEN POR METODO DE DETECCION ===")
                    resultado.groupBy("metodo_deteccion").count().show()
                if ident == "SP-Q10" and args.exportar_reporte:
                    resultado.write.mode("overwrite").parquet(HDFS_REPORTE)
                    print(f"SMARTCITY_SPARK_REPORTE={HDFS_REPORTE}", flush=True)
                print(f"SMARTCITY_SPARK_OK={ident}", flush=True)
            except Exception as e:
                fallidas.append(ident)
                print(f"SMARTCITY_SPARK_ERROR={ident}: {type(e).__name__}: {str(e)[:700]}", flush=True)

        print(f"\nSMARTCITY_SPARK_RESUMEN={len(ids)-len(fallidas)}/{len(ids)}", flush=True)
        if fallidas:
            raise SystemExit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
