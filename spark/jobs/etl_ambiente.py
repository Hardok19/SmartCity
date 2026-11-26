#!/usr/bin/env python3
"""SmartCity B3.2: primer ETL MongoDB -> Spark -> HDFS (Parquet).

Uso desde contenedor Spark con --packages org.mongodb.spark:mongo-spark-connector_2.12:10.4.1.

Es una primera etapa del ETL: procesa la colección MongoDB `lectura`.
La lectura independiente de los archivos raw OpenAQ/Open-Meteo y el join
con los maestros de PostgreSQL se incorporarán en una siguiente etapa.

No modifica documentos en MongoDB. Solo sobrescribe la ruta HDFS indicada
por --salida. Empezar en /smartcity/sandbox, nunca en rutas productivas.
"""

import argparse
import json
import os

from pyspark.sql import SparkSession, functions as F, types as T

VARIABLES = (
    "pm25", "pm10", "co", "no2", "o3", "co2",
    "temperatura", "humedad", "precipitacion", "viento",
)


def esquema_lectura():
    """Esquema explícito para no depender de inferSchema con documentos heterogéneos."""
    return T.StructType([
        T.StructField("sensor_id", T.StringType()),
        T.StructField("tipo", T.StringType()),
        T.StructField("timestamp", T.TimestampType()),
        T.StructField("ubicacion", T.StructType([
            T.StructField("zona", T.StringType()),
        ])),
        T.StructField("mediciones", T.StructType([
            T.StructField(v, T.DoubleType()) for v in VARIABLES
        ])),
        T.StructField("origen", T.StringType()),
        T.StructField("calidad_dato", T.StructType([
            T.StructField("valido", T.BooleanType()),
        ])),
    ])


def transformar(df):
    """Una fila por variable medida: preserva categoría, origen y unidad lógica."""
    pares = F.array(*[
        F.struct(F.lit(nombre).alias("tipo_metrica"),
                 F.col(f"mediciones.{nombre}").cast("double").alias("valor"))
        for nombre in VARIABLES
    ])
    largo = (df
        .select(
            F.col("ubicacion.zona").alias("zona"),
            F.col("timestamp"),
            F.col("sensor_id").alias("id_sensor"),
            F.col("tipo").alias("categoria"),
            F.col("origen"),
            F.col("calidad_dato.valido").alias("calidad_valida"),
            F.explode(pares).alias("m"),
        )
        .select("zona", "timestamp", "id_sensor", "categoria", "origen",
                "calidad_valida", "m.tipo_metrica", "m.valor")
    )
    valida = (
        F.col("zona").isin("Zona Norte", "Zona Sur", "Zona Centro", "Zona Este", "Zona Oeste")
        & F.col("timestamp").isNotNull()
        & F.col("id_sensor").isNotNull()
        & F.col("valor").isNotNull()
        & (~F.isnan("valor"))
        & (F.col("calidad_valida").isNull() | F.col("calidad_valida"))
        & (
            ((F.col("tipo_metrica") == "temperatura") & F.col("valor").between(-60, 60))
            | ((F.col("tipo_metrica") == "humedad") & F.col("valor").between(0, 100))
            | ((~F.col("tipo_metrica").isin("temperatura", "humedad")) & (F.col("valor") >= 0))
        )
    )
    return (
        largo.filter(valida)
        .dropDuplicates(["id_sensor", "timestamp", "tipo_metrica"])
        .withColumn("fuente", F.lit("Spark"))
        .withColumn("anio", F.year("timestamp"))
        .withColumn("mes", F.month("timestamp"))
        .select("zona", "timestamp", "id_sensor", "tipo_metrica", "valor",
                "fuente", "origen", "categoria", "anio", "mes")
    )


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--limite", type=int, default=0,
                   help="Cantidad máxima de documentos para la prueba; 0=toda la colección")
    p.add_argument("--salida", default="hdfs://namenode:8020/smartcity/sandbox/etl_ambiente_prueba",
                   help="Carpeta de salida HDFS, que será SOBRESCRITA")
    args = p.parse_args()
    if args.limite < 0:
        p.error("--limite no puede ser negativo")
    if not args.salida.startswith("hdfs://namenode:8020/smartcity/"):
        p.error("Para proteger los datos, --salida debe estar dentro de hdfs://namenode:8020/smartcity/")
    uri = os.environ.get("MONGO_URI")
    if not uri:
        p.error("Falta variable MONGO_URI dentro del contenedor Spark")

    spark = (SparkSession.builder
             .appName("SmartCity-B3-ETL-Ambiente")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.shuffle.partitions", "8")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    try:
        lector = (spark.read.format("mongodb")
                  .option("connection.uri", uri)
                  .option("database", "smartcity_db")
                  .option("collection", "lectura")
                  .option("mode", "PERMISSIVE")
                  .schema(esquema_lectura()))
        if args.limite:
            # Un único particionador evita aplicar el límite una vez por partición.
            lector = (lector.option("partitioner",
                "com.mongodb.spark.sql.connector.read.partitioner.SinglePartitionPartitioner")
                .option("aggregation.pipeline", json.dumps([{"$limit": args.limite}])))
        originales = lector.load()
        print("SMARTCITY_ETL_DOCUMENTOS_ENTRADA=" + str(originales.count()), flush=True)

        curado = transformar(originales)
        curado.write.mode("overwrite").partitionBy("anio", "mes").parquet(args.salida)

        # Releer desde HDFS para comprobar que la escritura se realizó.
        comprobacion = spark.read.parquet(args.salida)
        print("SMARTCITY_ETL_FILAS_PARQUET=" + str(comprobacion.count()), flush=True)
        print("SMARTCITY_ETL_SALIDA=" + args.salida, flush=True)
        comprobacion.select("zona", "id_sensor", "tipo_metrica", "valor", "origen").show(5, truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
