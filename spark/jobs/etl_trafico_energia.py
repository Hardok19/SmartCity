#!/usr/bin/env python3
"""B3.3: ETL de tráfico y energía SIMULADOS (D-03) -> Parquet largo particionado por anio/mes.

Entradas (HDFS, subidas con scripts/subir_crudos_hdfs.py --fuentes trafico energia):
    /smartcity/raw/trafico/trafico_<anio>.csv   (scripts/generar_trafico_sintetico.py)
    /smartcity/raw/energia/energia_<anio>.csv   (scripts/generar_energia_sintetica.py)
Salida: RUTAS["trafico_energia"] con las columnas del contrato
    zona, timestamp, id_sensor, tipo_metrica (trafico|energia), valor, fuente="Spark"
  + origen, velocidad_promedio_kmh, congestion_pct (solo tráfico), anio, mes.
Unidades (contrato.UNIDADES): trafico = vehiculos/hora, energia = kWh.

Uso:  .\\scripts\\spark_submit.ps1 etl_trafico_energia
"""
import argparse

from pyspark.sql import functions as F, types as T
from pyspark.sql.window import Window

from common import RUTAS, get_spark

ZONAS = ("Zona Norte", "Zona Sur", "Zona Centro", "Zona Este", "Zona Oeste")
FORMATO_TS = "yyyy-MM-dd'T'HH:mm:ssX"

ESQUEMA_TRAFICO = T.StructType([
    T.StructField("timestamp_utc", T.StringType()),
    T.StructField("zona", T.StringType()),
    T.StructField("id_sensor", T.StringType()),
    T.StructField("vehiculos_hora", T.IntegerType()),
    T.StructField("velocidad_promedio_kmh", T.DoubleType()),
    T.StructField("congestion_pct", T.DoubleType()),
    T.StructField("origen", T.StringType()),
])
ESQUEMA_ENERGIA = T.StructType([
    T.StructField("timestamp_utc", T.StringType()),
    T.StructField("zona", T.StringType()),
    T.StructField("id_sensor", T.StringType()),
    T.StructField("consumo_kwh", T.DoubleType()),
    T.StructField("origen", T.StringType()),
])


def leer_trafico(spark, ruta):
    return (spark.read.option("header", "true").schema(ESQUEMA_TRAFICO).csv(ruta)
            .select(
                "zona", F.to_timestamp("timestamp_utc", FORMATO_TS).alias("timestamp"), "id_sensor",
                F.lit("trafico").alias("tipo_metrica"),
                F.col("vehiculos_hora").cast("double").alias("valor"),
                "origen", "velocidad_promedio_kmh", "congestion_pct",
            ))


def leer_energia(spark, ruta):
    return (spark.read.option("header", "true").schema(ESQUEMA_ENERGIA).csv(ruta)
            .select(
                "zona", F.to_timestamp("timestamp_utc", FORMATO_TS).alias("timestamp"), "id_sensor",
                F.lit("energia").alias("tipo_metrica"),
                F.col("consumo_kwh").alias("valor"),
                "origen",
                F.lit(None).cast("double").alias("velocidad_promedio_kmh"),
                F.lit(None).cast("double").alias("congestion_pct"),
            ))


def limpiar(df):
    valido = (
        F.col("zona").isin(*ZONAS)
        & F.col("timestamp").isNotNull()
        & F.col("id_sensor").isNotNull()
        & F.col("valor").isNotNull() & (~F.isnan("valor")) & (F.col("valor") >= 0)
        & (F.col("origen") == "sintetico")
        & (F.col("velocidad_promedio_kmh").isNull() | F.col("velocidad_promedio_kmh").between(0, 150))
        & (F.col("congestion_pct").isNull() | F.col("congestion_pct").between(0, 100))
    )
    ventana = Window.partitionBy("id_sensor", "timestamp", "tipo_metrica").orderBy(F.asc("valor"))
    return (df.filter(valido)
            .withColumn("rn", F.row_number().over(ventana)).filter("rn = 1").drop("rn")
            .withColumn("fuente", F.lit("Spark"))
            .withColumn("anio", F.year("timestamp"))
            .withColumn("mes", F.month("timestamp"))
            .select("zona", "timestamp", "id_sensor", "tipo_metrica", "valor", "fuente", "origen",
                    "velocidad_promedio_kmh", "congestion_pct", "anio", "mes"))


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--trafico", default=RUTAS["raw"] + "/trafico/trafico_*.csv")
    ap.add_argument("--energia", default=RUTAS["raw"] + "/energia/energia_*.csv")
    ap.add_argument("--salida", default=RUTAS["trafico_energia"])
    args = ap.parse_args()
    if not args.salida.startswith("hdfs://namenode:8020/smartcity/"):
        ap.error("--salida debe estar dentro de hdfs://namenode:8020/smartcity/")

    spark = get_spark("SmartCity-B3.3-ETL-Trafico-Energia")
    try:
        crudo = leer_trafico(spark, args.trafico).unionByName(leer_energia(spark, args.energia))
        entrada = crudo.count()
        curado = limpiar(crudo)
        curado.write.mode("overwrite").partitionBy("anio", "mes").parquet(args.salida)
        leido = spark.read.parquet(args.salida)
        n = leido.count()
        print(f"SMARTCITY_B33_ENTRADA={entrada}", flush=True)
        print(f"SMARTCITY_B33_FILAS_PARQUET={n}", flush=True)
        print(f"SMARTCITY_B33_PCT_DESCARTE={100 * (entrada - n) / max(entrada, 1):.2f}", flush=True)
        print(f"SMARTCITY_B33_SALIDA={args.salida}", flush=True)
        leido.groupBy("tipo_metrica", "zona").agg(
            F.count("*").alias("filas"), F.round(F.avg("valor"), 2).alias("promedio"),
        ).orderBy("tipo_metrica", "zona").show(20, truncate=False)
        print("SMARTCITY_B33_RESUMEN=OK", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
