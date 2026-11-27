#!/usr/bin/env python3
"""Reto adicional — Spark Structured Streaming: alertas en tiempo casi real.

Fuente: archivos JSON Lines que llegan continuamente a HDFS /smartcity/streaming/entrada
(scripts/streaming_productor.py). Cada micro-lote (trigger cada 10 s):
  1. agrupa por ventana de 1 minuto (tiempo del evento, watermark de 2 minutos) y zona:
     PM2.5 promedio y máximo, vehículos/hora promedio, nº de lecturas;
  2. regla de contaminación: PM2.5 promedio > umbral (35,4 µg/m³, config/umbrales.yaml)  -> alerta
     regla de congestión:   vehículos/hora promedio > límite (1200 por contador)          -> alerta
  3. escribe (upsert, idempotente) en MongoDB: `stream_ventana` (todas las ventanas, para el dashboard en
     vivo) y `alerta` (regla stream_pm25 / stream_congestion, estado "abierta"); y las alertas en HDFS
     /smartcity/streaming/alertas (Parquet, histórico).

Batch frente a streaming: los ETL y análisis de B3/B4 procesan TODO el histórico una vez; aquí el mismo
motor (Spark SQL / DataFrames) procesa SOLO lo nuevo de cada micro-lote, guardando su estado (ventanas
abiertas, archivos ya leídos) en el checkpoint de HDFS para retomar sin duplicar si el job se reinicia.

    .\\scripts\\spark_submit.ps1 streaming_alertas --duracion 300
"""
import argparse

from pyspark.sql import functions as F, types as T

from common import BASE, MONGO_DB, get_spark, mongo_uri

ENTRADA = BASE + "/streaming/entrada"
ALERTAS_HDFS = BASE + "/streaming/alertas"
CHECKPOINT = BASE + "/streaming/checkpoints/alertas"
ID_ZONA = {"Zona Norte": 1, "Zona Sur": 2, "Zona Centro": 3, "Zona Este": 4, "Zona Oeste": 5}
NIVELES_PM25 = ((150.4, "critica"), (55.4, "alta"), (35.4, "media"))   # config/umbrales.yaml

ESQUEMA = T.StructType([
    T.StructField("sensor_id", T.StringType()),
    T.StructField("tipo", T.StringType()),
    T.StructField("zona", T.StringType()),
    T.StructField("timestamp", T.StringType()),
    T.StructField("variable", T.StringType()),
    T.StructField("valor", T.DoubleType()),
    T.StructField("origen", T.StringType()),
])


def escribir_mongo(df, coleccion):
    (df.write.format("mongodb").mode("append")
     .option("connection.uri", mongo_uri()).option("database", MONGO_DB).option("collection", coleccion)
     .option("operationType", "replace").option("upsertDocument", "true")
     .save())


def procesar_lote(umbral_pm25, umbral_vehiculos):
    zona_id = F.create_map(*[x for z, i in ID_ZONA.items() for x in (F.lit(z), F.lit(i))])

    def procesar(lote, id_lote):
        lote = lote.persist()
        n = lote.count()
        if n == 0:
            lote.unpersist()
            return
        ventanas = (lote.select(
            F.concat_ws("|", "zona", F.date_format("ventana.start", "yyyy-MM-dd'T'HH:mm:ss")).alias("_id"),
            "zona", F.col("ventana.start").alias("inicio"), F.col("ventana.end").alias("fin"),
            F.round("pm25_promedio", 2).alias("pm25_promedio"), F.round("pm25_maximo", 2).alias("pm25_maximo"),
            F.round("vehiculos_promedio", 1).alias("vehiculos_promedio"), "lecturas",
            F.current_timestamp().alias("actualizado")))
        escribir_mongo(ventanas, "stream_ventana")

        base_alerta = lambda regla, variable, valor, umbral, nivel: lote.select(  # noqa: E731
            F.concat_ws("|", F.lit(regla), "zona", F.date_format("ventana.start", "yyyy-MM-dd'T'HH:mm:ss")).alias("_id"),
            F.lit(regla).alias("regla"), F.lit(variable).alias("variable"),
            F.round(valor, 2).alias("valor"), F.lit(float(umbral)).alias("umbral"), nivel.alias("nivel"),
            F.col("ventana.end").alias("emitida"), F.lit("abierta").alias("estado"),
            F.struct(zona_id[F.col("zona")].cast("int").alias("id_zona"), F.col("zona").alias("nombre")).alias("zona"),
            F.array().cast("array<string>").alias("acciones"), F.lit("streaming").alias("origen"),
            F.col("ventana.start").alias("ventana_inicio"))
        nivel_pm25 = F.when(F.col("pm25_promedio") > NIVELES_PM25[0][0], NIVELES_PM25[0][1]) \
            .when(F.col("pm25_promedio") > NIVELES_PM25[1][0], NIVELES_PM25[1][1]).otherwise(NIVELES_PM25[2][1])
        alertas = (base_alerta("stream_pm25", "pm25", F.col("pm25_promedio"), umbral_pm25, nivel_pm25)
                   .where(F.col("valor") > umbral_pm25)
                   .unionByName(base_alerta("stream_congestion", "vehiculos_hora", F.col("vehiculos_promedio"),
                                            umbral_vehiculos, F.lit("alta"))
                                .where(F.col("valor") > umbral_vehiculos)))
        n_alertas = alertas.count()
        if n_alertas:
            escribir_mongo(alertas, "alerta")
            alertas.drop("_id").write.mode("append").parquet(ALERTAS_HDFS)
        print(f"SMARTCITY_STREAM_MICROLOTE id={id_lote} ventanas={n} alertas={n_alertas}", flush=True)
        lote.unpersist()
    return procesar


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--duracion", type=int, default=300, help="segundos que corre el job (0 = sin fin)")
    ap.add_argument("--umbral-pm25", type=float, default=35.4)
    ap.add_argument("--umbral-vehiculos", type=float, default=1200.0)
    ap.add_argument("--ventana", default="1 minute")
    ap.add_argument("--trigger", default="10 seconds")
    a = ap.parse_args()

    spark = get_spark("SmartCity-Streaming-Alertas", shuffle_partitions=4)
    entrada = (spark.readStream.schema(ESQUEMA).option("maxFilesPerTrigger", 50)
               .json(ENTRADA + "/lote_*.json")
               .withColumn("timestamp", F.to_timestamp("timestamp", "yyyy-MM-dd'T'HH:mm:ssX")))
    agregado = (entrada.withWatermark("timestamp", "2 minutes")
                .groupBy(F.window("timestamp", a.ventana).alias("ventana"), "zona")
                .agg(F.avg(F.when(F.col("variable") == "pm25", F.col("valor"))).alias("pm25_promedio"),
                     F.max(F.when(F.col("variable") == "pm25", F.col("valor"))).alias("pm25_maximo"),
                     F.avg(F.when(F.col("variable") == "vehiculos_hora", F.col("valor"))).alias("vehiculos_promedio"),
                     F.count("*").alias("lecturas")))
    consulta = (agregado.writeStream.outputMode("update")
                .foreachBatch(procesar_lote(a.umbral_pm25, a.umbral_vehiculos))
                .option("checkpointLocation", CHECKPOINT)
                .trigger(processingTime=a.trigger)
                .queryName("smartcity_alertas")
                .start())
    print(f"SMARTCITY_STREAM_INICIADO entrada={ENTRADA} umbral_pm25={a.umbral_pm25} "
          f"umbral_vehiculos={a.umbral_vehiculos} duracion={a.duracion or 'sin fin'}", flush=True)
    if a.duracion:
        consulta.awaitTermination(a.duracion)
        consulta.stop()
    else:
        consulta.awaitTermination()
    print("SMARTCITY_STREAM_FIN", flush=True)
    spark.stop()


if __name__ == "__main__":
    main()
