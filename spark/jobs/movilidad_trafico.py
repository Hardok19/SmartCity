#!/usr/bin/env python3
"""B4.2 Movilidad: analiza volumen, velocidad y congestion SIMULADOS.

Los conteos vehiculares NO son datos reales de TfL. Se unen por zona/hora
con PM2.5 y precipitacion por separado para mostrar fuentes y cobertura.
No sustituye ni modifica el analisis ambiental de movilidad existente.
"""
import argparse
from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.sql.window import Window

TRAFICO = "hdfs://namenode:8020/smartcity/raw/trafico/*.csv"
AMBIENTE = "hdfs://namenode:8020/smartcity/sandbox/etl_ambiente_completa"
SALIDA = "hdfs://namenode:8020/smartcity/sandbox/analitica"

ESQUEMA = T.StructType([
    T.StructField("timestamp_utc", T.StringType()),
    T.StructField("zona", T.StringType()),
    T.StructField("id_sensor", T.StringType()),
    T.StructField("vehiculos_hora", T.IntegerType()),
    T.StructField("velocidad_promedio_kmh", T.DoubleType()),
    T.StructField("congestion_pct", T.DoubleType()),
    T.StructField("origen", T.StringType()),
])


def analizar_trafico(spark, ambiente=None, entrada_trafico=TRAFICO,
                    entrada_ambiente=AMBIENTE, salida=SALIDA):
    if ambiente is None:
        ambiente = spark.read.parquet(entrada_ambiente)
    trafico = (
        spark.read.option("header", "true").schema(ESQUEMA).csv(entrada_trafico)
        .withColumn("timestamp", F.to_timestamp("timestamp_utc", "yyyy-MM-dd'T'HH:mm:ssX"))
        .filter(
            F.col("timestamp").isNotNull()
            & F.col("zona").isin("Zona Norte", "Zona Sur", "Zona Centro", "Zona Este", "Zona Oeste")
            & F.col("id_sensor").isNotNull()
            & (F.col("origen") == "sintetico")
            & (F.col("vehiculos_hora") >= 0)
            & F.col("velocidad_promedio_kmh").between(0, 150)
            & F.col("congestion_pct").between(0, 100)
        )
        .dropDuplicates(["id_sensor", "timestamp"])
        .select("zona", "id_sensor", "timestamp", "vehiculos_hora",
                "velocidad_promedio_kmh", "congestion_pct", "origen")
        .cache()
    )
    n = trafico.count()
    if n < 1:
        raise RuntimeError("No se pudo leer trafico; suba primero data/raw/trafico a HDFS")
    print(f"SMARTCITY_MOV_TRAFICO_ENTRADA={n}", flush=True)

    try:
        local = (
            trafico.withColumn("fecha_hora_local", F.from_utc_timestamp("timestamp", "Europe/London"))
            .withColumn("fecha_local", F.to_date("fecha_hora_local"))
            .withColumn("hora_local", F.hour("fecha_hora_local"))
            .withColumn("tipo_dia", F.when(F.dayofweek("fecha_hora_local").between(2, 6), "laborable")
                       .otherwise("fin_de_semana"))
        )
        # Agregar por zona y hora; si en el futuro hay varios sensores evita sesgo.
        horas = local.groupBy("zona", "fecha_local", "hora_local", "tipo_dia").agg(
            F.sum("vehiculos_hora").alias("vehiculos_hora"),
            F.avg("velocidad_promedio_kmh").alias("velocidad_promedio_kmh"),
            F.avg("congestion_pct").alias("congestion_pct"),
        )
        perfil = horas.groupBy("zona", "tipo_dia", "hora_local").agg(
            F.count("*").alias("horas_observadas_simuladas"),
            F.countDistinct("fecha_local").alias("dias_cubiertos"),
            F.round(F.avg("vehiculos_hora"), 2).alias("vehiculos_promedio_hora"),
            F.round(F.avg("velocidad_promedio_kmh"), 2).alias("velocidad_promedio_kmh"),
            F.round(F.avg("congestion_pct"), 2).alias("congestion_promedio_pct"),
        ).withColumn("origen", F.lit("sintetico"))
        ruta = f"{salida}/movilidad_trafico_perfil_horario"
        perfil.write.mode("overwrite").parquet(ruta)
        guardado = spark.read.parquet(ruta)
        print(f"SMARTCITY_MOV_TRAFICO_PERFIL_FILAS={guardado.count()}", flush=True)
        print(f"SMARTCITY_MOV_TRAFICO_PERFIL_RUTA={ruta}", flush=True)

        # La hora pico se DEDUCE del conteo, no se fuerza a 7/8/18.
        ventana = Window.partitionBy("zona", "tipo_dia").orderBy(
            F.desc("vehiculos_promedio_hora"), F.asc("hora_local")
        )
        pico = guardado.withColumn("posicion", F.row_number().over(ventana))
        pico = pico.filter(F.col("posicion") == 1).drop("posicion")
        ruta_pico = f"{salida}/movilidad_trafico_horas_pico"
        pico.write.mode("overwrite").parquet(ruta_pico)
        salida_pico = spark.read.parquet(ruta_pico)
        if salida_pico.count() != 10:
            raise RuntimeError("Se esperaban 5 zonas x 2 tipos de dia en el ranking de trafico")
        print("SMARTCITY_MOV_TRAFICO_PICOS_FILAS=10", flush=True)
        print(f"SMARTCITY_MOV_TRAFICO_PICOS_RUTA={ruta_pico}", flush=True)
        salida_pico.orderBy("zona", "tipo_dia").show(10, truncate=False)

        # CRUCE AMBIENTAL: separar origen de PM2.5 observado/sintetico.
        trafico_utc = trafico.groupBy("zona", F.date_trunc("hour", "timestamp").alias("hora_utc")).agg(
            F.sum("vehiculos_hora").alias("vehiculos_hora"),
            F.avg("congestion_pct").alias("congestion_pct"),
        )
        pm25 = (
            ambiente.filter((F.col("tipo_metrica") == "pm25")
                            & F.col("origen").isin("openaq", "sintetico"))
            .groupBy("zona", F.date_trunc("hour", "timestamp").alias("hora_utc"), "origen")
            .agg(F.avg("valor").alias("pm25_promedio"))
        )
        interseccion = trafico_utc.join(pm25, ["zona", "hora_utc"], "inner")
        corr = interseccion.groupBy("zona", "origen").agg(
            F.count("*").alias("horas_emparejadas"),
            F.round(F.corr("vehiculos_hora", "pm25_promedio"), 4).alias("correlacion_trafico_pm25"),
            F.round(F.avg("vehiculos_hora"), 2).alias("vehiculos_promedio"),
            F.round(F.avg("pm25_promedio"), 3).alias("pm25_promedio"),
        ).withColumnRenamed("origen", "origen_pm25").withColumn("origen_trafico", F.lit("sintetico"))
        ruta_corr = f"{salida}/movilidad_trafico_correlacion_pm25"
        corr.write.mode("overwrite").parquet(ruta_corr)
        corr_salida = spark.read.parquet(ruta_corr)
        print(f"SMARTCITY_MOV_TRAFICO_PM25_FILAS={corr_salida.count()}", flush=True)
        print(f"SMARTCITY_MOV_TRAFICO_PM25_RUTA={ruta_corr}", flush=True)
        corr_salida.orderBy("zona", "origen_pm25").show(12, truncate=False)

        # Integracion con la SEGUNDA fuente, clima externo de Open-Meteo.
        lluvia = (
            ambiente.filter((F.col("tipo_metrica") == "precipitacion")
                            & (F.col("origen") == "open-meteo"))
            .groupBy("zona", F.date_trunc("hour", "timestamp").alias("hora_utc"))
            .agg(F.avg("valor").alias("precipitacion_mm"))
        )
        meteo = trafico_utc.join(lluvia, ["zona", "hora_utc"], "inner").groupBy("zona").agg(
            F.count("*").alias("horas_emparejadas"),
            F.round(F.corr("vehiculos_hora", "precipitacion_mm"), 4).alias("correlacion_trafico_lluvia"),
            F.round(F.avg("precipitacion_mm"), 3).alias("lluvia_promedio_mm"),
        ).withColumn("origen_trafico", F.lit("sintetico"))
        ruta_meteo = f"{salida}/movilidad_trafico_cruce_meteo"
        meteo.write.mode("overwrite").parquet(ruta_meteo)
        meteo_salida = spark.read.parquet(ruta_meteo)
        print(f"SMARTCITY_MOV_TRAFICO_METEO_FILAS={meteo_salida.count()}", flush=True)
        print(f"SMARTCITY_MOV_TRAFICO_METEO_RUTA={ruta_meteo}", flush=True)
        meteo_salida.orderBy("zona").show(5, truncate=False)
        print("SMARTCITY_MOV_TRAFICO_AVISO=Conteos y velocidad SIMULADOS; correlacion NO implica causalidad", flush=True)
    finally:
        trafico.unpersist()


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--entrada-trafico", default=TRAFICO)
    ap.add_argument("--entrada-ambiente", default=AMBIENTE)
    ap.add_argument("--salida", default=SALIDA)
    args = ap.parse_args()
    if not args.salida.startswith("hdfs://namenode:8020/smartcity/sandbox/"):
        ap.error("La salida debe permanecer en HDFS sandbox")
    spark = (SparkSession.builder.appName("SmartCity-B42-Movilidad-Trafico-Simulado")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.shuffle.partitions", "8").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    try:
        analizar_trafico(spark, entrada_trafico=args.entrada_trafico,
                        entrada_ambiente=args.entrada_ambiente, salida=args.salida)
        print("SMARTCITY_MOV_TRAFICO_RESUMEN=OK", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
