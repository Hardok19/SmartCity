#!/usr/bin/env python3
"""B3.2: ETL reproducible sobre HDFS raw, SIN sobrescribir ETL original.

Entradas: raw OpenAQ CSV + raw Open-Meteo JSON + export Mongo lectura JSONL.
Esquemas explicitos, validacion, catalogo maestro CSV, deduplicacion entre
raw y Mongo. Parquet largo con particiones anio/mes en RUTAS["ambiente_hdfs"]
(/smartcity/curated/ambiente_hdfs); nunca sobrescribe RUTAS["ambiente"] (ETL desde Mongo).

Nota: JOIN definitivo por JDBC con maestros PostgreSQL queda pendiente hasta
que la pareja A publique sus tablas y credenciales de lectura definitivas.
"""
import argparse
from pyspark.sql import functions as F, types as T
from pyspark.sql.window import Window

from common import CATALOGO_SENSORES, RUTAS, get_spark, salida_permitida

HDFS = "hdfs://namenode:8020/smartcity"
SALIDA = RUTAS["ambiente_hdfs"]
CATALOGO = CATALOGO_SENSORES
METRICAS = ("pm25", "pm10", "co", "no2", "o3", "co2", "temperatura", "humedad", "precipitacion", "viento")
ZONAS = ("Zona Norte", "Zona Sur", "Zona Centro", "Zona Este", "Zona Oeste")

ESQUEMA_MAESTROS = T.StructType([
    T.StructField(nombre, T.StringType()) for nombre in (
        "id_sensor", "nombre", "tipo_metrica", "categoria", "zona", "lat", "lon",
        "fecha_instalacion", "estado", "organismo", "origen", "id_estacion_openaq",
        "frecuencia_min", "envia_datos", "fecha_baja")
])
ESQUEMA_AQ = T.StructType([
    T.StructField("timestamp_utc", T.StringType()),
    T.StructField("parameter", T.StringType()),
    T.StructField("value", T.DoubleType()),
    T.StructField("unit", T.StringType()),
])
ESQUEMA_METEO = T.StructType([
    T.StructField("hourly", T.StructType([
        T.StructField("time", T.ArrayType(T.StringType())),
        T.StructField("temperature_2m", T.ArrayType(T.DoubleType())),
        T.StructField("relative_humidity_2m", T.ArrayType(T.DoubleType())),
        T.StructField("precipitation", T.ArrayType(T.DoubleType())),
        T.StructField("wind_speed_10m", T.ArrayType(T.DoubleType())),
    ])),
])
ESQUEMA_MONGO = T.StructType([
    T.StructField("sensor_id", T.StringType()),
    T.StructField("tipo", T.StringType()),
    T.StructField("timestamp", T.StructType([
        T.StructField("$date", T.StringType()),
    ])),
    T.StructField("ubicacion", T.StructType([
        T.StructField("zona", T.StringType()),
    ])),
    T.StructField("mediciones", T.StructType([
        T.StructField(m, T.DoubleType()) for m in METRICAS
    ])),
    T.StructField("origen", T.StringType()),
    T.StructField("calidad_dato", T.StructType([
        T.StructField("valido", T.BooleanType()),
    ])),
])


def maestro_sensores(spark, ruta):
    tabla = spark.read.option("header", "true").schema(ESQUEMA_MAESTROS).csv(ruta)
    base = tabla.select("id_sensor", "zona", "origen", "id_estacion_openaq", "categoria")
    if base.filter(F.col("id_sensor").isNull()).count() or base.groupBy("id_sensor").count().filter("count > 1").count():
        raise RuntimeError("Catalogo maestro: IDs vacios o duplicados")
    return base.cache()


def leer_openaq(spark, catalogo, ruta):
    lecturas = (
        spark.read.option("header", "true").schema(ESQUEMA_AQ).csv(ruta)
        .withColumn("id_estacion_openaq", F.regexp_extract(F.input_file_name(), r"/openaq/([0-9]+)/", 1))
        .withColumn("timestamp", F.to_timestamp("timestamp_utc"))
        .withColumn("tipo_metrica", F.regexp_replace(F.lower("parameter"), r"[._]", ""))
        .withColumnRenamed("value", "valor")
    )
    ref = catalogo.filter(F.col("origen") == "openaq").select(
        "id_sensor", "zona", "id_estacion_openaq", "categoria")
    return (lecturas.join(F.broadcast(ref), "id_estacion_openaq", "inner")
            .withColumn("origen", F.lit("openaq"))
            .withColumn("fuente_raw", F.lit("openaq_csv"))
            .withColumn("calidad_valida", F.lit(True))
            .select("zona", "timestamp", "id_sensor", "categoria", "origen",
                    "tipo_metrica", "valor", "fuente_raw", "calidad_valida"))


def leer_openmeteo(spark, catalogo, ruta):
    fuente = (
        spark.read.schema(ESQUEMA_METEO).json(ruta)
        .withColumn("zona_slug", F.regexp_extract(F.input_file_name(), r"/zona_([^_/]+)_[0-9]{4}\.json$", 1))
        .withColumn("zona", F.concat(F.lit("Zona "), F.initcap("zona_slug")))
        .withColumn("h", F.explode(F.arrays_zip(
            F.col("hourly.time").alias("time"),
            F.col("hourly.temperature_2m").alias("temperature_2m"),
            F.col("hourly.relative_humidity_2m").alias("relative_humidity_2m"),
            F.col("hourly.precipitation").alias("precipitation"),
            F.col("hourly.wind_speed_10m").alias("wind_speed_10m"))))
        .withColumn("timestamp", F.to_timestamp(F.col("h.time"), "yyyy-MM-dd'T'HH:mm"))
    )
    datos = fuente.select(
        "zona", "timestamp",
        F.explode(F.array(
            F.struct(F.lit("temperatura").alias("tipo_metrica"), F.col("h.temperature_2m").alias("valor")),
            F.struct(F.lit("humedad").alias("tipo_metrica"), F.col("h.relative_humidity_2m").alias("valor")),
            F.struct(F.lit("precipitacion").alias("tipo_metrica"), F.col("h.precipitation").alias("valor")),
            F.struct(F.lit("viento").alias("tipo_metrica"), F.col("h.wind_speed_10m").alias("valor")),
        )).alias("medicion"),
    ).select("zona", "timestamp", "medicion.tipo_metrica", "medicion.valor")
    ref = catalogo.filter(F.col("origen") == "open-meteo").select("id_sensor", "zona", "categoria")
    return (datos.join(F.broadcast(ref), "zona", "inner")
            .withColumn("origen", F.lit("open-meteo"))
            .withColumn("fuente_raw", F.lit("openmeteo_json"))
            .withColumn("calidad_valida", F.lit(True))
            .select("zona", "timestamp", "id_sensor", "categoria", "origen",
                    "tipo_metrica", "valor", "fuente_raw", "calidad_valida"))


def leer_mongo(spark, ruta):
    raw = spark.read.schema(ESQUEMA_MONGO).json(ruta)
    pares = F.array(*[
        F.struct(F.lit(nombre).alias("tipo_metrica"),
                 F.col(f"mediciones.{nombre}").alias("valor")) for nombre in METRICAS
    ])
    return (raw.select(F.col("ubicacion.zona").alias("zona"),
                    F.to_timestamp(F.col("timestamp.`$date`")).alias("timestamp"),
                    F.col("sensor_id").alias("id_sensor"),
                    F.col("tipo").alias("categoria"), "origen",
                    F.col("calidad_dato.valido").alias("calidad_valida"),
                    F.explode(pares).alias("m"))
            .select("zona", "timestamp", "id_sensor", "categoria", "origen", "calidad_valida",
                    F.col("m.tipo_metrica").alias("tipo_metrica"),
                    F.col("m.valor").alias("valor"))
            .withColumn("fuente_raw", F.lit("mongo_jsonl")))


def transformar(spark, aq, meteo, mongo, catalogo):
    # Contar mediciones realmente presentes, no huecos de la estructura JSON.
    combinado = aq.unionByName(meteo).unionByName(mongo).filter(F.col("valor").isNotNull())
    entrada = combinado.count()
    print(f"SMARTCITY_B32_MEDICIONES_ENTRADA={entrada}", flush=True)
    valido = (
        F.col("zona").isin(*ZONAS)
        & F.col("timestamp").isNotNull()
        & F.col("id_sensor").isNotNull()
        & F.col("tipo_metrica").isin(*METRICAS)
        & (F.col("calidad_valida").isNull() | F.col("calidad_valida"))
        & (~F.isnan("valor"))
        & (
            ((F.col("tipo_metrica") == "temperatura") & F.col("valor").between(-60, 60))
            | ((F.col("tipo_metrica") == "humedad") & F.col("valor").between(0, 100))
            | ((~F.col("tipo_metrica").isin("temperatura", "humedad")) & (F.col("valor") >= 0))
        )
    )
    limpios = combinado.filter(valido)
    antes_catalogo = limpios.count()
    # Integridad referencial por catalogo LOCAL, no se presenta como JOIN a PostgreSQL.
    ref = catalogo.select(F.col("id_sensor").alias("ref_sensor"),
                          F.col("zona").alias("ref_zona"),
                          F.col("categoria").alias("ref_categoria"))
    asignados = (limpios.join(F.broadcast(ref), F.col("id_sensor") == F.col("ref_sensor"), "inner")
                 .filter(F.col("zona") == F.col("ref_zona"))
                 .drop("ref_sensor", "ref_zona", "categoria")
                 .withColumnRenamed("ref_categoria", "categoria"))
    antes_dedupe = asignados.count()
    # Precedencia: medicion cruda externa > replica Mongo > sintesis Mongo.
    orden = F.when(F.col("fuente_raw") == "openaq_csv", 1).when(
        F.col("fuente_raw") == "openmeteo_json", 1).otherwise(2)
    ventana = Window.partitionBy("id_sensor", "timestamp", "tipo_metrica").orderBy(
        orden, F.asc("origen"), F.asc("valor"))
    curado = (
        asignados.withColumn("rn", F.row_number().over(ventana))
        .filter(F.col("rn") == 1).drop("rn")
        .withColumn("fuente", F.lit("Spark"))
        .withColumn("anio", F.year("timestamp"))
        .withColumn("mes", F.month("timestamp"))
        .select("zona", "timestamp", "id_sensor", "tipo_metrica", "valor",
                "fuente", "origen", "categoria", "fuente_raw", "anio", "mes")
    )
    return curado, entrada, antes_catalogo, antes_dedupe


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--fecha-mongo", required=True, help="Snapshot HDFS, AAAA-MM-DD")
    ap.add_argument("--salida", default=SALIDA)
    ap.add_argument("--catalogo", default=CATALOGO)
    args = ap.parse_args()
    if not salida_permitida(args.salida) or args.salida.rstrip("/") == RUTAS["ambiente"]:
        ap.error("--salida debe estar en curated, analytics o sandbox y no puede ser RUTAS['ambiente'] (ETL desde Mongo)")
    if len(args.fecha_mongo) != 10 or args.fecha_mongo[4] != '-' or args.fecha_mongo[7] != '-':
        ap.error("--fecha-mongo debe ser AAAA-MM-DD")
    spark = get_spark("SmartCity-B32-HDFS-RAW-ETL")
    catalogo = None
    try:
        catalogo = maestro_sensores(spark, args.catalogo)
        aq = leer_openaq(spark, catalogo, HDFS + "/raw/openaq/*/*.csv")
        meteo = leer_openmeteo(spark, catalogo, HDFS + "/raw/openmeteo/*.json")
        mongo = leer_mongo(spark,
            HDFS + f"/raw/mongo_exports/lectura/fecha={args.fecha_mongo}/lectura.jsonl")
        curado, entrada, pre_catalogo, pre_dedupe = transformar(spark, aq, meteo, mongo, catalogo)
        curado.write.mode("overwrite").partitionBy("anio", "mes").parquet(args.salida)
        leido = spark.read.parquet(args.salida)
        n_final = leido.count()
        descartados = entrada - n_final
        print(f"SMARTCITY_B32_VALIDAS_ANTES_CATALOGO={pre_catalogo}", flush=True)
        print(f"SMARTCITY_B32_VALIDAS_ANTES_DEDUP={pre_dedupe}", flush=True)
        print(f"SMARTCITY_B32_FILAS_PARQUET={n_final}", flush=True)
        print(f"SMARTCITY_B32_DESCARTADAS_O_DEDUP={descartados}", flush=True)
        print(f"SMARTCITY_B32_PCT_DESCARTE={100 * descartados / max(entrada,1):.2f}", flush=True)
        print(f"SMARTCITY_B32_SALIDA={args.salida}", flush=True)
        print("SMARTCITY_B32_CATALOGO=CSV_LOCAL_PENDIENTE_JOIN_POSTGRESQL", flush=True)
        leido.groupBy("origen", "fuente_raw").count().orderBy("origen", "fuente_raw").show(20, truncate=False)
        if n_final < 1_000_000:
            raise RuntimeError("El ETL integral no alcanzo 1 millon de filas; revisar entrada")
        print("SMARTCITY_B32_RESUMEN=OK", flush=True)
    finally:
        if catalogo is not None:
            catalogo.unpersist()
        spark.stop()


if __name__ == "__main__":
    main()
