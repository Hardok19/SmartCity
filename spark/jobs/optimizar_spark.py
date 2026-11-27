#!/usr/bin/env python3
"""SmartCity B4.3: benchmarks reproducibles de Spark SQL/DataFrames.

Lee el ETL curado en HDFS; solo escribe salidas en /smartcity/sandbox/optimizacion_b43.
No altera MongoDB, PostgreSQL, ni las tablas de Spark B4.1/B4.2.

Ejemplos:
    spark-submit optimizar_spark.py --pruebas particiones join --repeticiones 2
    spark-submit optimizar_spark.py --pruebas todas --repeticiones 2

IMPORTANTE: una mejora NO esta garantizada; comparar las mediciones reales.
"""
import argparse
import statistics
import time
from collections import defaultdict

from pyspark import StorageLevel
from pyspark.sql import SparkSession, functions as F, types as T

ENTRADA = "hdfs://namenode:8020/smartcity/sandbox/etl_ambiente_completa"
DESTINO = "hdfs://namenode:8020/smartcity/sandbox/optimizacion_b43"
PRUEBAS = ("particiones", "join", "aqe", "cache", "csv")


def construir_consulta_zona(base):
    """Una agregacion que lee 'valor' y 'zona', para ambos metodos de lectura."""
    return base.groupBy("zona").agg(
        F.count("*").alias("total"),
        F.round(F.avg("valor"), 5).alias("promedio"),
    )


def ejecutar(prueba, variante, repeticion, consulta, mediciones):
    inicio = time.perf_counter()
    filas = consulta.collect()  # accion real; Spark evalua trabajos de forma perezosa
    segundos = time.perf_counter() - inicio
    # Los conteos enteros son controles de equivalencia independientes del orden de ejecucion.
    total = sum(int(f["total"]) for f in filas)
    control = tuple(sorted((str(f["zona"]), int(f["total"]),
                            None if f["promedio"] is None else float(f["promedio"]))
                           for f in filas))
    mediciones.append((prueba, variante, repeticion, float(segundos), len(filas), total))
    print(
        f"B43_TIEMPO prueba={prueba} variante={variante} "
        f"rep={repeticion} segundos={segundos:.3f} total={total} filas={len(filas)}",
        flush=True,
    )
    return control


def controles_iguales(uno, otro):
    if len(uno) != len(otro):
        return False
    for a, b in zip(uno, otro):
        if a[:2] != b[:2]:
            return False
        if (a[2] is None) != (b[2] is None):
            return False
        if a[2] is not None and abs(a[2] - b[2]) > 0.001:
            return False
    return True


def medir_pareja(prueba, variantes, repeticiones, mediciones, planes=True):
    """Compara consultas equivalentes y alterna orden para reducir sesgo de cache."""
    consultas = {nombre: construir() for nombre, construir in variantes}
    if planes:
        for nombre, consulta in consultas.items():
            print(f"=== B43_EXPLAIN prueba={prueba} variante={nombre} ===", flush=True)
            consulta.explain(mode="formatted")
    referencia = None
    for repeticion in range(1, repeticiones + 1):
        orden = list(consultas.items())
        if repeticion % 2 == 0:
            orden.reverse()
        for nombre, consulta in orden:
            control = ejecutar(prueba, nombre, repeticion, consulta, mediciones)
            if referencia is None:
                referencia = control
            elif not controles_iguales(referencia, control):
                raise RuntimeError(
                    f"B43_ERROR_RESULTADOS prueba={prueba} variante={nombre}: "
                    "los conteos difieren. No se puede comparar rendimiento."
                )
    print(f"B43_CONTROL_{prueba.upper()}=OK", flush=True)


def prueba_particiones(spark, df, repeticiones, mediciones):
    print("=== B43 PARTICIONES: con y sin pruning por anio ===", flush=True)
    # Mismo conjunto esperado: todas las lecturas de 2024. Zona horaria UTC.
    def sin_pruning():
        return construir_consulta_zona(df.where(F.year("timestamp") == 2024))

    def con_pruning():
        return construir_consulta_zona(df.where(F.col("anio") == 2024))

    medir_pareja("particiones", [
        ("filtro_timestamp", sin_pruning),
        ("filtro_particion_anio", con_pruning),
    ], repeticiones, mediciones)
    print("B43_NOTA_PARTICIONES=Revisar PartitionFilters en los EXPLAIN; no basta solo tiempo", flush=True)


def prueba_join(spark, df, repeticiones, mediciones):
    print("=== B43 JOIN: sort-merge frente a broadcast ===", flush=True)
    catalogo = (spark.read.option("header", True)
        .csv("/opt/smartcity/data/maestros/sensores.csv")
        .select("id_sensor", "categoria")
        .dropDuplicates(["id_sensor"]))
    base = df.where((F.col("anio") == 2024) & (F.col("tipo_metrica") == "pm25"))
    base = base.select("id_sensor", "zona", "valor")
    anterior_aqe = spark.conf.get("spark.sql.adaptive.enabled")
    anterior_umbral = spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
    anterior_shuffles = spark.conf.get("spark.sql.shuffle.partitions")
    try:
        # Desactivar auto-broadcast y AQE SOLO durante el contraste: así se distingue el hint.
        spark.conf.set("spark.sql.adaptive.enabled", "false")
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
        spark.conf.set("spark.sql.shuffle.partitions", "8")

        def consulta(join_cat):
            return (base.join(join_cat, on="id_sensor", how="inner")
                .groupBy("zona")
                .agg(F.count("*").alias("total"),
                     F.round(F.avg("valor"), 5).alias("promedio")))

        medir_pareja("join", [
            ("sin_broadcast_auto_desactivado", lambda: consulta(catalogo)),
            ("con_broadcast_hint", lambda: consulta(F.broadcast(catalogo))),
        ], repeticiones, mediciones)
    finally:
        spark.conf.set("spark.sql.adaptive.enabled", anterior_aqe)
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", anterior_umbral)
        spark.conf.set("spark.sql.shuffle.partitions", anterior_shuffles)


def prueba_aqe(spark, df, repeticiones, mediciones):
    print("=== B43 AQE: adaptive execution ON/OFF ===", flush=True)
    anterior_aqe = spark.conf.get("spark.sql.adaptive.enabled")
    anterior_shuffles = spark.conf.get("spark.sql.shuffle.partitions")
    try:
        spark.conf.set("spark.sql.shuffle.partitions", "32")
        for repeticion in range(1, repeticiones + 1):
            orden = ["sin_aqe", "con_aqe"]
            if repeticion % 2 == 0:
                orden.reverse()
            referencia = None
            for nombre in orden:
                spark.conf.set("spark.sql.adaptive.enabled", "true" if nombre == "con_aqe" else "false")
                consulta = construir_consulta_zona(df.where((F.col("anio") == 2024)
                    & (F.col("tipo_metrica") == "pm25")))
                if repeticion == 1:
                    print(f"=== B43_EXPLAIN prueba=aqe variante={nombre} ===", flush=True)
                    consulta.explain(mode="formatted")
                control = ejecutar("aqe", nombre, repeticion, consulta, mediciones)
                if referencia is None:
                    referencia = control
                elif not controles_iguales(referencia, control):
                    raise RuntimeError("B43_ERROR_RESULTADOS AQE: conteos diferentes")
        print("B43_CONTROL_AQE=OK", flush=True)
    finally:
        spark.conf.set("spark.sql.adaptive.enabled", anterior_aqe)
        spark.conf.set("spark.sql.shuffle.partitions", anterior_shuffles)


def prueba_cache(spark, df, repeticiones, mediciones):
    print("=== B43 CACHE: dos consultas reutilizando el mismo subconjunto ===", flush=True)
    base = (df.where((F.col("anio") == 2024) & (F.col("tipo_metrica") == "pm25"))
            .select("zona", "origen", "id_sensor", "valor"))
    def consultas(subset):
        q1 = construir_consulta_zona(subset)
        q2 = (subset.groupBy("zona", "origen")
              .agg(F.count("*").alias("total"), F.round(F.avg("valor"), 5).alias("promedio")))
        # Dos agrupaciones distintas sobre el mismo subconjunto: zona y zona-origen.
        return [q1, q2]

    sin_cache = consultas(base)
    t0 = time.perf_counter()
    control_sin = []
    for i, q in enumerate(sin_cache, start=1):
        control_sin.append(ejecutar("cache", "sin_cache", i, q, mediciones))
    tiempo_sin = time.perf_counter() - t0

    cacheado = base.persist(StorageLevel.MEMORY_AND_DISK)
    try:
        inicio = time.perf_counter()
        n = cacheado.count()  # la materializacion es un coste REAL del cache
        segundos_materializar = time.perf_counter() - inicio
        print(f"B43_CACHE_MATERIALIZAR={segundos_materializar:.3f} filas={n}", flush=True)
        print("=== B43_EXPLAIN prueba=cache variante=con_cache ===", flush=True)
        consultas(cacheado)[0].explain(mode="formatted")
        t1 = time.perf_counter()
        control_con = []
        for i, q in enumerate(consultas(cacheado), start=1):
            control_con.append(ejecutar("cache", "con_cache", i, q, mediciones))
        tiempo_reutilizar = time.perf_counter() - t1
        if any(not controles_iguales(a, b) for a, b in zip(control_sin, control_con)):
            raise RuntimeError("B43_ERROR_RESULTADOS cache: conteos diferentes")
        print(f"B43_CACHE_SIN_CACHE_TOTAL={tiempo_sin:.3f}", flush=True)
        print(f"B43_CACHE_CON_CACHE_CON_MATERIALIZACION={segundos_materializar + tiempo_reutilizar:.3f}", flush=True)
        print(f"B43_CACHE_SOLO_REUTILIZACION={tiempo_reutilizar:.3f}", flush=True)
        print("B43_CONTROL_CACHE=OK", flush=True)
    finally:
        cacheado.unpersist(blocking=True)


def prueba_csv(spark, df, repeticiones, mediciones, destino):
    print("=== B43 CSV frente a PARQUET: misma muestra de PM2.5 de 2024 ===", flush=True)
    muestra = (df.where((F.col("anio") == 2024) & (F.col("tipo_metrica") == "pm25"))
        .select("zona", "id_sensor", "origen", "valor"))
    parquet = f"{destino}/datos_prueba_parquet"
    csv = f"{destino}/datos_prueba_csv"
    muestra.write.mode("overwrite").parquet(parquet)
    muestra.write.mode("overwrite").option("header", True).csv(csv)
    print("B43_CSV_NOTA=Los dos formatos se generaron a partir de exactamente el mismo DataFrame", flush=True)
    esquema = T.StructType([
        T.StructField("zona", T.StringType()),
        T.StructField("id_sensor", T.StringType()),
        T.StructField("origen", T.StringType()),
        T.StructField("valor", T.DoubleType()),
    ])
    medir_pareja("csv", [
        ("csv", lambda: construir_consulta_zona(spark.read.option("header", True).schema(esquema).csv(csv))),
        ("parquet", lambda: construir_consulta_zona(spark.read.parquet(parquet))),
    ], repeticiones, mediciones)
    print(f"B43_RUTA_DATOS_CSV={csv}", flush=True)
    print(f"B43_RUTA_DATOS_PARQUET={parquet}", flush=True)


def resumen_y_guardar(spark, mediciones, destino):
    by_key = defaultdict(list)
    for prueba, variante, rep, segundos, filas, total in mediciones:
        by_key[(prueba, variante)].append(segundos)
    print("=== B43_RESUMEN_MEDIANAS_SEGUNDOS ===", flush=True)
    for (prueba, variante), tiempos in sorted(by_key.items()):
        print(f"B43_MEDIANA prueba={prueba} variante={variante} segundos={statistics.median(tiempos):.3f}", flush=True)
    pares = {
        "particiones": ("filtro_timestamp", "filtro_particion_anio"),
        "join": ("sin_broadcast_auto_desactivado", "con_broadcast_hint"),
        "aqe": ("sin_aqe", "con_aqe"),
        "csv": ("csv", "parquet"),
    }
    for prueba, (base, opcion) in pares.items():
        a, b = by_key.get((prueba, base)), by_key.get((prueba, opcion))
        if a and b:
            med_base, med_opcion = statistics.median(a), statistics.median(b)
            mejora = 100.0 * (med_base - med_opcion) / med_base if med_base > 0 else 0.0
            print(f"B43_DIFERENCIA prueba={prueba} base={base} comparada={opcion} "
                  f"porcentaje={mejora:.2f} "
                  "(positivo=mejor; negativo=peor)", flush=True)
    esquema = T.StructType([
        T.StructField("prueba", T.StringType(), False),
        T.StructField("variante", T.StringType(), False),
        T.StructField("repeticion", T.IntegerType(), False),
        T.StructField("segundos", T.DoubleType(), False),
        T.StructField("filas_resultado", T.IntegerType(), False),
        T.StructField("total_registros", T.LongType(), False),
    ])
    res = spark.createDataFrame(mediciones, schema=esquema)
    res.write.mode("overwrite").parquet(f"{destino}/mediciones_parquet")
    res.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{destino}/mediciones_csv")
    print(f"B43_MEDICIONES_GUARDADAS={destino}", flush=True)
    print(f"B43_PRUEBAS_OK={len(by_key)} variantes", flush=True)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--pruebas", nargs="+", choices=[*PRUEBAS, "todas"], default=["particiones", "join"])
    p.add_argument("--repeticiones", type=int, default=2)
    args = p.parse_args()
    if not 1 <= args.repeticiones <= 10:
        p.error("--repeticiones debe estar entre 1 y 10")
    elegidas = list(PRUEBAS) if "todas" in args.pruebas else list(dict.fromkeys(args.pruebas))
    etiqueta = "_".join(elegidas)
    ruta = f"{DESTINO}/ejecucion_{etiqueta}"
    spark = (SparkSession.builder.appName("SmartCity-B4.3-Optimizacion")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    try:
        df = spark.read.parquet(ENTRADA)
        faltan = {"anio", "mes", "zona", "timestamp", "tipo_metrica", "origen", "valor", "id_sensor"} - set(df.columns)
        if faltan:
            raise RuntimeError(f"Faltan columnas en ETL: {sorted(faltan)}")
        mediciones = []
        print(f"B43_ENTRADA={ENTRADA}", flush=True)
        print(f"B43_CORES={spark.sparkContext.defaultParallelism}", flush=True)
        print(f"B43_PRUEBAS={','.join(elegidas)}", flush=True)
        print(f"B43_REPETICIONES={args.repeticiones}", flush=True)
        if "particiones" in elegidas:
            prueba_particiones(spark, df, args.repeticiones, mediciones)
        if "join" in elegidas:
            prueba_join(spark, df, args.repeticiones, mediciones)
        if "aqe" in elegidas:
            prueba_aqe(spark, df, args.repeticiones, mediciones)
        if "cache" in elegidas:
            prueba_cache(spark, df, args.repeticiones, mediciones)
        if "csv" in elegidas:
            prueba_csv(spark, df, args.repeticiones, mediciones, ruta)
        resumen_y_guardar(spark, mediciones, ruta)
        print("B43_RESUMEN=OK", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
