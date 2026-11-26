#!/usr/bin/env python3
"""SmartCity B4.2: analisis descriptivo, temporal, correlacional, predictivo, espacial, proxy de movilidad, anomalias y comparacion entre zonas.

Uso dentro de smartcity_spark_master:
    spark-submit --master spark://spark-master:7077 /opt/smartcity/spark/jobs/analitica.py

Solo lee el Parquet limpio de B3.2. Escribe resultados Parquet en HDFS sandbox,
no modifica datos MongoDB/PostgreSQL ni sobrescribe el ETL original.
"""
import argparse
import json
import os
import math
from itertools import combinations_with_replacement

from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator

ENTRADA = "hdfs://namenode:8020/smartcity/sandbox/etl_ambiente_completa"
SALIDA = "hdfs://namenode:8020/smartcity/sandbox/analitica"


def analizar_descriptivo(df):
    """Distribución, cobertura y estadísticos por zona/variable/origen."""
    resumen = (
        df.groupBy("zona", "tipo_metrica", "origen")
        .agg(
            F.count("*").alias("mediciones"),
            F.countDistinct("id_sensor").alias("sensores_activos"),
            F.round(F.avg("valor"), 3).alias("promedio"),
            F.round(F.stddev_samp("valor"), 3).alias("desviacion_estandar"),
            F.round(F.min("valor"), 3).alias("minimo"),
            F.round(F.max("valor"), 3).alias("maximo"),
        )
    )
    ruta = f"{SALIDA}/descriptivo"
    resumen.write.mode("overwrite").parquet(ruta)
    verificado = df.sparkSession.read.parquet(ruta)
    print(f"SMARTCITY_B42_DESCRIPTIVO_FILAS={verificado.count()}", flush=True)
    print(f"SMARTCITY_B42_DESCRIPTIVO_RUTA={ruta}", flush=True)
    verificado.orderBy(F.desc("mediciones")).show(10, truncate=False)


def analizar_temporal(df):
    """Evolución mensual PM2.5 y perfil de temperatura por hora del día."""
    pm25 = (
        df.filter(F.col("tipo_metrica") == "pm25")
        .groupBy("zona", "origen", F.date_format("timestamp", "yyyy-MM").alias("periodo"))
        .agg(
            F.count("*").alias("mediciones"),
            F.round(F.avg("valor"), 3).alias("pm25_promedio"),
        )
    )
    ruta_pm25 = f"{SALIDA}/temporal_pm25_mensual"
    pm25.write.mode("overwrite").parquet(ruta_pm25)
    salida_pm25 = df.sparkSession.read.parquet(ruta_pm25)
    print(f"SMARTCITY_B42_TEMPORAL_PM25_FILAS={salida_pm25.count()}", flush=True)
    print(f"SMARTCITY_B42_TEMPORAL_PM25_RUTA={ruta_pm25}", flush=True)
    salida_pm25.orderBy("periodo", "zona", "origen").show(10, truncate=False)

    temperatura = (
        df.filter(F.col("tipo_metrica") == "temperatura")
        .groupBy("zona", "origen", F.hour("timestamp").alias("hora_dia"))
        .agg(
            F.count("*").alias("mediciones"),
            F.round(F.avg("valor"), 3).alias("temperatura_promedio"),
        )
    )
    ruta_temp = f"{SALIDA}/temporal_temperatura_hora"
    temperatura.write.mode("overwrite").parquet(ruta_temp)
    salida_temp = df.sparkSession.read.parquet(ruta_temp)
    print(f"SMARTCITY_B42_TEMPORAL_TEMPERATURA_FILAS={salida_temp.count()}", flush=True)
    print(f"SMARTCITY_B42_TEMPORAL_TEMPERATURA_RUTA={ruta_temp}", flush=True)
    salida_temp.orderBy("zona", "origen", "hora_dia").show(10, truncate=False)



def analizar_correlacional(df):
    """Matriz triangular de Pearson por zona y grupo de procedencia.

    Las observaciones se emparejan por hora y zona, no por posicion en los
    archivos. Datos de OpenAQ (aire) y Open-Meteo (clima) forman un grupo;
    datos sinteticos forman otro. No se mezclan observaciones entre grupos.
    Solo se cuentan horas con ambas variables disponibles para cada par.
    """
    variables = [
        "pm25", "pm10", "temperatura", "humedad", "precipitacion", "viento"
    ]
    variables_clima = ["temperatura", "humedad", "precipitacion", "viento"]

    externa_aire = (
        (F.col("origen") == "openaq")
        & F.col("tipo_metrica").isin("pm25", "pm10")
    )
    externa_clima = (
        (F.col("origen") == "open-meteo")
        & F.col("tipo_metrica").isin(*variables_clima)
    )
    sintetica = (
        (F.col("origen") == "sintetico")
        & F.col("tipo_metrica").isin(*variables)
    )

    horas = (
        df.filter(externa_aire | externa_clima | sintetica)
        .select("zona", "timestamp", "tipo_metrica", "origen", "valor")
        .withColumn("hora", F.date_trunc("hour", "timestamp"))
        .withColumn(
            "grupo_fuente",
            F.when(F.col("origen") == "sintetico", "sintetico")
            .otherwise("fuentes_externas")
        )
        .groupBy("zona", "hora", "grupo_fuente", "tipo_metrica")
        .agg(F.avg("valor").alias("promedio_horario"))
    )

    # Una fila por zona/hora/grupo y una columna por indicador.
    ancho = (
        horas.groupBy("zona", "hora", "grupo_fuente")
        .pivot("tipo_metrica", variables)
        .agg(F.first("promedio_horario"))
    )

    # Corr() ignora pares incompletos; contamos los pares efectivamente usados.
    pares = list(combinations_with_replacement(variables, 2))
    expresiones = []
    for indice, (a, b) in enumerate(pares):
        ambos = F.col(a).isNotNull() & F.col(b).isNotNull()
        expresiones.append(
            F.sum(F.when(ambos, 1).otherwise(0)).cast("long")
            .alias(f"n_{indice}")
        )
        expresiones.append(F.corr(F.col(a), F.col(b)).alias(f"r_{indice}"))

    agregados = ancho.groupBy("zona", "grupo_fuente").agg(*expresiones)

    # Pasar la matriz triangular a formato largo, sin duplicar pares A-B / B-A.
    filas = []
    for indice, (a, b) in enumerate(pares):
        r = F.col(f"r_{indice}")
        n = F.col(f"n_{indice}")
        filas.append(F.struct(
            F.lit(a).alias("variable_a"),
            F.lit(b).alias("variable_b"),
            n.alias("pares_validos"),
            F.when((n >= 2) & (~F.isnan(r)), F.round(r, 4))
            .otherwise(F.lit(None).cast("double"))
            .alias("correlacion_pearson")
        ))

    resultado = (
        agregados.select("zona", "grupo_fuente", F.explode(F.array(*filas)).alias("p"))
        .select("zona", "grupo_fuente", "p.*")
    )

    ruta = f"{SALIDA}/correlacional_matriz"
    resultado.write.mode("overwrite").parquet(ruta)
    verificado = df.sparkSession.read.parquet(ruta)
    print(f"SMARTCITY_B42_CORRELACIONAL_FILAS={verificado.count()}", flush=True)
    print(f"SMARTCITY_B42_CORRELACIONAL_RUTA={ruta}", flush=True)
    print("=== CORRELACIONES DE PM2.5 POR ZONA Y FUENTE ===", flush=True)
    verificado.filter(F.col("variable_a") == "pm25").orderBy(
        "zona", "grupo_fuente", "variable_b"
    ).show(30, truncate=False)



def analizar_predictivo(df):
    """Regresion de PM2.5 diario segun clima, sin fuga temporal.

    Se ajusta un modelo por zona/grupo de fuente. Solo 2023 entrena,
    y 2024 queda como conjunto de prueba cronologicamente posterior.
    Usamos el clima OBSERVADO del dia en 2024; no es pronostico futuro.
    """
    aire_externo = (
        (F.col("origen") == "openaq") & (F.col("tipo_metrica") == "pm25")
    )
    clima_externo = (
        (F.col("origen") == "open-meteo")
        & F.col("tipo_metrica").isin("temperatura", "humedad", "viento", "precipitacion")
    )
    sint = (
        (F.col("origen") == "sintetico")
        & F.col("tipo_metrica").isin(
            "pm25", "temperatura", "humedad", "viento", "precipitacion"
        )
    )
    base = (
        df.filter(aire_externo | clima_externo | sint)
        .select("zona", "timestamp", "origen", "tipo_metrica", "valor")
        .withColumn("dia", F.to_date("timestamp"))
        .withColumn(
            "grupo_fuente",
            F.when(F.col("origen") == "sintetico", "sintetico")
            .otherwise("fuentes_externas"),
        )
    )

    # Cada ejemplo es un dia/zona/grupo; las caracteristicas no incluyen
    # el PM2.5 del propio dia, solo variables meteorologicas y estacionalidad.
    variables = ["pm25", "temperatura", "humedad", "viento", "precipitacion"]
    resumenes = [
        F.avg(F.when(F.col("tipo_metrica") == v, F.col("valor"))).alias(v)
        for v in variables
    ]
    resumenes.append(
        F.countDistinct(
            F.when(
                F.col("tipo_metrica") == "pm25",
                F.date_trunc("hour", F.col("timestamp")),
            )
        ).alias("horas_pm25")
    )
    diarios = (
        base.groupBy("zona", "grupo_fuente", "dia")
        .agg(*resumenes)
        # Exigir al menos seis horas con PM2.5 para evitar dias poco cubiertos.
        .filter(F.col("horas_pm25") >= 6)
        .dropna(subset=variables)
        .withColumn("anio", F.year("dia"))
        .filter(F.col("anio").isin(2023, 2024))
        .withColumn(
            "estacion_seno",
            F.sin(F.lit(2.0 * math.pi / 365.25) * F.dayofyear("dia")),
        )
        .withColumn(
            "estacion_coseno",
            F.cos(F.lit(2.0 * math.pi / 365.25) * F.dayofyear("dia")),
        )
        .cache()
    )

    total_dias = diarios.count()
    print(f"SMARTCITY_B42_PREDICTIVO_DIAS={total_dias}", flush=True)
    if total_dias == 0:
        diarios.unpersist()
        raise RuntimeError("No hay dias con PM2.5 y clima para entrenar")

    combinaciones = (
        diarios.groupBy("zona", "grupo_fuente", "anio")
        .count()
        .collect()
    )
    conteos = {
        (r["zona"], r["grupo_fuente"], r["anio"]): r["count"]
        for r in combinaciones
    }
    grupos = sorted(set((r["zona"], r["grupo_fuente"]) for r in combinaciones))
    columnas_entrada = [
        "temperatura", "humedad", "viento", "precipitacion",
        "estacion_seno", "estacion_coseno",
    ]
    assembler = VectorAssembler(inputCols=columnas_entrada, outputCol="features")
    evaluador = RegressionEvaluator(labelCol="pm25", predictionCol="prediction")

    filas_metricas = []
    salidas_predicciones = []
    for zona, grupo in grupos:
        n_train = conteos.get((zona, grupo, 2023), 0)
        n_test = conteos.get((zona, grupo, 2024), 0)
        if n_train < 40 or n_test < 30:
            print(
                f"SMARTCITY_B42_PREDICTIVO_OMITIDO={zona}/{grupo}: "
                f"entrenamiento={n_train}, prueba={n_test}", flush=True,
            )
            continue

        subconjunto = diarios.filter(
            (F.col("zona") == zona) & (F.col("grupo_fuente") == grupo)
        )
        entrenamiento = assembler.transform(
            subconjunto.filter(F.col("anio") == 2023)
        )
        prueba = assembler.transform(
            subconjunto.filter(F.col("anio") == 2024)
        )

        modelo = LinearRegression(
            labelCol="pm25", featuresCol="features",
            predictionCol="prediction", maxIter=50,
            regParam=0.1, elasticNetParam=0.0,
        ).fit(entrenamiento)
        # Concentraciones negativas no tienen sentido fisico.
        predicho = modelo.transform(prueba).withColumn(
            "prediction", F.greatest(F.col("prediction"), F.lit(0.0))
        ).cache()

        rmse = evaluador.setMetricName("rmse").evaluate(predicho)
        r2 = evaluador.setMetricName("r2").evaluate(predicho)
        mae = evaluador.setMetricName("mae").evaluate(predicho)

        # Linea base: predecir siempre la media del conjunto de ENTRENAMIENTO.
        media_train = entrenamiento.agg(F.avg("pm25").alias("media")).first()["media"]
        baseline_rmse = prueba.agg(
            F.sqrt(F.avg(F.pow(F.col("pm25") - F.lit(media_train), 2)))
            .alias("rmse_baseline")
        ).first()["rmse_baseline"]

        filas_metricas.append((
            zona, grupo, int(n_train), int(n_test),
            float(rmse), float(mae), float(r2), float(baseline_rmse),
            bool(rmse < baseline_rmse),
        ))
        salidas_predicciones.append(
            predicho.select(
                "zona", "grupo_fuente", "dia", "horas_pm25",
                F.round("pm25", 3).alias("pm25_observado"),
                F.round("prediction", 3).alias("pm25_estimado"),
                F.round("temperatura", 3).alias("temperatura"),
                F.round("humedad", 3).alias("humedad"),
                F.round("viento", 3).alias("viento"),
                F.round("precipitacion", 3).alias("precipitacion"),
            )
        )
        # Liberar el cache tras recoger los resultados de cada zona.
        # La salida predicha conserva su lineage, que Spark recalcula al escribir.
        predicho.unpersist()
        print(
            f"SMARTCITY_B42_PREDICTIVO_MODELO={zona}/{grupo} "
            f"train={n_train} test={n_test} rmse={rmse:.3f} "
            f"rmse_baseline={baseline_rmse:.3f} r2={r2:.3f}",
            flush=True,
        )

    if not filas_metricas:
        diarios.unpersist()
        raise RuntimeError("No hay grupos con suficientes dias de entrenamiento y prueba")

    esquema_metricas = T.StructType([
        T.StructField("zona", T.StringType(), False),
        T.StructField("grupo_fuente", T.StringType(), False),
        T.StructField("dias_entrenamiento_2023", T.IntegerType(), False),
        T.StructField("dias_prueba_2024", T.IntegerType(), False),
        T.StructField("rmse", T.DoubleType(), False),
        T.StructField("mae", T.DoubleType(), False),
        T.StructField("r2", T.DoubleType(), False),
        T.StructField("rmse_baseline", T.DoubleType(), False),
        T.StructField("supera_baseline", T.BooleanType(), False),
    ])
    metricas = df.sparkSession.createDataFrame(filas_metricas, esquema_metricas)
    ruta_metricas = f"{SALIDA}/predictivo_metricas"
    metricas.write.mode("overwrite").parquet(ruta_metricas)

    union = salidas_predicciones[0]
    for pred in salidas_predicciones[1:]:
        union = union.unionByName(pred)
    ruta_estimaciones = f"{SALIDA}/predictivo_estimaciones_2024"
    union.coalesce(2).write.mode("overwrite").parquet(ruta_estimaciones)

    metricas_guardadas = df.sparkSession.read.parquet(ruta_metricas)
    estimaciones_guardadas = df.sparkSession.read.parquet(ruta_estimaciones)
    print(f"SMARTCITY_B42_PREDICTIVO_MODELOS={metricas_guardadas.count()}", flush=True)
    print(f"SMARTCITY_B42_PREDICTIVO_METRICAS_RUTA={ruta_metricas}", flush=True)
    print(f"SMARTCITY_B42_PREDICTIVO_ESTIMACIONES_FILAS={estimaciones_guardadas.count()}", flush=True)
    print(f"SMARTCITY_B42_PREDICTIVO_ESTIMACIONES_RUTA={ruta_estimaciones}", flush=True)
    metricas_guardadas.orderBy("zona", "grupo_fuente").show(10, truncate=False)
    estimaciones_guardadas.orderBy("zona", "grupo_fuente", "dia").show(10, truncate=False)
    diarios.unpersist()



def analizar_espacial(df):
    """Distribucion geografica de PM2.5 por zona y sensor.

    Cruza lecturas del ETL con catalogo maestro de sensores que incluye
    las coordenadas. Las cinco zonas usan sus centroides Open-Meteo.
    Se separan estrictamente las muestras OpenAQ y sinteticas.
    No se interpolan valores ni se infiere contaminacion de zonas sin PM2.5 real.
    """
    spark = df.sparkSession
    catalogo = (
        spark.read.option("header", True).csv("/opt/smartcity/data/maestros/sensores.csv")
        .select(
            "id_sensor", F.col("zona").alias("zona_catalogo"), "origen",
            F.col("lat").cast("double").alias("latitud"),
            F.col("lon").cast("double").alias("longitud"),
        )
        .dropDuplicates(["id_sensor"])
    )

    # Centroide de cada zona segun el sensor Open-Meteo de esa zona.
    centroides = (
        catalogo.filter(F.col("origen") == "open-meteo")
        .select(F.col("zona_catalogo").alias("zona"), "latitud", "longitud")
        .dropDuplicates(["zona"])
    )
    if centroides.count() != 5:
        raise RuntimeError("Se esperaban cinco centroides Open-Meteo en el catalogo")

    pm25 = df.filter(
        (F.col("tipo_metrica") == "pm25")
        & F.col("origen").isin("openaq", "sintetico")
        & F.col("valor").isNotNull()
    ).select("zona", "id_sensor", "origen", "valor")

    conteo_pm25 = pm25.count()
    print(f"SMARTCITY_B42_ESPACIAL_PM25_ENTRADA={conteo_pm25}", flush=True)

    # Distribucion por zona y fuente. Incluir filas con cobertura real inexistente.
    por_zona = pm25.groupBy("zona", "origen").agg(
        F.count("*").alias("mediciones"),
        F.countDistinct("id_sensor").alias("sensores_activos"),
        F.round(F.avg("valor"), 3).alias("pm25_promedio"),
        F.round(F.max("valor"), 3).alias("pm25_maximo"),
    )
    fuentes = spark.createDataFrame(
        [("openaq",), ("sintetico",)], ["origen"]
    )
    zonas = (
        centroides.crossJoin(F.broadcast(fuentes))
        .join(por_zona, ["zona", "origen"], "left")
        .fillna({"mediciones": 0, "sensores_activos": 0})
        .select(
            "zona", "origen", "latitud", "longitud", "mediciones",
            "sensores_activos", "pm25_promedio", "pm25_maximo"
        )
    )

    ruta_zonas = f"{SALIDA}/espacial_zonas_pm25"
    zonas.write.mode("overwrite").parquet(ruta_zonas)
    zonas_guardadas = spark.read.parquet(ruta_zonas)
    n_zonas = zonas_guardadas.count()
    print(f"SMARTCITY_B42_ESPACIAL_ZONAS_FILAS={n_zonas}", flush=True)
    print(f"SMARTCITY_B42_ESPACIAL_ZONAS_RUTA={ruta_zonas}", flush=True)
    zonas_guardadas.orderBy("zona", "origen").show(10, truncate=False)

    # Hotspots por sensor: no mezclar sensores fisicos y sintéticos en el ranking.
    resumen_sensores = (
        pm25.groupBy("id_sensor", "zona", "origen")
        .agg(
            F.count("*").alias("mediciones"),
            F.round(F.avg("valor"), 3).alias("pm25_promedio"),
            F.round(F.max("valor"), 3).alias("pm25_maximo"),
        )
    )
    c = catalogo.select("id_sensor", "zona_catalogo", "latitud", "longitud")
    sensores = (
        resumen_sensores.join(F.broadcast(c), "id_sensor", "left")
        .withColumn(
            "zona_consistente",
            F.col("zona") == F.col("zona_catalogo")
        )
        .withColumn(
            "ranking_en_zona",
            F.dense_rank().over(
                Window.partitionBy("zona", "origen")
                .orderBy(F.desc("pm25_promedio"))
            ),
        )
        .select(
            "zona", "origen", "id_sensor", "latitud", "longitud",
            "mediciones", "pm25_promedio", "pm25_maximo",
            "ranking_en_zona", "zona_consistente"
        )
    )
    ruta_sensores = f"{SALIDA}/espacial_sensores_pm25"
    sensores.write.mode("overwrite").parquet(ruta_sensores)
    sensores_guardados = spark.read.parquet(ruta_sensores)
    n_sensores = sensores_guardados.count()
    print(f"SMARTCITY_B42_ESPACIAL_SENSORES_FILAS={n_sensores}", flush=True)
    print(f"SMARTCITY_B42_ESPACIAL_SENSORES_RUTA={ruta_sensores}", flush=True)
    sensores_guardados.orderBy("zona", "origen", "ranking_en_zona").show(25, truncate=False)

    # Controles de integridad: las agregaciones no pueden perder mediciones.
    suma_zonas = zonas_guardadas.agg(F.sum("mediciones")).first()[0]
    suma_sensores = sensores_guardados.agg(F.sum("mediciones")).first()[0]
    incompletos = sensores_guardados.filter(
        F.col("latitud").isNull() | F.col("longitud").isNull()
        | F.col("zona_consistente").isNull() | (~F.col("zona_consistente"))
    ).count()
    print(f"SMARTCITY_B42_ESPACIAL_CONTROL_ZONAS={suma_zonas}", flush=True)
    print(f"SMARTCITY_B42_ESPACIAL_CONTROL_SENSORES={suma_sensores}", flush=True)
    print(f"SMARTCITY_B42_ESPACIAL_SENSORES_SIN_COORDENADAS_O_ZONA_INVALIDA={incompletos}", flush=True)
    if suma_zonas != conteo_pm25 or suma_sensores != conteo_pm25 or incompletos != 0:
        raise RuntimeError("La validacion del analisis espacial no coincide con la entrada")


def analizar_movilidad(df):
    """Proxy ambiental de horarios pico, NO medicion de trafico real.

    Se comparan los niveles de PM2.5 en horas habitualmente asociadas a
    desplazamientos frente a horas nocturnas de referencia, en dias laborables.
    No hay vehiculos, velocidades ni conteos de trafico en este DataFrame.

    Fuente: OpenAQ y sensores sinteticos, siempre separados. Los timestamps
    del ETL se interpretan como UTC y convierten a hora local Europe/London.
    """
    spark = df.sparkSession
    horas_pico = [7, 8, 9, 17, 18, 19]
    horas_valle = [2, 3, 4, 5]

    # Evitar sesgo por volumen desigual de sensores: primero un promedio
    # por zona, origen, fecha y hora; despues comparar dichos promedios.
    por_hora = (
        df.filter(
            (F.col("tipo_metrica") == "pm25")
            & F.col("origen").isin("openaq", "sintetico")
            & F.col("valor").isNotNull()
        )
        .select("zona", "origen", "timestamp", "valor")
        .withColumn("fecha_hora_local", F.from_utc_timestamp("timestamp", "Europe/London"))
        .withColumn("fecha_local", F.to_date("fecha_hora_local"))
        .withColumn("hora_local", F.hour("fecha_hora_local"))
        .withColumn(
            "tipo_dia",
            F.when(F.dayofweek("fecha_hora_local").between(2, 6), "laborable")
            .otherwise("fin_de_semana")
        )
        .groupBy("zona", "origen", "fecha_local", "hora_local", "tipo_dia")
        .agg(
            F.avg("valor").alias("pm25_hora"),
            F.count("*").alias("lecturas_en_bloque")
        )
    )

    # Perfil horario, para ver los patrones observados y decidir si hay
    # (o no) un incremento de contaminacion a las horas predefinidas.
    perfil = (
        por_hora.groupBy("zona", "origen", "tipo_dia", "hora_local")
        .agg(
            F.count("*").alias("bloques_horarios"),
            F.countDistinct("fecha_local").alias("dias_cubiertos"),
            F.round(F.avg("pm25_hora"), 3).alias("pm25_promedio"),
        )
    )
    ruta_perfil = f"{SALIDA}/movilidad_proxy_perfil_horario"
    perfil.write.mode("overwrite").parquet(ruta_perfil)
    guardado_perfil = spark.read.parquet(ruta_perfil)
    print(f"SMARTCITY_B42_MOVILIDAD_PERFIL_FILAS={guardado_perfil.count()}", flush=True)
    print(f"SMARTCITY_B42_MOVILIDAD_PERFIL_RUTA={ruta_perfil}", flush=True)
    guardado_perfil.orderBy("zona", "origen", "tipo_dia", "hora_local").show(12, truncate=False)

    # Las horas pico se definen a priori, no se obtienen de datos de trafico.
    solo_laborables = por_hora.filter(F.col("tipo_dia") == "laborable")
    es_pico = F.col("hora_local").isin(*horas_pico)
    es_valle = F.col("hora_local").isin(*horas_valle)
    comparacion = (
        solo_laborables.groupBy("zona", "origen")
        .agg(
            F.count(F.when(es_pico, 1)).alias("bloques_pico"),
            F.count(F.when(es_valle, 1)).alias("bloques_valle"),
            F.avg(F.when(es_pico, F.col("pm25_hora"))).alias("media_pico"),
            F.avg(F.when(es_valle, F.col("pm25_hora"))).alias("media_valle"),
        )
        .withColumn("pm25_pico", F.round("media_pico", 3))
        .withColumn("pm25_valle", F.round("media_valle", 3))
        .withColumn("diferencia_pm25", F.round(F.col("media_pico") - F.col("media_valle"), 3))
        .withColumn(
            "variacion_pct",
            F.when(F.col("media_valle") > 0,
                   F.round(100 * (F.col("media_pico") - F.col("media_valle")) / F.col("media_valle"), 2))
        )
        .withColumn("horas_pico_definidas", F.lit("07-09 y 17-19"))
        .withColumn("horas_valle_definidas", F.lit("02-05"))
        .withColumn("naturaleza", F.lit("proxy_ambiental_no_trafico"))
        .select(
            "zona", "origen", "bloques_pico", "bloques_valle", "pm25_pico",
            "pm25_valle", "diferencia_pm25", "variacion_pct",
            "horas_pico_definidas", "horas_valle_definidas", "naturaleza"
        )
    )
    ruta_comparacion = f"{SALIDA}/movilidad_proxy_pico_valle"
    comparacion.write.mode("overwrite").parquet(ruta_comparacion)
    guardado_comparacion = spark.read.parquet(ruta_comparacion)
    filas_comparacion = guardado_comparacion.count()
    print(f"SMARTCITY_B42_MOVILIDAD_PICO_VALLE_FILAS={filas_comparacion}", flush=True)
    print(f"SMARTCITY_B42_MOVILIDAD_PICO_VALLE_RUTA={ruta_comparacion}", flush=True)
    guardado_comparacion.orderBy("zona", "origen").show(10, truncate=False)

    # Top 3 SOLO sintetico, porque es el unico origen con cobertura PM2.5
    # en las cinco zonas. Resultado destinado a un grafico comparativo.
    top3 = (
        guardado_comparacion.filter(
            (F.col("origen") == "sintetico")
            & (F.col("bloques_pico") > 0)
            & (F.col("bloques_valle") > 0)
        )
        .orderBy(F.desc("diferencia_pm25"), "zona")
        .limit(3)
    )
    ruta_top3 = f"{SALIDA}/movilidad_proxy_top3_pico_valle"
    top3.write.mode("overwrite").parquet(ruta_top3)
    guardado_top3 = spark.read.parquet(ruta_top3)
    print(f"SMARTCITY_B42_MOVILIDAD_TOP3_FILAS={guardado_top3.count()}", flush=True)
    print(f"SMARTCITY_B42_MOVILIDAD_TOP3_RUTA={ruta_top3}", flush=True)
    print("=== TOP 3 DIFERENCIA PM2.5 PICO VS VALLE (SOLO SINTETICO) ===", flush=True)
    guardado_top3.orderBy(F.desc("diferencia_pm25"), "zona").show(3, truncate=False)
    print("SMARTCITY_B42_MOVILIDAD_AVISO=Esto no identifica horas de mayor trafico; solo compara PM2.5 por franjas horarias", flush=True)



def analizar_anomalias(df):
    """B4.2: sensores anormales, con DataFrames de PySpark y datos crudos.

    A) En MongoDB se leen SOLO los documentos marcados como no validos;
       el generador marca principalmente PM2.5/temperatura/humedad, no
       todas las variables del documento. Se escoge la metrica principal.
    B) En el Parquet curado se aplica |Z| >= 3 por sensor/indicador, tomando
       estadisticas sobre las lecturas validas (minimo 30 valores).

    Ambas poblaciones son disjuntas por diseño del ETL: los documentos
    invalidos fueron excluidos del Parquet curado. Por ello no se duplican
    entre si. La regla de Z-score es exploratoria, no un diagnostico causal.
    """
    uri = os.getenv("MONGO_URI")
    if not uri:
        raise RuntimeError(
            "El analisis 'anomalias' necesita MONGO_URI en el contenedor "
            "para recuperar las lecturas invalidadas en MongoDB"
        )
    spark = df.sparkSession
    variables = [
        "pm25", "pm10", "co", "no2", "o3", "co2",
        "temperatura", "humedad", "precipitacion", "viento",
    ]

    # Estadisticas sobre la parte ya limpiada por B3.2; los invalidos
    # NO intervienen en el calculo de media y desviacion de referencia.
    validas = df.select(
        "id_sensor", "zona", "timestamp", "tipo_metrica", "valor", "origen"
    ).filter(
        F.col("valor").isNotNull()
        & (~F.isnan("valor"))
        & F.col("id_sensor").isNotNull()
        & F.col("tipo_metrica").isNotNull()
    )
    estadisticas = validas.groupBy("id_sensor", "tipo_metrica").agg(
        F.count("*").alias("n_referencia"),
        F.avg("valor").alias("media_referencia"),
        F.stddev_samp("valor").alias("desviacion_referencia"),
    )

    def z_score_columna():
        return F.when(
            (F.col("n_referencia") >= 30)
            & F.col("desviacion_referencia").isNotNull()
            & (F.col("desviacion_referencia") > 0),
            (F.col("valor") - F.col("media_referencia"))
            / F.col("desviacion_referencia"),
        )

    z_scores = (
        validas.join(F.broadcast(estadisticas), ["id_sensor", "tipo_metrica"], "inner")
        .withColumn("z_score", z_score_columna())
        .filter(F.abs("z_score") >= F.lit(3.0))
        .select(
            "id_sensor", "zona", "timestamp", "tipo_metrica",
            F.round("valor", 3).alias("valor"),
            F.lit("z_score").alias("metodo_deteccion"),
            F.lit("abs(z)>=3 (referencia: lecturas limpias)").alias("detalle"),
            F.round("z_score", 3).alias("z_score"), "origen",
        )
    )

    # Lectura temprana con $match para NO transferir el millon de
    # documentos de MongoDB. Un unico particionador evita collStats.
    esquema = T.StructType([
        T.StructField("sensor_id", T.StringType()),
        T.StructField("tipo", T.StringType()),
        T.StructField("timestamp", T.TimestampType()),
        T.StructField("ubicacion", T.StructType([
            T.StructField("zona", T.StringType()),
        ])),
        T.StructField("mediciones", T.StructType([
            T.StructField(v, T.DoubleType()) for v in variables
        ])),
        T.StructField("origen", T.StringType()),
        T.StructField("calidad_dato", T.StructType([
            T.StructField("valido", T.BooleanType()),
            T.StructField("flags", T.ArrayType(T.StringType())),
        ])),
    ])
    documentos_marcados = (
        spark.read.format("mongodb")
        .option("connection.uri", uri)
        .option("database", "smartcity_db")
        .option("collection", "lectura")
        .option(
            "partitioner",
            "com.mongodb.spark.sql.connector.read.partitioner.SinglePartitionPartitioner",
        )
        .option("aggregation.pipeline", json.dumps([
            {"$match": {"calidad_dato.valido": False}}
        ]))
        .schema(esquema)
        .load()
    )

    # Segun generar_sinteticos.py, los flags se inyectan en:
    # - calidad_aire: pm25 (pico, negativo, pegado)
    # - clima: temperatura (pico, pegado), humedad (fuera_de_rango)
    # Para un negativo de una fuente externa, priorizamos la variable
    # efectivamente negativa, sin inventar una medicion de PM2.5.
    aire = ["pm25", "pm10", "co", "no2", "o3", "co2"]
    metricas_negativas = [
        F.when(F.col(f"mediciones.{v}") < 0, F.lit(v)) for v in aire
    ]
    metricas_presentes = [
        F.when(F.col(f"mediciones.{v}").isNotNull(), F.lit(v))
        for v in aire
    ]
    metrica_aire = F.coalesce(*(metricas_negativas + metricas_presentes))
    metrica_clima = F.when(
        F.array_contains(F.col("calidad_dato.flags"), "fuera_de_rango"),
        F.lit("humedad"),
    ).otherwise(F.lit("temperatura"))
    metrica_principal = F.when(
        F.col("tipo") == "clima", metrica_clima
    ).otherwise(metrica_aire)

    valor_principal = F.coalesce(*[
        F.when(F.col("tipo_metrica") == v, F.col(f"mediciones.{v}"))
        for v in variables
    ])
    marcadas_base = (
        documentos_marcados
        .withColumn("id_sensor", F.col("sensor_id"))
        .withColumn("zona", F.col("ubicacion.zona"))
        .withColumn("tipo_metrica", metrica_principal)
        .withColumn("valor", valor_principal)
        .withColumn("detalle", F.concat_ws(",", F.col("calidad_dato.flags")))
        .select("id_sensor", "zona", "timestamp", "tipo_metrica", "valor", "detalle", "origen")
    )
    # Solo deben omitirse documentos sin identificador/fecha/valor. Contamos
    # explicitamente estas omisiones para no sobreafirmar la cobertura.
    documento_sin_valor = (
        F.col("id_sensor").isNull()
        | F.col("timestamp").isNull()
        | F.col("tipo_metrica").isNull()
        | F.col("valor").isNull()
        | F.isnan("valor")
    )
    marcadas_validas = marcadas_base.filter(~documento_sin_valor)
    marcadas = (
        marcadas_validas
        .join(F.broadcast(estadisticas), ["id_sensor", "tipo_metrica"], "left")
        .withColumn("z_score", z_score_columna())
        .select(
            "id_sensor", "zona", "timestamp", "tipo_metrica",
            F.round("valor", 3).alias("valor"),
            F.lit("marca_calidad").alias("metodo_deteccion"),
            F.when(F.length("detalle") > 0, F.col("detalle"))
            .otherwise(F.lit("sin_detalle")).alias("detalle"),
            F.round("z_score", 3).alias("z_score"), "origen",
        )
    )

    # Son conjuntos disjuntos: marcadas=Mongo invalidas, z_scores=ETL limpio.
    # Deduplicamos por sensor, instante, metrica y metodo como salvaguarda.
    detalle = (
        marcadas.unionByName(z_scores)
        .dropDuplicates(["id_sensor", "timestamp", "tipo_metrica", "metodo_deteccion"])
    )
    ruta_detalle = f"{SALIDA}/anomalias_detalle"
    detalle.write.mode("overwrite").parquet(ruta_detalle)
    detalle_guardado = spark.read.parquet(ruta_detalle)
    conteo_total = detalle_guardado.count()
    resumen_metodo = detalle_guardado.groupBy("metodo_deteccion").count()
    conteos = {r["metodo_deteccion"]: int(r["count"]) for r in resumen_metodo.collect()}
    n_marcadas = conteos.get("marca_calidad", 0)
    n_z = conteos.get("z_score", 0)
    print(f"SMARTCITY_B42_ANOMALIAS_MARCADAS={n_marcadas}", flush=True)
    print(f"SMARTCITY_B42_ANOMALIAS_ZSCORE={n_z}", flush=True)
    print(f"SMARTCITY_B42_ANOMALIAS_TOTAL={conteo_total}", flush=True)
    print(f"SMARTCITY_B42_ANOMALIAS_DETALLE_RUTA={ruta_detalle}", flush=True)
    if conteo_total != n_marcadas + n_z:
        raise RuntimeError("El conteo por metodos no coincide con el total de anomalías")
    if n_marcadas == 0 or n_z == 0:
        print("SMARTCITY_B42_ANOMALIAS_AVISO=Alguno de los metodos no produjo casos; revisar datos y umbral", flush=True)
    print("=== MUESTRA DE ANOMALIAS MARCADAS ===", flush=True)
    detalle_guardado.filter(F.col("metodo_deteccion") == "marca_calidad")\
        .orderBy(F.desc("timestamp")).show(10, truncate=False)
    print("=== MUESTRA DE ANOMALIAS POR Z-SCORE ===", flush=True)
    detalle_guardado.filter(F.col("metodo_deteccion") == "z_score")\
        .orderBy(F.desc(F.abs("z_score"))).show(10, truncate=False)

    # Resumen para informes y graficos de calidad del dato.
    resumen = (
        detalle_guardado.groupBy(
            "zona", "tipo_metrica", "origen", "metodo_deteccion", "detalle"
        )
        .agg(
            F.count("*").alias("casos"),
            F.countDistinct("id_sensor").alias("sensores_afectados"),
            F.round(F.max(F.abs("z_score")), 3).alias("zscore_absoluto_maximo"),
        )
    )
    ruta_resumen = f"{SALIDA}/anomalias_resumen"
    resumen.write.mode("overwrite").parquet(ruta_resumen)
    resumen_guardado = spark.read.parquet(ruta_resumen)
    print(f"SMARTCITY_B42_ANOMALIAS_RESUMEN_FILAS={resumen_guardado.count()}", flush=True)
    print(f"SMARTCITY_B42_ANOMALIAS_RESUMEN_RUTA={ruta_resumen}", flush=True)
    resumen_guardado.orderBy(F.desc("casos")).show(15, truncate=False)

    # Ranking de sensores con mas casos, sin sumar lecturas de distinto
    # origen ni confundir eventos de calidad con medidas de trafico.
    sensores = (
        detalle_guardado.groupBy("zona", "id_sensor", "origen")
        .agg(
            F.count("*").alias("total_anomalias"),
            F.sum(F.when(F.col("metodo_deteccion") == "marca_calidad", 1)
                  .otherwise(0)).alias("marcadas"),
            F.sum(F.when(F.col("metodo_deteccion") == "z_score", 1)
                  .otherwise(0)).alias("estadisticas"),
            F.round(F.max(F.abs("z_score")), 3).alias("zscore_absoluto_maximo"),
        )
    )
    ruta_sensores = f"{SALIDA}/anomalias_sensores"
    sensores.write.mode("overwrite").parquet(ruta_sensores)
    sensores_guardados = spark.read.parquet(ruta_sensores)
    print(f"SMARTCITY_B42_ANOMALIAS_SENSORES_FILAS={sensores_guardados.count()}", flush=True)
    print(f"SMARTCITY_B42_ANOMALIAS_SENSORES_RUTA={ruta_sensores}", flush=True)
    print("=== SENSORES CON MAS ANOMALIAS ===", flush=True)
    sensores_guardados.orderBy(F.desc("total_anomalias"), "id_sensor")\
        .show(15, truncate=False)

    # La consulta Mongo tiene un $match; una discrepancia debera
    # investigarse, no ignorarse como una perdida de documentos.
    n_mongo = documentos_marcados.count()
    n_sin_valor = marcadas_base.filter(documento_sin_valor).count()
    print(f"SMARTCITY_B42_ANOMALIAS_DOCUMENTOS_MONGO_MARCADOS={n_mongo}", flush=True)
    print(f"SMARTCITY_B42_ANOMALIAS_DOCUMENTOS_SIN_VALOR={n_sin_valor}", flush=True)
    if n_sin_valor:
        print("SMARTCITY_B42_ANOMALIAS_AVISO=Algunos documentos marcados no tenian metrica principal utilizable", flush=True)
    if n_marcadas > n_mongo:
        raise RuntimeError("Se crearon mas marcas que documentos marcados en MongoDB")
    print("SMARTCITY_B42_ANOMALIAS_METODOLOGIA=marcadas=MongoDB invalido; z_score=Parquet curado; no representan necesariamente anomalías independientes de origen externo", flush=True)

def analizar_comparativo(df):
    """B4.2: compara el comportamiento anual de indicadores y zonas.

    Las comparaciones son *por variable y por origen*, nunca promediando
    magnitudes distintas ni mezclando OpenAQ con datos sinteticos.
    Se incluyen conteos y dias cubiertos para contextualizar los promedios.
    No constituye una comparacion causal ni una estimacion de trafico.
    """
    spark = df.sparkSession
    base = (
        df.filter(
            F.col("timestamp").isNotNull()
            & F.col("valor").isNotNull()
            & F.col("zona").isNotNull()
            & F.col("origen").isNotNull()
            & F.col("tipo_metrica").isNotNull()
        )
        .withColumn("anio_analisis", F.year("timestamp"))
        .filter(F.col("anio_analisis").isin(2023, 2024))
        .withColumn("dia_analisis", F.to_date("timestamp"))
    )

    # Comparacion de todas las variables en unidades compatibles; cada
    # combinacion fuente/indicador/anio tiene su propio ranking entre zonas.
    perfil = (
        base.groupBy("zona", "origen", "tipo_metrica", "anio_analisis")
        .agg(
            F.count("*").alias("mediciones"),
            F.countDistinct("dia_analisis").alias("dias_cubiertos"),
            F.countDistinct("id_sensor").alias("sensores_activos"),
            F.round(F.avg("valor"), 3).alias("promedio"),
            F.round(F.percentile_approx("valor", 0.5, 1000), 3).alias("mediana"),
            F.round(F.stddev_samp("valor"), 3).alias("desviacion_estandar"),
            F.round(F.min("valor"), 3).alias("minimo"),
            F.round(F.max("valor"), 3).alias("maximo"),
        )
        .withColumn(
            "ranking_promedio_descendente",
            F.dense_rank().over(
                Window.partitionBy("anio_analisis", "origen", "tipo_metrica")
                .orderBy(F.desc("promedio"))
            ),
        )
        .withColumnRenamed("anio_analisis", "anio")
    )
    ruta_perfil = f"{SALIDA}/comparativo_perfil_anual"
    perfil.write.mode("overwrite").parquet(ruta_perfil)
    perfil_guardado = spark.read.parquet(ruta_perfil)
    print(f"SMARTCITY_B42_COMPARATIVO_PERFIL_FILAS={perfil_guardado.count()}", flush=True)
    print(f"SMARTCITY_B42_COMPARATIVO_PERFIL_RUTA={ruta_perfil}", flush=True)
    print("=== PERFIL POR ZONA: PM2.5 EN 2024 (ORIGENES SEPARADOS) ===", flush=True)
    perfil_guardado.filter(
        (F.col("tipo_metrica") == "pm25") & (F.col("anio") == 2024)
    ).orderBy("origen", "ranking_promedio_descendente", "zona").show(10, truncate=False)

    # Tabla interanual PM2.5: incluso donde OpenAQ no tiene mediciones
    # se conserva una fila con cero muestras y promedios NULL.
    pm25_anual = (
        base.filter(
            (F.col("tipo_metrica") == "pm25")
            & F.col("origen").isin("openaq", "sintetico")
        )
        .groupBy("zona", "origen", "anio_analisis")
        .agg(
            F.count("*").alias("mediciones"),
            F.countDistinct("dia_analisis").alias("dias_cubiertos"),
            F.avg("valor").alias("promedio_sin_redondear"),
        )
    )
    resumen = (
        pm25_anual.groupBy("zona", "origen")
        .agg(
            F.max(F.when(F.col("anio_analisis") == 2023, F.col("mediciones")))
                .alias("mediciones_2023"),
            F.max(F.when(F.col("anio_analisis") == 2024, F.col("mediciones")))
                .alias("mediciones_2024"),
            F.max(F.when(F.col("anio_analisis") == 2023, F.col("dias_cubiertos")))
                .alias("dias_cubiertos_2023"),
            F.max(F.when(F.col("anio_analisis") == 2024, F.col("dias_cubiertos")))
                .alias("dias_cubiertos_2024"),
            F.max(F.when(F.col("anio_analisis") == 2023, F.col("promedio_sin_redondear")))
                .alias("promedio_2023_crudo"),
            F.max(F.when(F.col("anio_analisis") == 2024, F.col("promedio_sin_redondear")))
                .alias("promedio_2024_crudo"),
        )
    )
    zonas = base.select("zona").distinct()
    fuentes = spark.createDataFrame([("openaq",), ("sintetico",)], ["origen"])
    comparacion = (
        zonas.crossJoin(F.broadcast(fuentes))
        .join(resumen, ["zona", "origen"], "left")
        .fillna({
            "mediciones_2023": 0, "mediciones_2024": 0,
            "dias_cubiertos_2023": 0, "dias_cubiertos_2024": 0,
        })
        .withColumn("pm25_2023", F.round("promedio_2023_crudo", 3))
        .withColumn("pm25_2024", F.round("promedio_2024_crudo", 3))
        .withColumn(
            "diferencia_absoluta",
            F.round(F.col("promedio_2024_crudo") - F.col("promedio_2023_crudo"), 3),
        )
        .withColumn(
            "variacion_pct",
            F.when(
                F.col("promedio_2023_crudo") > 0,
                F.round(
                    100.0 * (F.col("promedio_2024_crudo") - F.col("promedio_2023_crudo"))
                    / F.col("promedio_2023_crudo"), 2,
                ),
            ),
        )
        .withColumn(
            "calidad_cobertura",
            F.when(
                (F.col("dias_cubiertos_2023") == 0)
                | (F.col("dias_cubiertos_2024") == 0),
                F.lit("sin_datos_en_ambos_anios"),
            )
            .when(
                (F.col("dias_cubiertos_2023") < 300)
                | (F.col("dias_cubiertos_2024") < 300),
                F.lit("cobertura_parcial_revisar"),
            )
            .otherwise(F.lit("cobertura_300_dias_o_mas")),
        )
        .select(
            "zona", "origen", "mediciones_2023", "dias_cubiertos_2023", "pm25_2023",
            "mediciones_2024", "dias_cubiertos_2024", "pm25_2024",
            "diferencia_absoluta", "variacion_pct", "calidad_cobertura",
        )
    )
    ruta_pm25 = f"{SALIDA}/comparativo_pm25_2023_2024"
    comparacion.write.mode("overwrite").parquet(ruta_pm25)
    pm25_guardado = spark.read.parquet(ruta_pm25)
    total = pm25_guardado.count()
    print(f"SMARTCITY_B42_COMPARATIVO_PM25_FILAS={total}", flush=True)
    print(f"SMARTCITY_B42_COMPARATIVO_PM25_RUTA={ruta_pm25}", flush=True)
    print("=== COMPARACION HISTORICA PM2.5 POR ZONA Y ORIGEN ===", flush=True)
    pm25_guardado.orderBy("zona", "origen").show(10, truncate=False)

    # Top 3 de aumento absoluto interanual SOLO del dataset sintetico.
    # Se diferencia del top 3 de horarios pico del analisis de movilidad.
    top3 = (
        pm25_guardado.filter(
            (F.col("origen") == "sintetico")
            & F.col("diferencia_absoluta").isNotNull()
            & (F.col("diferencia_absoluta") > 0)
        )
        .orderBy(F.desc("diferencia_absoluta"), "zona")
        .limit(3)
    )
    ruta_top3 = f"{SALIDA}/comparativo_top3_incremento_interanual"
    top3.write.mode("overwrite").parquet(ruta_top3)
    top3_guardado = spark.read.parquet(ruta_top3)
    print(f"SMARTCITY_B42_COMPARATIVO_TOP3_FILAS={top3_guardado.count()}", flush=True)
    print(f"SMARTCITY_B42_COMPARATIVO_TOP3_RUTA={ruta_top3}", flush=True)
    top3_guardado.orderBy(F.desc("diferencia_absoluta"), "zona").show(3, truncate=False)

    if total != 10:
        raise RuntimeError(f"Comparativo PM2.5 debe tener 5 zonas x 2 fuentes = 10 filas; obtuvo {total}")
    print(
        "SMARTCITY_B42_COMPARATIVO_NOTA=Correlacion y comparaciones no prueban causalidad; "
        "cobertura anual desigual y simulacion requieren advertencias en el informe",
        flush=True,
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--analisis", nargs="+", choices=["descriptivo", "temporal", "correlacional", "predictivo", "espacial", "movilidad", "anomalias", "comparativo"],
        default=["descriptivo", "temporal"],
        help="Selecciona los análisis (por defecto, descriptivo y temporal).",
    )
    args = parser.parse_args()
    seleccionados = list(dict.fromkeys(args.analisis))

    spark = (
        SparkSession.builder
        .appName("SmartCity-B4.2-Analitica")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        df = spark.read.parquet(ENTRADA)
        requeridas = {"zona", "timestamp", "id_sensor", "tipo_metrica", "valor", "origen"}
        faltantes = requeridas - set(df.columns)
        if faltantes:
            raise RuntimeError(f"Faltan columnas en Parquet: {sorted(faltantes)}")
        print(f"SMARTCITY_B42_ENTRADA={df.count()}", flush=True)
        if "descriptivo" in seleccionados:
            print("=== B4.2 DESCRIPTIVO ===", flush=True)
            analizar_descriptivo(df)
        if "temporal" in seleccionados:
            print("=== B4.2 TEMPORAL ===", flush=True)
            analizar_temporal(df)
        if "correlacional" in seleccionados:
            print("=== B4.2 CORRELACIONAL ===", flush=True)
            analizar_correlacional(df)
        if "predictivo" in seleccionados:
            print("=== B4.2 PREDICTIVO ===", flush=True)
            analizar_predictivo(df)
        if "espacial" in seleccionados:
            print("=== B4.2 ESPACIAL ===", flush=True)
            analizar_espacial(df)
        if "movilidad" in seleccionados:
            print("=== B4.2 MOVILIDAD (PROXY AMBIENTAL; NO TRAFICO REAL) ===", flush=True)
            analizar_movilidad(df)
        if "anomalias" in seleccionados:
            print("=== B4.2 ANOMALIAS: MARCAS DE CALIDAD + Z-SCORE ===", flush=True)
            analizar_anomalias(df)
        if "comparativo" in seleccionados:
            print("=== B4.2 COMPARATIVO ENTRE ZONAS (2023 VS 2024) ===", flush=True)
            analizar_comparativo(df)
        print(f"SMARTCITY_B42_RESUMEN={len(seleccionados)}/{len(seleccionados)}", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
