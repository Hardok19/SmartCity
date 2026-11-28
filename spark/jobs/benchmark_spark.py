"""C1 — Lado Spark de los experimentos de comparación tecnológica (E1, E2, E3, E5).

Lo lanza ``scripts/benchmark_comparacion.py`` con spark-submit dentro de smartcity_spark_master (D-04).
El dato lógico es el mismo en los tres motores: ``lectura_<n>`` (id_sensor, zona, tipo_metrica, timestamp, valor),
generado por el orquestador y subido como CSV a HDFS.

  --preparar  CSV de HDFS -> Parquet (esquema explícito). Mide la conversión y los bytes de cada formato.
  --medir     E1 agregación zona-hora, E2 búsqueda puntual (sin índice: recorre todo), E3 join con el catálogo
              de PostgreSQL (JDBC + broadcast). 1 calentamiento + N repeticiones; se informa la mediana.

Cada resultado sale en una línea ``SMARTCITY_BENCH {json}`` que el orquestador recoge.
"""
import argparse
import json
import os
import statistics
import time

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType

from common import HDFS, existe, get_spark

BASE = HDFS + "/smartcity/sandbox/bench"
PG_URL = os.environ.get("PG_JDBC_URL", "jdbc:postgresql://postgres:5432/smartcity_db")
PG_USER = os.environ.get("PG_USER", "smartcity")
PG_PASSWORD = os.environ.get("PG_PASSWORD", "smartcity123")

ESQUEMA = StructType([
    StructField("id_sensor", StringType(), False),
    StructField("zona", StringType(), False),
    StructField("tipo_metrica", StringType(), False),
    StructField("timestamp", TimestampType(), False),
    StructField("valor", DoubleType(), False),
])


def emitir(**datos):
    print("SMARTCITY_BENCH " + json.dumps(datos, ensure_ascii=False), flush=True)


def medir(funcion, repeticiones):
    funcion()                                   # calentamiento (JIT, caché de metadatos de HDFS)
    tiempos, resultado = [], None
    for _ in range(repeticiones):
        t0 = time.perf_counter()
        resultado = funcion()
        tiempos.append(time.perf_counter() - t0)
    return statistics.median(tiempos), tiempos, resultado


def bytes_hdfs(spark, ruta):
    jvm = spark.sparkContext._jvm
    p = jvm.org.apache.hadoop.fs.Path(ruta)
    return int(p.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration()).getContentSummary(p).getLength())


def preparar(spark, escalas):
    for n in escalas:
        csv, parquet = f"{BASE}/csv/lectura_{n}.csv", f"{BASE}/parquet/lectura_{n}"
        t0 = time.perf_counter()
        df = spark.read.csv(csv, header=True, schema=ESQUEMA, timestampFormat="yyyy-MM-dd HH:mm:ss")
        df.write.mode("overwrite").parquet(parquet)
        segundos = time.perf_counter() - t0
        filas = spark.read.parquet(parquet).count()
        emitir(experimento="E0", tecnologia="Spark", variante="csv_a_parquet", escala=n, segundos=segundos,
               detalle={"filas": filas, "bytes_csv": bytes_hdfs(spark, csv), "bytes_parquet": bytes_hdfs(spark, parquet)})


def catalogo(spark):
    def tabla(nombre):
        return (spark.read.format("jdbc").option("url", PG_URL).option("user", PG_USER)
                .option("password", PG_PASSWORD).option("driver", "org.postgresql.Driver")
                .option("dbtable", nombre).load())
    s = tabla("sensores").select("id_sensor", "tipo_sensor", "id_ubicacion", "id_organismo")
    t = tabla("tipos_sensor").select("tipo_sensor", "id_categoria")
    c = tabla("categorias_sensor").select("id_categoria", "nombre_categoria")
    u = tabla("ubicaciones").select("id_ubicacion", "distrito")
    o = tabla("organismos").select("id_organismo", F.col("nombre").alias("organismo"))
    # 50 sensores: se arma una sola vez y se envía completo a cada ejecutor (broadcast)
    cat = (s.join(t, "tipo_sensor").join(c, "id_categoria").join(u, "id_ubicacion")
           .join(o, "id_organismo", "left").select("id_sensor", "organismo", "nombre_categoria", "distrito"))
    return F.broadcast(cat.cache())


def medir_escala(spark, n, repeticiones, etiqueta, experimentos, cat):
    ruta = f"{BASE}/parquet/lectura_{n}"
    if not existe(spark, ruta):
        print(f"SMARTCITY_BENCH_AVISO falta {ruta}; ejecute primero --preparar", flush=True)
        return
    df = spark.read.parquet(ruta)
    variante = f"dataframe_{etiqueta}"

    if "E1" in experimentos:
        def e1():
            fila = (df.groupBy("zona", F.date_trunc("hour", "timestamp").alias("hora"))
                    .agg(F.avg("valor").alias("p"))
                    .agg(F.count("*").alias("grupos"), F.sum("p").alias("suma")).collect()[0])
            return {"grupos": fila["grupos"], "suma": round(fila["suma"], 4)}
        med, ts, res = medir(e1, repeticiones)
        emitir(experimento="E1", tecnologia="Spark", variante=variante, escala=n, segundos=med,
               detalle={**res, "tiempos": ts})
        if etiqueta in ("cores1", "cores2"):
            emitir(experimento="E5", tecnologia="Spark", variante=etiqueta, escala=n, segundos=med,
                   detalle={**res, "tiempos": ts, "consulta": "E1"})

    if "E2" in experimentos:
        def e2():
            filas = (df.filter(F.col("id_sensor") == "S-011").orderBy(F.col("timestamp").desc())
                     .limit(100).collect())
            return {"filas": len(filas), "ultima": str(filas[0]["timestamp"]) if filas else None}
        med, ts, res = medir(e2, repeticiones)
        emitir(experimento="E2", tecnologia="Spark", variante="full_scan_parquet", escala=n, segundos=med,
               detalle={**res, "tiempos": ts})

    if "E3" in experimentos:
        def e3():
            agregado = (df.join(cat, "id_sensor").groupBy("organismo", "nombre_categoria", "distrito")
                        .agg(F.count("*").alias("lecturas"), F.avg("valor").alias("prom")))
            fila = agregado.agg(F.count("*").alias("grupos"), F.sum("lecturas").alias("total")).collect()[0]
            return {"grupos": fila["grupos"], "total": int(fila["total"])}
        med, ts, res = medir(e3, repeticiones)
        emitir(experimento="E3", tecnologia="Spark", variante=f"join_broadcast_{etiqueta}", escala=n,
               segundos=med, detalle={**res, "tiempos": ts})


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--preparar", action="store_true")
    ap.add_argument("--medir", action="store_true")
    ap.add_argument("--escalas", nargs="+", type=int, default=[100_000, 1_000_000])
    ap.add_argument("--repeticiones", type=int, default=3)
    ap.add_argument("--etiqueta", default="cores2", help="nombre de la variante (p. ej. cores1/cores2 para E5)")
    ap.add_argument("--experimentos", nargs="+", default=["E1", "E2", "E3"])
    a = ap.parse_args()

    spark = get_spark("smartcity_benchmark", spark__executor__extraJavaOptions="-Duser.timezone=UTC")
    cores = spark.sparkContext.getConf().get("spark.cores.max", "?")
    print(f"SMARTCITY_BENCH_INFO nucleos_ejecutor={cores}", flush=True)
    if a.preparar:
        preparar(spark, a.escalas)
    if a.medir:
        cat = catalogo(spark) if "E3" in a.experimentos else None
        for n in a.escalas:
            medir_escala(spark, n, a.repeticiones, a.etiqueta, a.experimentos, cat)
    spark.stop()


if __name__ == "__main__":
    main()
