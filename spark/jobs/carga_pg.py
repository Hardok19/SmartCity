#!/usr/bin/env python3
"""B3.4: carga de resultados de Spark en PostgreSQL (tablas de A2.4, sql/ddl/003_analitica.sql).

Destinos (contrato de la pareja A):
  medicion_horaria       (id_sensor, timestamp, variable, valor, origen)  PK (id_sensor, timestamp, variable)
  agg_ambiente_hora_zona (zona, timestamp, pm25/temperatura/humedad_promedio, total_mediciones)  PK (zona, timestamp)
  spark_kpi_zona         (zona, fecha, metrica, valor)  PK (zona, fecha, metrica)  — formato largo
  spark_anomalia         (id_sensor, timestamp, variable, valor, tipo_anomalia)  PK (id_sensor, timestamp, variable)

Método (idempotente): Spark agrega -> tabla staging por JDBC (overwrite) -> INSERT … SELECT … ON CONFLICT
DO NOTHING (o DO UPDATE con --actualizar) -> DROP staging -> refresco de vistas materializadas.
spark_anomalia es la foto del último análisis de anomalías: se reemplaza completa (DELETE + INSERT en la misma
transacción), para que no queden lecturas que un análisis posterior ya no marca.
Solo se cargan filas cuyas FK existen (sensor, variable, zona). Antes de cargar medicion_horaria se borran
las lecturas de prueba (origen = 'prueba_pg') de scripts/pg_generar_mediciones.py. Tráfico y energía (B3.3,
sensores T-00x/E-00x) también se cargan en medicion_horaria con variable = trafico | energia.

Uso:  .\\scripts\\spark_submit.ps1 carga_pg [--tablas medicion_horaria agg spark_kpi_zona spark_anomalia] [--actualizar]
Requiere el driver JDBC (org.postgresql:postgresql) en --packages; spark_submit.ps1 ya lo incluye.
"""
import argparse
import os

from pyspark.sql import functions as F

from common import RUTAS, existe, get_spark

PG_URL = os.environ.get("PG_JDBC_URL", "jdbc:postgresql://postgres:5432/smartcity_db")
PG_USER = os.environ.get("PG_USER", "smartcity")
PG_PASSWORD = os.environ.get("PG_PASSWORD", "smartcity123")
TABLAS = ("medicion_horaria", "agg", "spark_kpi_zona", "spark_anomalia")
VARIABLES_KPI = ("pm25", "pm10", "no2", "o3", "co", "temperatura", "humedad", "precipitacion", "viento")

DESTINOS = {
    "medicion_horaria": {
        "tabla": "medicion_horaria", "pk": ("id_sensor", "timestamp", "variable"),
        "columnas": ("id_sensor", "timestamp", "variable", "valor", "origen"),
    },
    "agg": {
        "tabla": "agg_ambiente_hora_zona", "pk": ("zona", "timestamp"),
        "columnas": ("zona", "timestamp", "pm25_promedio", "temperatura_promedio", "humedad_promedio",
                     "total_mediciones"),
    },
    "spark_kpi_zona": {
        "tabla": "spark_kpi_zona", "pk": ("zona", "fecha", "metrica"),
        "columnas": ("zona", "fecha", "metrica", "valor"),
    },
    "spark_anomalia": {
        "tabla": "spark_anomalia", "pk": ("id_sensor", "timestamp", "variable"),
        "columnas": ("id_sensor", "timestamp", "variable", "valor", "tipo_anomalia"),
        "reemplazar": True,
    },
}


# ------------------------------------------------------------------ JDBC
def leer_pg(spark, consulta):
    return (spark.read.format("jdbc").option("url", PG_URL).option("user", PG_USER)
            .option("password", PG_PASSWORD).option("driver", "org.postgresql.Driver")
            .option("query", consulta).load())


def escribir_staging(df, tabla):
    (df.write.format("jdbc").option("url", PG_URL).option("user", PG_USER).option("password", PG_PASSWORD)
     .option("driver", "org.postgresql.Driver").option("dbtable", tabla).option("batchsize", 10000)
     .mode("overwrite").save())


def ejecutar_sql(spark, sentencias):
    """Ejecuta SQL en PostgreSQL desde el driver usando JDBC de la JVM (no requiere psycopg2)."""
    jvm = spark.sparkContext._jvm
    props = jvm.java.util.Properties()
    props.setProperty("user", PG_USER)
    props.setProperty("password", PG_PASSWORD)
    con = jvm.org.postgresql.Driver().connect(PG_URL, props)  # driver cargado por --packages
    try:
        con.setAutoCommit(False)
        resultados = []
        for sql in sentencias:
            st = con.createStatement()
            if sql.lstrip().upper().startswith("SELECT"):
                rs = st.executeQuery(sql)
                resultados.append(rs.getLong(1) if rs.next() else None)
            else:
                resultados.append(st.executeUpdate(sql))
            st.close()
        con.commit()
        return resultados
    except Exception:
        con.rollback()
        raise
    finally:
        con.close()


def cargar(spark, df, clave, actualizar):
    destino = DESTINOS[clave]
    tabla, pk, cols = destino["tabla"], destino["pk"], destino["columnas"]
    staging = f"stg_spark_{tabla}"
    df = df.select(*cols)
    n_spark = df.count()
    escribir_staging(df, staging)
    lista = ", ".join(cols)
    seleccion = ", ".join(f"(timestamp AT TIME ZONE 'UTC')" if c == "timestamp" else c for c in cols)
    if actualizar:
        resto = [c for c in cols if c not in pk]
        conflicto = f"DO UPDATE SET {', '.join(f'{c} = EXCLUDED.{c}' for c in resto)}"
    else:
        conflicto = "DO NOTHING"
    reemplazar = destino.get("reemplazar", False)
    resultados = ejecutar_sql(spark, ([f"DELETE FROM {tabla}"] if reemplazar else []) + [
        f"INSERT INTO {tabla} ({lista}) SELECT {seleccion} FROM {staging} ON CONFLICT ({', '.join(pk)}) {conflicto}",
        f"DROP TABLE IF EXISTS {staging}",
        f"SELECT COUNT(*) FROM {tabla}",
    ])
    borradas = resultados.pop(0) if reemplazar else None
    insertadas, _, total = resultados
    print(f"SMARTCITY_B34_{tabla.upper()} spark={n_spark} insertadas_o_actualizadas={insertadas} total_pg={total}"
          + (f" reemplazadas={borradas}" if reemplazar else ""), flush=True)
    return n_spark, insertadas, total


# ------------------------------------------------------------------ transformaciones
def medicion_horaria(amb, sensores, variables):
    base = (amb.withColumn("hora", F.date_trunc("hour", "timestamp"))
            .groupBy("id_sensor", "hora", "tipo_metrica")
            .agg(F.avg("valor").alias("valor"), F.first("origen", ignorenulls=True).alias("origen")))
    return (base.join(F.broadcast(sensores), "id_sensor", "inner")
            .join(F.broadcast(variables), base.tipo_metrica == variables.codigo, "inner")
            .select("id_sensor", F.col("hora").alias("timestamp"), F.col("tipo_metrica").alias("variable"),
                    "valor", F.coalesce("origen", F.lit("sintetico")).alias("origen")))


def agg_hora_zona(amb, zonas):
    prom = lambda v: F.avg(F.when(F.col("tipo_metrica") == v, F.col("valor")))  # noqa: E731
    return (amb.withColumn("timestamp", F.date_trunc("hour", "timestamp"))
            .groupBy("zona", "timestamp")
            .agg(prom("pm25").alias("pm25_promedio"), prom("temperatura").alias("temperatura_promedio"),
                 prom("humedad").alias("humedad_promedio"), F.count("*").alias("total_mediciones"))
            .join(F.broadcast(zonas), "zona", "inner"))


def kpi_zona(amb, zonas):
    diario = (amb.filter(F.col("tipo_metrica").isin(*VARIABLES_KPI))
              .withColumn("fecha", F.to_date("timestamp"))
              .groupBy("zona", "fecha", "tipo_metrica")
              .agg(F.avg("valor").alias("valor"), F.count("*").alias("n")))
    promedios = diario.select("zona", "fecha", F.concat(F.col("tipo_metrica"), F.lit("_promedio")).alias("metrica"),
                              "valor")
    conteos = (diario.groupBy("zona", "fecha").agg(F.sum("n").cast("double").alias("valor"))
               .withColumn("metrica", F.lit("total_mediciones")))
    return (promedios.unionByName(conteos.select("zona", "fecha", "metrica", "valor"))
            .join(F.broadcast(zonas), "zona", "inner"))


def anomalias(spark, sensores):
    detalle = spark.read.parquet(f"{RUTAS['analitica']}/anomalias_detalle")
    return (detalle.groupBy("id_sensor", "timestamp", "tipo_metrica")
            .agg(F.first("valor", ignorenulls=True).alias("valor"),
                 F.concat_ws(",", F.array_sort(F.collect_set("metodo_deteccion"))).alias("tipo_anomalia"))
            .filter(F.col("valor").isNotNull())
            .join(F.broadcast(sensores), "id_sensor", "inner")
            .select("id_sensor", "timestamp", F.col("tipo_metrica").alias("variable"), "valor", "tipo_anomalia"))


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--tablas", nargs="+", choices=TABLAS, default=list(TABLAS))
    ap.add_argument("--actualizar", action="store_true", help="ON CONFLICT DO UPDATE en vez de DO NOTHING")
    ap.add_argument("--entrada", default=RUTAS["ambiente"])
    ap.add_argument("--trafico-energia", default=RUTAS["trafico_energia"],
                    help="Parquet de B3.3 (vacío para omitir): tráfico y energía también van a medicion_horaria")
    args = ap.parse_args()

    spark = get_spark("SmartCity-B3.4-Carga-PostgreSQL",
                      spark__executor__extraJavaOptions="-Duser.timezone=UTC")
    try:
        sensores = leer_pg(spark, "SELECT id_sensor FROM sensores").cache()
        variables = leer_pg(spark, "SELECT codigo FROM variables_medicion").cache()
        zonas = leer_pg(spark, "SELECT nombre_zona AS zona FROM ubicaciones").cache()
        n_sensores, n_zonas = sensores.count(), zonas.count()
        print(f"SMARTCITY_B34_CATALOGO_PG sensores={n_sensores} variables={variables.count()} zonas={n_zonas}",
              flush=True)
        if n_sensores == 0 or n_zonas == 0:
            raise RuntimeError("PostgreSQL sin catálogo: ejecutar antes scripts/pg_init.py y scripts/pg_cargar_maestros.py")
        hay_ambiente = existe(spark, args.entrada)
        if hay_ambiente:
            amb = spark.read.parquet(args.entrada).select("zona", "timestamp", "id_sensor", "tipo_metrica", "valor",
                                                          "origen")
        else:
            print(f"SMARTCITY_B34_AVISO=No existe {args.entrada}: se omiten las tablas ambientales", flush=True)
        hay_te = bool(args.trafico_energia) and existe(spark, args.trafico_energia)

        if "medicion_horaria" in args.tablas:
            # Lecturas de prueba de scripts/pg_generar_mediciones.py: no deben mezclarse con datos reales.
            borradas = ejecutar_sql(spark, ["DELETE FROM medicion_horaria WHERE origen = 'prueba_pg'"])[0]
            print(f"SMARTCITY_B34_PRUEBA_PG_BORRADAS={borradas}", flush=True)
            if hay_ambiente:
                cargar(spark, medicion_horaria(amb, sensores, variables), "medicion_horaria", args.actualizar)
            if hay_te:
                te = (spark.read.parquet(args.trafico_energia)
                      .select("zona", "timestamp", "id_sensor", "tipo_metrica", "valor", "origen"))
                cargar(spark, medicion_horaria(te, sensores, variables), "medicion_horaria", args.actualizar)
        if "agg" in args.tablas and hay_ambiente:
            cargar(spark, agg_hora_zona(amb, zonas), "agg", args.actualizar)
        if "spark_kpi_zona" in args.tablas and hay_ambiente:
            cargar(spark, kpi_zona(amb, zonas), "spark_kpi_zona", args.actualizar)
        if "spark_anomalia" in args.tablas and existe(spark, f"{RUTAS['analitica']}/anomalias_detalle"):
            cargar(spark, anomalias(spark, sensores), "spark_anomalia", args.actualizar)

        # Refresco de vistas materializadas: sp_refrescar_mv() si A ya la creó; si no, la MV existente.
        existe_sp = ejecutar_sql(spark, ["SELECT COUNT(*) FROM pg_proc WHERE proname = 'sp_refrescar_mv'"])[0]
        if existe_sp:
            ejecutar_sql(spark, ["CALL sp_refrescar_mv()"])
            print("SMARTCITY_B34_MV=sp_refrescar_mv()", flush=True)
        else:
            ejecutar_sql(spark, ["REFRESH MATERIALIZED VIEW mv_actividad_sensor"])
            print("SMARTCITY_B34_MV=mv_actividad_sensor", flush=True)
        print("SMARTCITY_B34_RESUMEN=OK", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
