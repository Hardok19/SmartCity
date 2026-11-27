"""Rutas HDFS de SmartCity: única fuente de verdad para los jobs Spark (common.py) y el dashboard
(conectores_mongo_spark.py). Sin dependencias, para poder importarse también desde Windows.

Por ahora apuntan a /smartcity/sandbox (donde están los resultados ya validados). Cuando el equipo
acuerde las rutas finales (/smartcity/curated, /smartcity/analytics) se cambian SOLO aquí.
"""

RUTAS = {
    "raw": "/smartcity/raw",
    "ambiente": "/smartcity/sandbox/etl_ambiente_completa",          # B3.2 (Mongo -> Parquet)
    "ambiente_hdfs": "/smartcity/sandbox/etl_ambiente_hdfs_integral",  # B3.2 (raw HDFS -> Parquet)
    "trafico_energia": "/smartcity/sandbox/etl_trafico_energia",     # B3.3
    "analitica": "/smartcity/sandbox/analitica",                      # B4.2
    "consultas": "/smartcity/sandbox/consultas_spark",                # B4.1 (resultado de cada SP-Qxx)
    "reporte_q10": "/smartcity/sandbox/reporte_spark_q10",            # SP-Q10
    "optimizacion": "/smartcity/sandbox/optimizacion_b43",            # B4.3
}
