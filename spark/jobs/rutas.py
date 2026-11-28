"""Rutas HDFS de SmartCity: única fuente de verdad para los jobs Spark (common.py) y el dashboard
(conectores_mongo_spark.py). Sin dependencias, para poder importarse también desde Windows.

Capas (hadoop/hdfs/01_estructura.sh, docs/tecnico/07_estrategia_big_data.md §7.2):
  raw/        crudos inmutables (descargas, CSV simulados, export de MongoDB)
  curated/    Parquet limpio particionado anio=/mes= (salida de los ETL)
  analytics/  resultados de los análisis y consultas Spark que lee el dashboard
  sandbox/    pruebas y experimentos (B4.3, benchmarks, operaciones de la UI)
"""

RUTAS = {
    "raw": "/smartcity/raw",
    "ambiente": "/smartcity/curated/ambiente",                 # B3.2 (Mongo -> Parquet)
    "ambiente_hdfs": "/smartcity/curated/ambiente_hdfs",       # B3.2 (raw HDFS -> Parquet)
    "trafico_energia": "/smartcity/curated/trafico_energia",   # B3.3
    "analitica": "/smartcity/analytics",                       # B4.2 (una carpeta por salida de cada análisis)
    "consultas": "/smartcity/analytics/consultas_spark",       # B4.1 (resultado de cada SP-Qxx)
    "reporte_q10": "/smartcity/analytics/reporte_q10",         # SP-Q10
    "optimizacion": "/smartcity/sandbox/optimizacion_b43",     # B4.3 (experimento: se queda en sandbox)
}

# Capas donde los jobs pueden escribir resultados (las rutas de salida se validan contra estos prefijos).
CAPAS_ESCRITURA = ("/smartcity/curated/", "/smartcity/analytics/", "/smartcity/sandbox/")
