# Flujo de datos entre las cuatro tecnologías

Recorrido de un dato desde que se descarga o genera hasta que aparece en el dashboard. Cada flecha indica el script o
job que la implementa.

PNG (para el documento impreso): `docs/diagramas/img/flujo_datos.png`, generado con `python scripts/exportar_diagramas.py`.

```mermaid
flowchart TD
    A1["descargar_openaq.py<br/>descargar_openmeteo.py"] --> R1["data/raw<br/>CSV · JSON"]
    A2["generar_sinteticos.py"] --> R2["data/interim/mongo<br/>JSONL"]
    A3["generar_trafico_sintetico.py<br/>generar_energia_sintetica.py"] --> R3["data/raw/trafico · energia<br/>CSV"]

    R1 -- "02_cargar_raw.sh / subir_crudos_hdfs.py" --> H1[("HDFS raw/openaq · openmeteo")]
    R3 -- "02_cargar_raw.sh" --> H3[("HDFS raw/trafico · energia")]
    R2 -- "mongo_cargar.py<br/>insert_many, lotes de 10k" --> M1[("MongoDB lectura · evento · alerta · incidente")]
    M1 -- "subir_crudos_hdfs.py --fuentes mongo<br/>(conteo Mongo = líneas HDFS)" --> H2[("HDFS raw/mongo_exports")]

    M1 -- "etl_ambiente.py" --> C1[("Parquet ambiente<br/>anio/mes")]
    H1 -- "etl_ambiente_hdfs.py" --> C1
    H2 -- "etl_ambiente_hdfs.py" --> C1
    H3 -- "etl_trafico_energia.py" --> C2[("Parquet tráfico · energía<br/>anio/mes")]

    H1 -- "mr01 · mr02 · mr03 · mr05" --> MR[("HDFS mapreduce/mrNN")]
    H2 -- "mr04 eventos por zona" --> MR

    C1 -- "ejecutar_consultas.py<br/>SP-Q01…Q10" --> C3[("Parquet consultas_spark · reporte_q10")]
    C1 -- "analitica.py<br/>8 análisis + MLlib" --> C4[("Parquet analitica")]

    P0[("PostgreSQL maestros<br/>pg_init.py · pg_cargar_maestros.py")] -- "JDBC" --> CPG
    C1 --> CPG["carga_pg.py<br/>staging + INSERT … ON CONFLICT"]
    C2 --> CPG
    C3 --> CPG
    C4 --> CPG
    CPG --> P1[("PostgreSQL medicion_horaria · agg_ambiente_hora_zona<br/>spark_kpi_zona · spark_anomalia")]
    P1 -- "CALL sp_refrescar_mv()" --> P2[("mv_kpi_diario_zona · mv_actividad_sensor")]

    SP["streaming_productor.py"] -- "lote JSON cada 5 s" --> H4[("HDFS streaming/entrada")]
    H4 -- "streaming_alertas.py<br/>ventana 1 min + watermark" --> M2[("MongoDB stream_ventana · alerta")]

    P0 & P1 & P2 & MR & H1 --> CA["conectores_postgres_hadoop.py"]
    M1 & M2 & C1 & C2 & C3 & C4 --> CB["conectores_mongo_spark.py"]
    CA & CB --> UI["dashboard.py"]
```

**Integraciones que pide el enunciado** (objetivo específico 5):

1. *Datos de sensores → MongoDB → HDFS → Spark*: `mongo_cargar.py` → `subir_crudos_hdfs.py --fuentes mongo` →
   `etl_ambiente_hdfs.py`.
2. *Datos limpios de Spark → PostgreSQL*: `carga_pg.py` (idempotente: volver a ejecutarlo no duplica filas).
3. *Catálogo de PostgreSQL → Spark*: `carga_pg.py` y `benchmark_spark.py` leen sensores, tipos, categorías y
   organismos por JDBC.
4. *Streaming → MongoDB*: las alertas del job de streaming entran en la misma colección `alerta` que las del
   procesamiento batch, con `origen = "streaming"`.
