# Arquitectura — SmartCity Analytics

Cuatro motores, cada uno en el rol que mejor cumple (ver [04_justificacion_tecnologica.md](../tecnico/04_justificacion_tecnologica.md)).
El dashboard no contiene SQL ni pipelines: solo importa los dos módulos de conectores.

PNG (para el documento impreso): `docs/diagramas/img/arquitectura.png`, generado con `python scripts/exportar_diagramas.py`.

```mermaid
flowchart LR
    subgraph F["Fuentes"]
        direction TB
        AQ["OpenAQ<br/>calidad del aire (real)"]
        OM["Open-Meteo<br/>clima (real)"]
        SY["Generadores sintéticos<br/>IoT · tráfico · energía"]
        ST["Productor streaming<br/>lote cada 5 s"]
    end

    subgraph M["MongoDB — documentos IoT"]
        direction TB
        ML[("lectura · evento<br/>alerta · incidente")]
    end

    subgraph H["Hadoop — almacenamiento distribuido"]
        direction TB
        HR[("HDFS /smartcity/raw<br/>3 DataNodes · réplica ×2")]
        HC[("HDFS curated · analytics<br/>Parquet anio/mes")]
        MR["MapReduce en YARN<br/>mr01…mr05"]
    end

    subgraph S["Spark — procesamiento distribuido"]
        direction TB
        SE["ETL · Spark SQL<br/>8 análisis · MLlib"]
        SS["Structured Streaming<br/>ventanas de 1 min"]
    end

    subgraph P["PostgreSQL — relacional"]
        direction TB
        PM[("Maestros 3FN<br/>sensores · zonas · organismos")]
        PH[("medicion_horaria particionada<br/>spark_kpi_zona · spark_anomalia")]
    end

    subgraph D["Streamlit"]
        direction TB
        CA["conectores_postgres_hadoop.py"]
        CB["conectores_mongo_spark.py"]
        UI["dashboard.py<br/>KPIs · filtros · gráficos · mapa · consultas"]
    end

    AQ --> HR
    OM --> HR
    AQ --> ML
    OM --> ML
    SY --> ML
    SY --> HR
    ML -- "export JSONL" --> HR
    HR --> MR
    HR --> SE
    ML -- "conector Mongo" --> SE
    PM -- "JDBC (catálogo)" --> SE
    SE --> HC
    SE -- "JDBC + ON CONFLICT" --> PH
    ST --> SS
    SS -- "alertas" --> ML
    PM --- CA
    PH --- CA
    HR --- CA
    MR --- CA
    ML --- CB
    HC --- CB
    SE --- CB
    CA --> UI
    CB --> UI
```

| Capa | Tecnología | Rol | Datos |
|---|---|---|---|
| Ingesta | Scripts Python | Descarga idempotente y generación reproducible (semilla 2026) | `data/raw/`, `data/interim/` |
| Operacional documental | MongoDB 7 | Lecturas con esquema variable, eventos, alertas, incidentes | ~1,07 M documentos |
| Almacenamiento masivo | HDFS 3.2.1 | Crudos inmutables, exports, Parquet curado, salidas MapReduce | réplica ×2 en 3 nodos |
| Procesamiento batch | MapReduce (YARN) y Spark 3.5 | Conteos masivos; ETL, SQL, análisis, ML | 4,38 M filas en formato largo |
| Procesamiento continuo | Spark Structured Streaming | Alertas por umbral en ventanas de 1 min | lotes de HDFS → Mongo |
| Relacional | PostgreSQL 16 | Maestros con integridad, mediciones limpias, resultados de Spark | `medicion_horaria` ≥ 175k filas |
| Presentación | Streamlit | Vista única con KPIs de 4 grupos, consultas por tecnología, comparación | — |
