# Despliegue en Docker

Un solo `docker compose` levanta los 10 contenedores en la red por defecto del proyecto (`docker_default`). El dashboard y los scripts corren en
Windows y llegan a los servicios por los puertos publicados; los jobs Spark corren **dentro** del master (D-04).

PNG (para el documento impreso): `docs/diagramas/img/despliegue_docker.png`, generado con `python scripts/exportar_diagramas.py`.

```mermaid
flowchart TB
    subgraph HOST["Anfitrión (Windows) — Python 3.11+ · Streamlit"]
        DASH["dashboard.py :8501"]
        SCR["scripts/*.py · *.ps1 · *.sh"]
    end

    subgraph NET["Docker · red docker_default"]
        PG[("smartcity_postgres<br/>postgres:16<br/>:5433→5432")]
        MG[("smartcity_mongo<br/>mongo:7<br/>:27017")]
        subgraph HAD["Hadoop 3.2.1 (bde2020)"]
            NN["smartcity_namenode<br/>:9870 · :8020 · 1 GB"]
            DN1["datanode1 :9864<br/>512 MB"]
            DN2["datanode2 :9865<br/>512 MB"]
            DN3["datanode3 :9866<br/>512 MB"]
            RM["resourcemanager :8088<br/>768 MB"]
            NM["nodemanager :8042<br/>Python 3 · 2,5 GB"]
        end
        subgraph SPK["Spark 3.5"]
            SM["smartcity_spark_master<br/>:8080 · :7077<br/>repo en /opt/smartcity (solo lectura)"]
            SW["smartcity_spark_worker<br/>2 núcleos · 2 GB"]
        end
    end

    DASH -- "SQLAlchemy" --> PG
    DASH -- "pymongo" --> MG
    DASH -- "JMX :9870 · docker exec" --> NN
    DASH -- "docker exec spark-submit" --> SM
    SCR -- "COPY · insert_many · hdfs dfs -put" --> PG & MG & NN
    NN --- DN1 & DN2 & DN3
    RM --- NM
    SM --- SW
    SW -- "HDFS :8020" --> NN
    SW -- "mongo:27017" --> MG
    SW -- "JDBC postgres:5432" --> PG
```

| Contenedor | Imagen | Puertos (anfitrión) | Memoria |
|---|---|---|---|
| smartcity_postgres | `postgres:16` | 5433 → 5432 | sin límite |
| smartcity_mongo | `mongo:7` | 27017 | sin límite |
| smartcity_namenode | `bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8` | 9870, 8020 | 1 GB |
| smartcity_datanode1…3 | `bde2020/hadoop-datanode:…` | 9864, 9865, 9866 | 512 MB c/u |
| smartcity_resourcemanager | `bde2020/hadoop-resourcemanager:…` | 8088 | 768 MB |
| smartcity_nodemanager | `smartcity-hadoop-nodemanager:3.2.1` (bde2020 + Python 3) | 8042 | 2,5 GB |
| smartcity_spark_master | `bitnamilegacy/spark:3.5` (o `smartcity-spark-ml:3.5`) | 8080, 7077 | sin límite |
| smartcity_spark_worker | ídem | — | 2 GB para ejecutores, 2 núcleos |

El puerto 5433 se eligió porque el 5432 del anfitrión lo ocupa un PostgreSQL instalado en Windows. Los límites de
memoria permiten levantar todo con ~8 GB libres para Docker.
