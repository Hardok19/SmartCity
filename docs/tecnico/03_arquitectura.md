# 3. Arquitectura

## 3.1 Vista general

Diagrama: [diagramas/arquitectura.md](../diagramas/arquitectura.md) (Mermaid; GitHub lo muestra renderizado).

<!-- incluir-diagrama: ../diagramas/arquitectura.md -->

Cada motor cumple un rol:

| Motor | Rol en SmartCity | Qué guarda o hace |
|---|---|---|
| **MongoDB** | Base operativa de documentos IoT | Lecturas con esquema distinto por tipo y firmware, eventos, alertas (batch y streaming), incidentes con texto libre |
| **HDFS** | Repositorio histórico distribuido | Crudos inmutables, exports de Mongo, Parquet curado, salidas de MapReduce y del streaming |
| **MapReduce** (YARN) | Procesamiento batch cercano a los datos | Las 5 preguntas de Hadoop: conteos y promedios sobre todo el histórico |
| **Spark** | Motor de procesamiento y análisis | ETL a formato largo, 10 consultas Spark SQL, 8 análisis, MLlib, streaming con alertas |
| **PostgreSQL** | Base relacional de referencia | Maestros en 3FN con integridad, mediciones horarias limpias particionadas, resultados agregados de Spark |
| **Streamlit** | Interfaz | Vista única con KPIs, filtros, 4 gráficos, tabla, mapa; consultas por tecnología; comparación |

## 3.2 Flujo de datos

Diagrama: [diagramas/flujo_datos.md](../diagramas/flujo_datos.md).

<!-- incluir-diagrama: ../diagramas/flujo_datos.md -->

Las integraciones del objetivo específico 5:

1. **Sensores → MongoDB → HDFS → Spark.** Las lecturas entran a Mongo (`mongo_cargar.py`); se exportan a HDFS en
   JSON Lines con validación de conteos (`subir_crudos_hdfs.py --fuentes mongo`); Spark las une con los crudos de
   OpenAQ y Open-Meteo (`etl_ambiente_hdfs.py`).
2. **Spark → PostgreSQL.** `carga_pg.py` escribe en una tabla de staging por JDBC y luego ejecuta
   `INSERT … ON CONFLICT`, por lo que se puede volver a ejecutar sin duplicar. Al final llama a `sp_refrescar_mv()`.
3. **PostgreSQL → Spark.** El catálogo de sensores (tipo, categoría, zona, organismo) se lee por JDBC y se envía en
   *broadcast* a los ejecutores.
4. **Streaming → MongoDB.** Las alertas en tiempo real llegan a la misma colección `alerta` que las del batch.

## 3.3 Despliegue

Diagrama: [diagramas/despliegue_docker.md](../diagramas/despliegue_docker.md); estructura de HDFS en
[diagramas/estructura_hdfs.md](../diagramas/estructura_hdfs.md).

<!-- incluir-diagrama: ../diagramas/despliegue_docker.md -->

Decisiones:

| Decisión | Motivo |
|---|---|
| Todo en Docker Compose (`docker/docker-compose.yml`) | Reproducible en cualquier computadora del equipo con un solo comando; versiones fijas de cada imagen |
| 3 DataNodes con replicación 2 (D-05) | Con 1 DataNode no se puede mostrar replicación ni tolerancia a fallos |
| YARN con NodeManager propio con Python 3 | Hadoop Streaming ejecuta los mappers y reducers en Python; la imagen original no lo trae |
| Spark con `spark-submit` dentro del master (D-04) | Un driver en Windows conectado al clúster en Docker falla por red y por la versión de Python; el repositorio se monta en `/opt/smartcity` en modo solo lectura |
| PostgreSQL publicado en 5433 | El 5432 del anfitrión lo ocupa un PostgreSQL local |
| `mem_limit` en Hadoop | Todo el entorno cabe en ~8 GB |

## 3.4 Organización del código

| Carpeta / archivo | Contenido |
|---|---|
| `contrato.py`, `comun.py` | Contrato de datos (zonas, tipos, columnas estándar, validaciones), `Filtros`, `Consulta`, configuración desde `.env` |
| `conectores_postgres_hadoop.py` | Pareja A: conexión, 17 consultas PG, EXPLAIN, operaciones HDFS, MapReduce, funciones para el dashboard |
| `conectores_mongo_spark.py` | Pareja B: conexión, 15 consultas Mongo, CRUD, 10 consultas Spark, análisis, streaming, funciones para el dashboard |
| `dashboard.py`, `dashboard_app/` | Interfaz Streamlit: páginas, componentes, filtros, adaptador de fuentes |
| `sql/` | DDL (`ddl/001…007`), consultas (`queries/PG-*.sql`), demo de la defensa |
| `mongo/` | Colecciones y validadores, índices, CRUD, consultas (`queries/MG-*.js`), export |
| `hadoop/` | Scripts de HDFS (`hdfs/01…04`) y jobs MapReduce (`mapreduce/mr01…mr05`) |
| `spark/` | Jobs (`jobs/*.py`) y consultas (`sql/SP-*.sql`) |
| `scripts/` | Descarga, generación, carga, optimización, benchmarks, documentación |
| `tests/` | Pruebas unitarias y de integración (se omiten si el servicio no responde) |
| `docs/` | Documento técnico, datos, diagramas, resultados, defensa, atributos |

## 3.5 Contrato entre módulos

Para que el dashboard no dependa de cómo cada pareja consulta su motor, todas las funciones que devuelven lecturas
respetan `contrato.COLUMNAS_ESTANDAR` = `zona, timestamp, id_sensor, tipo_metrica, valor, fuente`, con `timestamp` en
UTC, zonas del catálogo, `tipo_metrica` = la métrica concreta (`pm25`, `temperatura`, `trafico`…, con su unidad en
`contrato.UNIDADES`; decisión D-01) y `fuente` ∈ {PostgreSQL, MongoDB, HDFS, Spark}. Todas reciben un `comun.Filtros` (fechas,
perfil horario, zonas, tipo, sensor, tipo de evento, fuente, rango de valores). Las consultas obligatorias se registran
como `comun.Consulta` (id, tecnología, título, código mostrado, parámetros, función), y la página «Consultas por
tecnología» las lista sin conocer su implementación. Detalle: [INTERFAZ_DASHBOARD.md](INTERFAZ_DASHBOARD.md).
