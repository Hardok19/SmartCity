# Comparación tecnológica — resultados de los experimentos (C1)

Generado por `scripts/benchmark_comparacion.py`. Mediana de las repeticiones tras 1 calentamiento; los tiempos de PostgreSQL y MongoDB se miden desde el cliente Python con la conexión abierta, y los de Spark dentro del job ya iniciado (el arranque del job se informa aparte en E2). Datos CSV: [benchmarks.csv](benchmarks.csv); también en la tabla `benchmark_resultado`.

**Entorno:** cpus_docker = 12, memoria_docker = 7.7 GiB, postgresql = 16.15 (Debian 16.15-1.pgdg13+2), shared_buffers = 128MB, mongodb = 7.0.43, spark = 3.5 (1 worker, 2 núcleos, 2 GB), hdfs = 3.2.1 (3 DataNodes, replicación 2), anfitrion = Windows-11

## E0 · Carga (ingesta) del mismo CSV

| Motor | Variante | 100k | 1 M | 5 M |
|---|---|---|---|---|
| HDFS | `put_csv` | 2,06 | 2,41 | 11,82 |
| MongoDB | `crear_indice` | 228,0 ms | 2,14 | 14,21 |
| MongoDB | `insert_many` | 2,20 | 20,86 | 108 |
| PostgreSQL | `copy` | 144,9 ms | 1,92 | 9,59 |
| PostgreSQL | `crear_indice` | 175,9 ms | 1,49 | 13,82 |
| Spark | `csv_a_parquet` | 8,46 | 2,29 | 10,16 |

Tiempos en segundos (o ms cuando es < 1 s). La conversión a Parquet de 100k incluye el primer job de la sesión de Spark (carga de clases y conexión con los ejecutores), por eso tarda más que la de 1 M.

## E1 · Agregación promedio por zona-hora

| Motor | Variante | 100k | 1 M | 5 M |
|---|---|---|---|---|
| MongoDB | `aggregate_group` | 134,8 ms | 1,17 | 6,66 |
| PostgreSQL | `group_by_sql` | 48,3 ms | 170,7 ms | 999,4 ms |
| Spark | `dataframe_cores1` | 480,4 ms | 573,4 ms | 1,59 |
| Spark | `dataframe_cores2` | 534,8 ms | 539,5 ms | 840,9 ms |

Tiempos en segundos (o ms cuando es < 1 s).

## E2 · Búsqueda puntual: últimas 100 lecturas de S-011

| Motor | Variante | 100k | 1 M | 5 M |
|---|---|---|---|---|
| MongoDB | `con_indice` | 1,9 ms | 1,4 ms | 2,4 ms |
| PostgreSQL | `con_indice` | 1,4 ms | 0,7 ms | 0,7 ms |
| Spark | `full_scan_parquet` | 370,8 ms | 156,4 ms | 286,7 ms |
| Spark | `job_completo_con_arranque` | 12,70 | — | — |

Tiempos en segundos (o ms cuando es < 1 s). `job_completo_con_arranque` es un `spark-submit` completo (JVM, registro en el master, ejecutores y consulta), medido con la escala más chica.

## E3 · Join sensor → tipo → categoría → ubicación → organismo

| Motor | Variante | 100k | 1 M | 5 M |
|---|---|---|---|---|
| MongoDB | `lookup_por_documento` | 25,29 | 237 | — |
| MongoDB | `lookup_tras_agrupar` | 111,9 ms | 927,7 ms | 4,89 |
| PostgreSQL | `join_sql` | 48,5 ms | 576,5 ms | 3,52 |
| Spark | `join_broadcast_cores2` | 434,0 ms | 528,3 ms | 910,1 ms |

Tiempos en segundos (o ms cuando es < 1 s). `lookup_por_documento` con más de 100k filas se corre una sola vez, sin calentamiento (tarda minutos); con 5 M no se ejecuta.

## E4 · Nuevo tipo de sensor con campos nuevos (10.000 lecturas)

| Motor | Variante | 1 M |
|---|---|---|
| MongoDB | `consulta_exists_campo_nuevo` | 416,4 ms |
| MongoDB | `insert_10k_campos_nuevos` | 172,8 ms |
| PostgreSQL | `pg_add_column_default_constante` | 1,0 ms |
| PostgreSQL | `pg_add_column_default_volatil` | 4,21 |
| PostgreSQL | `pg_add_column_jsonb` | 14,8 ms |
| PostgreSQL | `pg_add_column_nullable` | 0,8 ms |
| PostgreSQL | `pg_insert_10k_nuevo_tipo` | 60,7 ms |
| PostgreSQL | `pg_insert_tipo_sensor` | 2,3 ms |

Tiempos en segundos (o ms cuando es < 1 s). Cada paso de PostgreSQL corre en una transacción que se revierte; el de MongoDB borra los documentos insertados al terminar.

## E5 · Spark con 1 frente a 2 núcleos (consulta E1)

| Motor | Variante | 100k | 1 M | 5 M |
|---|---|---|---|---|
| Spark | `cores1` | 480,4 ms | 573,4 ms | 1,59 |
| Spark | `cores2` | 534,8 ms | 539,5 ms | 840,9 ms |

Tiempos en segundos (o ms cuando es < 1 s).

## E6 · HDFS con un DataNode caído

| Motor | Medida | Segundos |
|---|---|---|
| HDFS | `deteccion_nodo_muerto` | 97,00 |
| HDFS | `lectura_con_nodo_caido` | 1,80 |
| HDFS | `re_replicacion_completa` | 152 |
| HDFS | `reingreso_nodo` | 7,00 |

Tiempos en segundos (o ms cuando es < 1 s). Cifras de `hadoop/hdfs/04_tolerancia.ps1` (A3.4), que apaga de verdad `smartcity_datanode2`.

## E7 · Índices: con y sin índice (consulta E2)

| Motor | Variante | 100k | 1 M | 5 M |
|---|---|---|---|---|
| MongoDB | `con_indice` | 1,9 ms | 1,4 ms | 2,4 ms |
| MongoDB | `sin_indice` | 48,5 ms | 446,9 ms | 2,22 |
| PostgreSQL | `con_indice` | 1,4 ms | 0,7 ms | 0,7 ms |
| PostgreSQL | `sin_indice` | 10,9 ms | 37,6 ms | 164,9 ms |

Tiempos en segundos (o ms cuando es < 1 s).
