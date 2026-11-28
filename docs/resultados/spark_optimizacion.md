# Optimización de Spark (B4.3)

Programa: [spark/jobs/optimizar_spark.py](../../spark/jobs/optimizar_spark.py). Lee el Parquet curado (`RUTAS["ambiente"]`, 4.380.538 filas en la ejecución del 2026-11-27) y escribe solo en `/smartcity/sandbox/optimizacion_b43/`.

> **Estado:** ejecutado el 2026-11-27 con `--pruebas todas --repeticiones 3` (10 variantes, controles de resultado OK). Mediciones por repetición en [spark_optimizacion.csv](spark_optimizacion.csv); planes completos (`explain(mode="formatted")`) en [spark_optimizacion_planes.txt](spark_optimizacion_planes.txt). Ninguna mejora se da por supuesta: se reporta lo que se midió.

## 1. Cómo ejecutarlo

```powershell
.\scripts\spark_submit.ps1 optimizar_spark --pruebas todas --repeticiones 3
```

Guardar la salida completa (incluye los `EXPLAIN`) y exportar las mediciones al repositorio para el dashboard (página «Comparación tecnológica»):

```bash
MSYS_NO_PATHCONV=1 docker exec smartcity_namenode bash -c \
  "hdfs dfs -cat /smartcity/sandbox/optimizacion_b43/ejecucion_particiones_join_aqe_cache_csv/mediciones_csv/part-*.csv" \
  > docs/resultados/spark_optimizacion.csv
```

Tomar capturas de la Spark UI (http://localhost:8080 → aplicación `SmartCity-B4.3-Optimizacion` → *SQL / DataFrame*) de cada par de planes y guardarlas en `docs/resultados/img/spark_b43_*.png`.

## 2. Diseño de las pruebas

Todas las comparaciones cumplen tres condiciones: (1) las dos variantes devuelven **el mismo resultado**, y el programa aborta si los conteos difieren; (2) el orden de las variantes se **alterna** en cada repetición para no favorecer a la segunda con la caché del sistema operativo; (3) se reporta la **mediana**.

| Prueba | Variante base | Variante optimizada | Qué evidencia buscar en el `EXPLAIN` |
|---|---|---|---|
| Particiones (*partition pruning*) | `WHERE year(timestamp) = 2024` | `WHERE anio = 2024` (columna de partición) | `PartitionFilters: [isnotnull(anio), (anio = 2024)]` solo en la optimizada; menos archivos leídos |
| JOIN | sort-merge (auto-broadcast y AQE desactivados) | `broadcast(catalogo_sensores)` | `SortMergeJoin` + 2 `Exchange` frente a `BroadcastHashJoin` sin *shuffle* del lado grande |
| AQE | `spark.sql.adaptive.enabled=false`, 32 particiones de *shuffle* | `…=true` | `AdaptiveSparkPlan isFinalPlan=true` y `AQEShuffleRead coalesced` (une particiones pequeñas) |
| Caché | 2 agregaciones que releen el subconjunto PM2.5 2024 | `persist(MEMORY_AND_DISK)` + las mismas 2 agregaciones | `InMemoryRelation` / `InMemoryTableScan`; se informa también el costo de materializar |
| Formato | CSV con esquema explícito | Parquet (mismo DataFrame) | `FileScan csv` frente a `FileScan parquet` con `ReadSchema` de 2 columnas (lectura columnar) |

## 3. Resultados

Configuración: 1 worker, 2 *cores*, 2 GB (`docker-compose.yml`); `spark.sql.shuffle.partitions=8` salvo en la prueba AQE.

| Prueba | Base (s, mediana) | Optimizada (s, mediana) | Diferencia | Evidencia en el plan |
|---|---:|---:|---:|---|
| Particiones (2.368.576 filas de 2024) | 0,079 (`year(timestamp)`) | 0,080 (`anio = 2024`) | −1 % (igual) | Solo la optimizada tiene `PartitionFilters: [isnotnull(anio), (anio = 2024)]`; la base lee las 24 particiones y filtra después |
| JOIN broadcast (320.994 filas PM2.5 2024 × catálogo) | 0,226 (sort-merge) | 0,122 (`broadcast`) | **46 % menos** | `SortMergeJoin` con dos `Exchange` frente a `BroadcastHashJoin` sin *shuffle* del lado grande |
| AQE (32 particiones de *shuffle*) | 1,047 (desactivado) | 0,635 (activado) | **39 % menos** | `AdaptiveSparkPlan` solo en la variante con AQE; el `Exchange hashpartitioning(zona, 32)` se une en tiempo de ejecución (`isFinalPlan=false` en el `explain` previo; la unión se ve en la Spark UI) |
| Caché, solo reutilización | 0,606 (2 agregaciones) | 0,275 | 55 % menos | `InMemoryRelation` + `InMemoryTableScan` |
| Caché, contando materializar (0,921 s) | 1,212 | 1,508 | **24 % más** | Con solo 2 reutilizaciones no se paga la materialización |
| CSV → Parquet (misma muestra) | 0,040 | 0,041 | igual | `FileScan csv` frente a `FileScan parquet` con `ReadSchema: struct<zona, valor>` (lectura columnar) |

Tamaño en disco de la misma muestra (`hdfs dfs -du -s -h …/datos_prueba_csv …/datos_prueba_parquet`): **CSV 9,5 MB ·
Parquet 573 KB** (17 veces menos; con la réplica de HDFS, 28,5 MB frente a 1,7 MB).

Variación entre repeticiones: la primera repetición de cada prueba incluye leer los archivos desde disco (p. ej.
particiones: 3,42 s con `year(timestamp)` frente a 0,67 s con `anio`, ×5); en las siguientes los bloques ya están en
la caché del sistema operativo y las dos variantes tardan lo mismo. Por eso la mediana de particiones y de CSV es casi
igual y el beneficio real está en lo que **no** se lee: 12 de 24 particiones y 2 de 4 columnas. En AQE y caché las
diferencias son de décimas de segundo y varían entre repeticiones (AQE activado 0,48–0,71 s; desactivado 0,68–1,21 s).

## 4. Cómo interpretar los resultados

- Con ~4,4 M filas y un solo worker, varias optimizaciones pueden **no** mejorar el tiempo, o incluso empeorarlo: el costo fijo de planificar el job y de transmitir el *broadcast* es comparable al trabajo útil. Si es así, se documenta tal cual. Lo que sí demuestra la optimización es el **plan** (menos datos leídos, menos *shuffle*).
- La caché solo conviene si el subconjunto se **reutiliza** lo suficiente como para pagar la materialización; por eso se informan dos cifras.
- La ventaja de Parquet frente a CSV crece con el número de columnas que **no** se leen. En la muestra hay 4 columnas y la consulta usa 2.
