# Optimización de Spark (B4.3)

Programa: [spark/jobs/optimizar_spark.py](../../spark/jobs/optimizar_spark.py). Lee el Parquet curado (`RUTAS["ambiente"]`, 4.389.705 filas en la ejecución de referencia) y escribe solo en `/smartcity/sandbox/optimizacion_b43/`.

> **Estado:** el programa está listo; **faltan las cifras** de una ejecución en el clúster. Las tablas de la §3 se completan con las líneas `B43_MEDIANA` y `B43_DIFERENCIA` de la salida. Ninguna mejora se da por supuesta: se reporta lo que se mida.

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

| Prueba | Base (s, mediana) | Optimizada (s, mediana) | Diferencia (%) | Evidencia en el plan |
|---|---:|---:|---:|---|
| Particiones | _pendiente_ | _pendiente_ | _pendiente_ | _pendiente_ |
| JOIN broadcast | _pendiente_ | _pendiente_ | _pendiente_ | _pendiente_ |
| AQE | _pendiente_ | _pendiente_ | _pendiente_ | _pendiente_ |
| Caché (solo reutilización / con materialización) | _pendiente_ | _pendiente_ | _pendiente_ | _pendiente_ |
| CSV → Parquet | _pendiente_ | _pendiente_ | _pendiente_ | _pendiente_ |

Tamaño en disco de la misma muestra (`hdfs dfs -du -s -h …/datos_prueba_csv …/datos_prueba_parquet`): CSV _pendiente_ · Parquet _pendiente_.

## 4. Cómo interpretar los resultados

- Con ~4,4 M filas y un solo worker, varias optimizaciones pueden **no** mejorar el tiempo, o incluso empeorarlo: el costo fijo de planificar el job y de transmitir el *broadcast* es comparable al trabajo útil. Si es así, se documenta tal cual. Lo que sí demuestra la optimización es el **plan** (menos datos leídos, menos *shuffle*).
- La caché solo conviene si el subconjunto se **reutiliza** lo suficiente como para pagar la materialización; por eso se informan dos cifras.
- La ventaja de Parquet frente a CSV crece con el número de columnas que **no** se leen. En la muestra hay 4 columnas y la consulta usa 2.
