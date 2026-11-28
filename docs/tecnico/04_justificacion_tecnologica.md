# 4. Justificación tecnológica y comparación

## 4.1 Pregunta y método

El enunciado pide demostrar **con experimentos** qué tecnología conviene para cada tipo de dato y cada necesidad de
procesamiento. Para que la comparación sea justa se usó el **mismo dato lógico** en los tres motores que consultan:
`lectura_<n>` = (id_sensor, zona, tipo_metrica, timestamp, valor) para los 50 sensores del catálogo, una lectura cada
15 minutos, a **100.000, 1.000.000 y 5.000.000 filas** (semilla 2026, `scripts/benchmark_comparacion.py`).

| Aspecto | Cómo se controló |
|---|---|
| Mismo contenido | Un único CSV por escala, cargado por `COPY` en PostgreSQL, `insert_many` en MongoDB y `hdfs dfs -put` + Parquet para Spark |
| Mismo resultado | Cada consulta devuelve una suma de control (grupos, suma de promedios, total). **Coincidió en los tres motores en las tres escalas** (p. ej. E1 con 5 M: 125.000 grupos y suma 18.658.787,3725 en los tres) |
| Repeticiones | 1 calentamiento + 3 repeticiones; se informa la mediana |
| Qué se mide | PostgreSQL y MongoDB: desde el cliente Python con la conexión abierta. Spark: la acción dentro de un job ya iniciado; el arranque del job se mide aparte (E2) |
| Índices | PostgreSQL `(id_sensor, timestamp DESC)` y MongoDB `{id_sensor: 1, timestamp: -1}`; Spark no tiene índices |
| Entorno | Una sola máquina: Docker con 12 CPU y 7,7 GB; PostgreSQL 16 (`shared_buffers` 128 MB, 2 workers paralelos); MongoDB 7.0; Spark 3.5 con 1 worker de 2 núcleos y 2 GB; HDFS 3.2.1 con 3 DataNodes |

Resultados completos: [benchmarks.md](../resultados/benchmarks.md) y [benchmarks.csv](../resultados/benchmarks.csv);
también en la tabla `benchmark_resultado` y, con gráficos interactivos, en la página «Comparación tecnológica» del
dashboard. A esto se suman los experimentos de cada motor: A2.5 ([postgres_optimizacion.md](../resultados/postgres_optimizacion.md)),
A3.4 ([hdfs_tolerancia.md](../resultados/hdfs_tolerancia.md)), B2.5 y B4.3.

**Límites del método.** Un solo nodo físico no muestra la escalabilidad horizontal de MongoDB (sharding) ni de Spark
(más workers): todos los motores compiten por la misma máquina. Los datos de benchmark son más regulares que los
reales y se comprimen mejor. Por eso las cifras sirven para comparar **tendencias** entre motores, no como rendimiento
absoluto de producción.

## 4.2 Matriz comparativa

La matriz del enunciado, con nuestra evaluación de cada celda. Cuando no coincidimos con la calificación del
enunciado se marca con **⚠** y se explica abajo.

| Criterio | Relacional (PostgreSQL) | Documental (MongoDB) | Distribuido (Hadoop: HDFS + MapReduce) | Procesamiento distribuido (Spark) |
|---|---|---|---|---|
| **Datos estructurados** | **Excelente.** Tipos, `NOT NULL`, `CHECK` y FK garantizan la forma del dato; 3FN sin redundancia (05) | **Bueno.** Los guarda, pero la estructura la impone la aplicación; el validador `$jsonSchema` es opcional y parcial | **Bueno.** Guarda CSV o Parquet sin entenderlos; la estructura la aplica quien lee | **Excelente.** Esquema explícito (`StructType`) y Parquet tipado y columnar: 255 MB de CSV quedan en 12 MB (E0) |
| **Datos semiestructurados** | **Bueno.** JSONB sirve, pero cada campo nuevo que se quiera consultar bien exige índice o columna; agregar una columna que reescribe la tabla tardó 4,2 s y la bloqueó (E4) | **Excelente.** Lecturas con campos distintos por tipo y firmware sin ninguna sentencia DDL (E4: 10.000 documentos con campos nuevos en 0,17 s) | **Excelente para almacenarlos ⚠, no para consultarlos.** HDFS guarda JSON Lines tal cual; consultarlos exige MapReduce o Spark | **Excelente.** Lee JSON y documentos de Mongo con esquema explícito; los campos ausentes quedan nulos |
| **Relaciones complejas** | **Excelente.** JOIN de 5 tablas sobre 1 M lecturas en 0,58 s (E3) con integridad garantizada | **Limitado.** `$lookup` por documento: 25 s con 100k y 237 s con 1 M lecturas, ×410 más lento que PostgreSQL (E3). Solo se acerca si se agrupa antes a mano (0,93 s) | **No es su propósito.** Un join en MapReduce es un job de varias fases escrito a mano | **Bueno ⚠ (no excelente).** Joins distribuidos y *broadcast* rápidos (0,91 s con 5 M, E3), pero sin FK ni transacciones: la relación no está garantizada |
| **Grandes volúmenes** | **Bueno ⚠.** Con 5 M filas sigue siendo rápido (agregación 1,0 s), pero escala en vertical: crece la máquina, no el número de nodos | **Bueno en un nodo, excelente con sharding ⚠.** Sin sharding fue el más lento en agregaciones (6,7 s con 5 M, E1) | **Excelente.** Bloques replicados en 3 nodos; agregar DataNodes amplía capacidad sin tocar los datos (07) | **Excelente.** Es el único cuyo tiempo crece menos que el volumen: ×5 datos → ×1,6 tiempo entre 1 M y 5 M (E1) |
| **Consultas SQL** | **Sí.** SQL completo: CTE recursivas, ventanas, `ROLLUP`, funciones (17 consultas PG-*) | **No tradicionalmente.** Aggregation Framework (`$group`, `$facet`, `$setWindowFields`), expresivo pero no SQL | **No directamente.** Haría falta Hive o Spark SQL encima de HDFS | **Sí.** Spark SQL sobre vistas temporales (10 consultas SP-*), con `PIVOT`, `CUBE` y ventanas |
| **Procesamiento distribuido** | **Limitado.** Paraleliza dentro de un servidor (2 workers en E1), no entre máquinas | **Escalable horizontalmente** con sharding y réplicas; en un nodo, cada agregación usa un solo hilo | **Sí.** MapReduce ejecuta las tareas donde están los bloques (localidad de datos) | **Sí.** E5: con 5 M filas, 2 núcleos tardan 0,84 s frente a 1,59 s con 1 (×1,9) |
| **Analítica** | **Excelente** en volumen medio: ventanas, CTE, MV (×3.900 más rápida que la consulta en vivo con 2,6 M filas, A2.5) | **Buena.** `$facet`, `$bucket`, promedios móviles; costosa sobre todo el histórico (E1) | **Básica / infraestructura.** Conteos y promedios en MapReduce (mr01–mr05); cada etapa escribe a disco | **Excelente.** 8 análisis, correlaciones, MLlib y streaming con la misma API |
| **Integridad relacional** | **Excelente.** FK con `ON DELETE RESTRICT`, `CHECK`, transacciones; la demo rechaza datos inválidos en vivo | **No relacional.** Ninguna garantía entre colecciones: un `sensor_id` inexistente se acepta | **No es su objetivo.** Archivos inmutables; garantiza que no se pierdan, no que sean coherentes | **No es su objetivo principal.** Valida en el ETL (catálogo en *broadcast*), pero no impide que otro proceso escriba datos incoherentes |

**Dónde matizamos la matriz del enunciado:**

1. **Grandes volúmenes, PostgreSQL «Excelente» → «Bueno».** A 5 M filas PostgreSQL sigue cerca de Spark
   (E1: 1,0 s frente a 0,84 s), pero su tiempo creció casi proporcional al volumen (×5,9 de 1 M a 5 M) y
   solo puede crecer comprando una máquina más grande. En nuestro proyecto lo usamos para el volumen *limpio y
   agregado* (≥ 175k filas en `medicion_horaria`), no para el histórico crudo.
2. **Grandes volúmenes, MongoDB «Excelente» → depende del sharding.** En un nodo fue el más lento en agregar
   (E1: 6,7 s con 5 M, ×6,7 PostgreSQL) y en cargar (E0: 108 s frente a 9,6 s del `COPY`). Su fortaleza en volumen
   es escribir lecturas continuamente y repartir colecciones entre shards, no recorrer todo el histórico.
3. **Semiestructurados, Hadoop «Excelente» → excelente para guardar.** HDFS no interpreta el contenido; el valor está
   en guardar el JSON crudo inmutable y replicado para reprocesarlo, y dejar la interpretación a Spark o MapReduce.
4. **Relaciones complejas, Spark «Excelente» → «Bueno».** Spark resuelve joins grandes muy bien (E3: ×3,9 más rápido
   que PostgreSQL con 5 M), pero «relaciones» también significa integridad. Spark no la garantiza; por eso el
   catálogo vive en PostgreSQL y Spark lo lee por JDBC.

## 4.3 Cada tipo de dato del proyecto en su tecnología

| Dato | Características | Tecnología | Por qué | Alternativa descartada |
|---|---|---|---|---|
| Catálogo: sensores, tipos, categorías, ubicaciones, organismos, usuarios, calendario | Estructurado, pequeño (cientos de filas), relaciones fijas, se consulta mucho | **PostgreSQL** | FK y `CHECK` evitan sensores sin zona o con tipo inexistente; 3FN; consultas puntuales con índice en < 1 ms (E2) | MongoDB: sin integridad entre colecciones y `$lookup` ×410 más lento (E3) |
| Mediciones horarias limpias y resultados de Spark (`medicion_horaria`, `spark_*`) | Estructurado, volumen medio, consultas por fecha y sensor | **PostgreSQL** particionado por mes | Poda de particiones (×10) e índices (×283) en A2.5, con 2,6 M filas; el dashboard las consulta en milisegundos | Spark directo: ~0,5 s por acción y ~12 s si hay que lanzar el job (E2) |
| Lecturas IoT crudas | Semiestructurado: campos distintos por tipo de sensor y por firmware, llegada continua | **MongoDB** | Cada documento trae solo sus campos; el firmware 2.x agregó `o3`, `co2` y `bateria_pct` sin migrar nada (E4); índice compuesto para consultas por sensor | PostgreSQL: columnas dispersas con muchos `NULL` o JSONB, y DDL por cada tipo nuevo (E4) |
| Eventos, alertas e incidentes | Detalles distintos por tipo, arreglos (historial, reportes ciudadanos), texto libre | **MongoDB** | Arreglos embebidos, índice de texto e índice geoespacial 2dsphere | PostgreSQL: tablas hijas por cada arreglo y un tipo de detalle por tabla |
| Archivos crudos (OpenAQ, Open-Meteo, tráfico, energía) y exports de Mongo | Masivo, inmutable, histórico | **HDFS** | Réplica ×2 en 3 nodos: la lectura siguió con un nodo caído (E6); se reprocesa sin volver a descargar | Disco local: un solo punto de falla y sin procesamiento paralelo |
| Conteos sobre todo el histórico (las 5 preguntas de Hadoop) | Batch, recorrido completo | **MapReduce** en YARN | Procesa en el nodo que tiene el bloque; los mappers emiten suma y conteo y el combiner reduce el tráfico de red | Python en una máquina: no escala ni tolera fallos |
| Limpieza, formato largo, análisis, ML | Batch masivo, transformaciones encadenadas | **Spark** → Parquet en HDFS | ETL de 1,07 M documentos a 4,38 M filas; con 5 M filas agrega más rápido que PostgreSQL (E1) y su tiempo crece menos que el volumen | PostgreSQL: cargar todo primero y competir con la carga operativa; MapReduce: escribe a disco en cada etapa |
| Lecturas en tiempo real | Continuo, alertas en segundos | **Spark Structured Streaming** → MongoDB | Ventanas por tiempo de evento con *watermark* y estado en checkpoint | Relanzar un batch cada minuto: latencia y duplicados |

## 4.4 Resultados de los experimentos

| ID | Experimento | Hipótesis | Resultado (mediana) | ¿Se cumplió? |
|---|---|---|---|---|
| E0 | Carga del mismo CSV de 5 M filas (255 MB) | — | `COPY` PG 9,6 s · `insert_many` Mongo 108 s · `hdfs put` 11,8 s · CSV→Parquet 10,2 s. Tamaño: PG 501 MB (tabla + índice), Mongo 132 MB + 112 MB de índices, Parquet 12 MB | — |
| E1 | Agregación promedio por zona-hora | PG/Mongo ganan en pequeño; Spark escala mejor | 100k: PG 48 ms · Mongo 135 ms · Spark 535 ms. 5 M: PG 1,00 s · Mongo 6,66 s · **Spark 0,84 s** | **Sí.** Spark pasa de ×11 más lento a ser el más rápido con 5 M |
| E2 | Últimas 100 lecturas de un sensor | Spark no sirve para consultas puntuales | 5 M: PG 0,7 ms · Mongo 2,4 ms · Spark 287 ms recorriendo el Parquet; **12,7 s** incluyendo el arranque del job (medido con 100k) | **Sí.** ×400 más lento sin arranque y ×18.000 con él |
| E3 | Join sensor-tipo-categoría-ubicación-organismo | PG superior en relaciones | 1 M: PG 0,58 s · Mongo `$lookup` por documento **237 s** · Mongo agrupando antes 0,93 s · Spark 0,53 s. 5 M: PG 3,52 s · Spark 0,91 s | **Sí frente a Mongo** (×410). Spark lo supera en volumen con *broadcast*, sin integridad |
| E4 | Tipo de sensor nuevo con campos nuevos | Mongo 0 migraciones | Mongo: 0 DDL, 10.000 documentos en 0,17 s. PG: `INSERT` del tipo + `ALTER TABLE`; columna anulable 0,8 ms, con valor por defecto volátil 4,2 s reescribiendo 1 M filas | **Sí**, con matiz: en PG el costo depende del diseño del cambio |
| E5 | Spark con 1 frente a 2 núcleos | Más recursos, menos tiempo | 100k: 480 → 535 ms (sin mejora) · 1 M: 573 → 540 ms · 5 M: 1,59 → 0,84 s (×1,9) | **Sí, solo con volumen.** Con poco dato domina el costo fijo |
| E6 | HDFS con un DataNode caído | Los datos siguen disponibles | Lectura completa con el mismo md5 en 1,8 s; nodo declarado muerto a los 97 s; re-replicado a los 152 s | **Sí** |
| E7 | Con y sin índice | Órdenes de magnitud en consultas selectivas | 5 M: PG 165 ms → 0,7 ms (**×236**, 45.052 → 48 bloques leídos); Mongo 2.221 ms → 2,4 ms (**×926**, 5.000.000 → 100 documentos examinados) | **Sí**, y la ganancia crece con el volumen (PG ×8 con 100k) |

<!-- incluir-imagen: ../resultados/img/e1.png -->
<!-- incluir-imagen: ../resultados/img/e3.png -->

**Por qué Spark ganó con 5 M filas en E1 y E3.** Spark leyó 12 MB de Parquet columnar y comprimido, mientras que
PostgreSQL recorrió 501 MB de tabla por filas. La comparación es motor **más formato de almacenamiento**, que es la
elección real que se hace en una arquitectura: el histórico analítico va en Parquet sobre HDFS y no en tablas
operativas. PostgreSQL recupera la ventaja con vistas materializadas (A2.5: ×3.900) cuando la consulta se conoce de
antemano.

## 4.5 ¿Qué pasa si los datos crecen ×100?

Proyección de nuestro dato a 500 M lecturas (~25 GB de CSV), con la tendencia medida entre 1 M y 5 M:

| Motor | Qué pasaría | Qué habría que hacer |
|---|---|---|
| PostgreSQL | La tabla pasaría de 0,5 GB a ~50 GB y dejaría de caber en memoria. La agregación de E1, que crece casi lineal, pasaría de ~1 s a minutos. Las consultas puntuales seguirían en milisegundos gracias a los índices (E7: el tiempo con índice no cambió entre 100k y 5 M) | Mantener en PostgreSQL solo lo limpio y agregado (como ya hacemos); particiones por mes con retención; vistas materializadas; réplicas de lectura |
| MongoDB | ~13 GB comprimidos más ~11 GB de índices: el conjunto de trabajo ya no cabe en la caché de un nodo. Las agregaciones de un solo hilo se volverían inviables (E1 ×100 ≈ 11 min) | Sharding por `sensor_id` + tiempo; colecciones de series temporales; TTL para mover lo antiguo a HDFS |
| HDFS | ~200 bloques de 128 MB por réplica; el NameNode guarda unos cientos de KB de metadata. Es el escenario para el que está diseñado | Agregar DataNodes (la capacidad crece lineal); mantener archivos grandes por año para no saturar el NameNode; Parquet en `curated` |
| Spark | Es el único cuyo tiempo creció menos que el volumen en E1 y E3. Con 1 worker serían minutos; con más workers se reparte el trabajo (E5: ×1,9 al duplicar núcleos con 5 M) | Más workers u otro clúster; particionar por `anio/mes` (poda de particiones) y *broadcast* de catálogos |
| MapReduce | Escala igual que HDFS, pero cada etapa escribe a disco: seguiría siendo más lento que Spark | Mantenerlo para conteos simples o reemplazarlo por Spark |

## 4.6 Conclusión

No hay una tecnología mejor en todo; hay una mejor **para cada necesidad**, y los experimentos lo muestran:

- **PostgreSQL** gana en lo puntual (E2: < 1 ms), en las relaciones con integridad (E3 frente a `$lookup`; demo de
  restricciones) y en volúmenes medios.
- **MongoDB** gana en flexibilidad: tipos y firmwares nuevos sin migraciones (E4) e ingesta de documentos variables,
  pero no es la herramienta para relacionar ni para recorrer todo el histórico (E1, E3).
- **HDFS** no consulta, **guarda**: crudos replicados que sobreviven a la caída de un nodo (E6) y que alimentan el
  reprocesamiento.
- **Spark** gana cuando el volumen crece (E1, E3, E5) y es el único que hace ETL, SQL, ML y streaming con la misma API,
  pero tiene un costo fijo que lo descarta para consultas interactivas (E2: 12,7 s con el arranque).

El valor está en la **combinación**: cada dato entra por el motor que lo sirve mejor y los resultados vuelven a donde
se consultan mejor (Spark → PostgreSQL, streaming → MongoDB).
