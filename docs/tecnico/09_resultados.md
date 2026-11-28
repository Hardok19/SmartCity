# 9. Resultados

Cifras obtenidas al ejecutar el proyecto completo en este entorno el 2026-11-27: OpenAQ (archivo público S3) y
Open-Meteo descargados, lecturas IoT generadas con la semilla 2026, carga en MongoDB, HDFS y PostgreSQL, ETL, análisis,
MapReduce y experimentos. La carga anterior de la pareja B (con la API de OpenAQ) dio cifras casi iguales
(1.068.777 lecturas y 4.389.705 filas Parquet).

## 9.1 Volúmenes alcanzados

| Requisito | Mínimo | Alcanzado |
|---|---|---|
| Registros en PostgreSQL | 100.000 | **2.632.112** en `medicion_horaria` (2.456.672 ambientales + 175.440 de tráfico y energía, cargados por Spark), más 87.720 agregados por zona-hora, 35.104 KPI y 104.137 anomalías |
| Documentos en MongoDB | 500.000 | **1.068.584** lecturas + 1.190 alertas + 1.428 eventos + 3.000 incidentes |
| Registros procesados por Spark | 1.000.000 | **4.380.538** filas en formato largo desde 1.068.584 documentos (y 4.397.164 desde los crudos de HDFS) |
| Total recomendado | > 5 M | ≈ 8,1 M de registros del proyecto (PG + Mongo + Parquet); los benchmarks agregan 6,1 M por motor |

Detalle por tecnología y tamaño en disco: [VOLUMENES.md](../datos/VOLUMENES.md).

## 9.2 PostgreSQL

- **17 consultas** (PG-Q01…Q10 obligatorias + PG-P01…P07 preguntas de ejemplo) corren sin error desde el dashboard y
  desde `pytest`.
- **¿Cuál zona registra mayor cantidad de vehículos?** (PG-P05): Zona Centro, con 5.943.738 vehículos en 2023–2024 y
  722 vehículos/h en promedio en días laborables (574 en fin de semana y feriados), seguida de Norte (4.158.440).
- **¿Cuál es la hora de mayor congestión?** (PG-P06, días laborables, hora de Londres, con la dimensión `calendario`):
  las 18 h (975 vehículos/h), luego 17 h (926) y 8 h (896). Coincide con el perfil de doble pico del generador de
  tráfico: es un supuesto de los datos simulados, no un hallazgo sobre Londres.
- **Inventario** (PG-Q03): 48 sensores activos y 2 inactivos; 2 nunca enviaron datos (PG-Q07 los distingue de los que
  dejaron de enviar).
- **¿Cuál zona presenta mayor contaminación?** (PG-P04): Zona Centro, PM2.5 medio 13,26 µg/m³ y pico de media
  móvil de 7 días de 50,6; la más limpia es Este (9,09). La racha más larga sobre el umbral diario también es de
  Centro: 18 días seguidos (2024-11-25 a 2024-12-12, PG-P07). La hora más contaminada es las 18 h en todas las zonas
  (PG-P03). **Temperatura promedio por zona** (PG-P01): de 11,29 °C (Centro) a 11,78 °C (Este).
- **Optimización** (A2.5, 2.632.112 filas): índice B-tree 114 ms → 0,40 ms (×283); poda de particiones ×10
  (255 → 25 ms); vista materializada 1.854 ms → 0,47 ms; BRIN ocupa 176 veces menos que B-tree (600 KB frente a
  103 MB) a cambio de ser algo más lento en un rango de 3 meses (51 frente a 42 ms)
  ([postgres_optimizacion.md](../resultados/postgres_optimizacion.md)).
- **Integridad:** `sql/demo/demo_defensa.sql` muestra el rechazo de una FK inválida, de un `CHECK` y de un borrado con
  `ON DELETE RESTRICT`.

## 9.3 MongoDB

- **15 consultas** (MG-Q01…Q10 + MG-P01…P05) con los mismos pipelines en `mongo/queries/*.js` y en el conector.
  MG-Q09 combina `$facet`, `$bucket`, `$dateTrunc`, `$setWindowFields` y `$lookup`; MG-Q05 usa `$geoNear`.
- **CRUD:** las 7 operaciones del enunciado (`insertOne`, `insertMany`, `find`, `findOne`, `updateOne`, `updateMany`,
  `deleteOne`) sobre `demo_crud`, probadas contra la base real.
- **Validador:** rechaza una lectura sin `sensor_id` y acepta una con campos nuevos.
- **Índices** (B2.5, `lectura` con 1.068.584 documentos, [mongo_indices.md](../resultados/mongo_indices.md)): las
  últimas 50 lecturas de un sensor examinan 50 documentos con `ix_sensor_ts` frente a 1.068.584 sin índice
  (533 ms → < 1 ms); las lecturas de una zona en un día, 1.550 frente a 1.068.584 (×227 en tiempo). En `alerta`
  (1.190 documentos) MongoDB prefiere recorrer la colección: el índice compuesto no sirve para un filtro que no
  incluye su primer campo. Con 5 M documentos (E7) la consulta por sensor fue ×926 más rápida.
- **Evolución de esquema** (E4): 10.000 documentos de un tipo nuevo, con campos nuevos, sin ninguna sentencia DDL.

## 9.4 Hadoop

- **HDFS** ([hdfs_demo.txt](../resultados/hdfs_demo.txt)): las operaciones del enunciado y además `du`, `stat` y
  `setrep`. El mismo archivo de 2,66 MB ocupa 1 bloque con 128 MB y 3 con bloques de 1 MB; cada bloque tiene 2 réplicas
  en DataNodes distintos.
- **Tolerancia a fallos** (E6): con `datanode2` apagado la lectura fue completa (mismo md5, 1,8 s); el nodo se declaró
  muerto a los 97 s y la re-replicación terminó a los 152 s.
- **MapReduce** sobre el export real (445 MB, 1.068.584 lecturas y 1.428 eventos): los 5 jobs terminan en YARN en
  17–26 s ([mapreduce_tiempos.csv](../resultados/mapreduce_tiempos.csv)). mr01 cuenta las mediciones de 38 sensores;
  mr02: el sensor con más lecturas es S-018 (34.990); mr03: temperatura promedio histórica 11,60 °C (407.677 lecturas;
  de 11,37 °C en Centro a 11,90 °C en Sur); mr04: 1.428 eventos, el mismo total que MongoDB, con Este a la cabeza (434,
  345 de contaminación alta); mr05: 4.929 combinaciones sensor-día-variable de marzo de 2024 con media, mínimo y
  máximo. Las pruebas unitarias comparan cada mapper y reducer con un cálculo independiente.

## 9.5 Spark

Detalle en [hallazgos_spark.md](../resultados/hallazgos_spark.md).

| Análisis | Resultado principal |
|---|---|
| Descriptivo | 85 grupos zona-indicador-origen; PM2.5 sintético en Zona Norte con promedio 11,79 y máximo 141,6 µg/m³ |
| Temporal | Evolución mensual de PM2.5 (163 filas) y curva diaria de temperatura (240 filas) |
| Correlacional | PM2.5–PM10 = 0,85 en Centro; PM2.5–viento = −0,20 (el viento dispersa) |
| Predictivo (MLlib) | Regresión lineal 2023 → 2024: R² de 0,02–0,05; con datos reales, peor que la línea base. El clima solo no predice el PM2.5 |
| Espacial | Zona más contaminada: Centro (13,07 µg/m³ en sensores sintéticos; 11,06 en la estación real), luego Norte (11,79) |
| Movilidad | Top 3 de incremento de PM2.5 en hora pico frente a valle: Norte +25,6 %, Sur +25,7 %, Oeste +24,5 % |
| Anomalías | 134.063 detecciones: 13.852 marcas de Mongo, 38.278 con \|z\| ≥ 3 y 81.933 fuera de la valla IQR; 29.926 las detectan z-score e IQR a la vez |
| Comparativo | Mayor incremento interanual de PM2.5: Norte +11,1 %, Sur +9,9 %, Oeste +7,0 % |

- **ETL:** 1.068.584 documentos → 4.380.538 mediciones en Parquet particionado por año y mes (15,5 MB); desde los
  crudos de HDFS, 4.957.616 mediciones de entrada → 4.397.164 válidas (11,3 % descartado o duplicado); tráfico y
  energía: 175.440 filas con 0 % descartado.
- **Spark → PostgreSQL:** 2.456.672 mediciones ambientales horarias + 175.440 de tráfico y energía, 87.720 agregados,
  35.104 KPI y 104.137 anomalías; conteo idéntico en ambos lados y 0 filas nuevas al re-ejecutar (idempotente; `spark_anomalia` se reemplaza completa en cada carga porque es la foto del último análisis).
- **Optimización** (B4.3, [spark_optimizacion.md](../resultados/spark_optimizacion.md)): *broadcast join* 46 % más
  rápido que sort-merge y AQE 39 % más rápido con 32 particiones de *shuffle*; la caché solo conviene si el subconjunto
  se reutiliza más de 2 veces (con la materialización, 24 % más lenta). La poda de particiones y Parquet no cambian el
  tiempo con datos tan pequeños, pero el plan lee 12 de 24 particiones y 2 de 4 columnas, y Parquet ocupa 17 veces
  menos que el CSV de la misma muestra.
- **Streaming:** con un episodio forzado en Zona Centro (PM2.5 ≈ 50–61 µg/m³), se generaron alertas **solo** para esa
  zona y sin duplicados en 10 micro-lotes.

## 9.6 Comparación tecnológica

Resumen de E0–E7 en [04_justificacion_tecnologica.md](04_justificacion_tecnologica.md) §4.4; resultados completos en
[benchmarks.md](../resultados/benchmarks.md). Lo esencial:

| Necesidad | Ganador medido | Cifra |
|---|---|---|
| Consulta puntual | PostgreSQL / MongoDB con índice | 0,7 ms / 2,4 ms con 5 M filas; Spark 287 ms (12,7 s con arranque) |
| Relacionar entidades | PostgreSQL (con integridad) | JOIN de 5 tablas sobre 1 M en 0,58 s; `$lookup` 237 s |
| Agregar todo el histórico | PostgreSQL hasta ~1 M; **Spark con 5 M** | 5 M: Spark 0,84 s · PG 1,00 s · Mongo 6,66 s |
| Esquema cambiante | MongoDB | 0 DDL; PG hasta 4,2 s de reescritura de tabla |
| Durabilidad ante fallos | HDFS | Lectura intacta con un nodo caído; auto-reparación en 152 s |
| Escalar con recursos | Spark | ×1,9 con el doble de núcleos y 5 M filas |
| Ingesta masiva | PostgreSQL `COPY` | 5 M filas en 9,6 s; MongoDB 108 s |

<!-- incluir-imagen: ../resultados/img/e1.png -->
<!-- incluir-imagen: ../resultados/img/e2.png -->
<!-- incluir-imagen: ../resultados/img/e3.png -->
<!-- incluir-imagen: ../resultados/img/e7.png -->

## 9.7 Dashboard

| Requisito del enunciado | Cumplimiento |
|---|---|
| Vista principal única | «Vista principal» con secciones Ambiente, IoT, Movilidad, Energía, gráficos, mapa, tabla y eventos |
| KPIs | Ambiente 5/5 (+ CO₂), IoT 6/6, Energía 5/5, Movilidad 4/5 + «tiempo promedio de recorrido» = sin dato (ninguna fuente lo mide) |
| 4 gráficos | Serie temporal, comparativa por zonas, distribución por tipo de sensor y **Top 3 zonas con mayor incremento de PM2.5 en hora pico (Spark)**, con botón «Recalcular» |
| Filtros | Fechas y perfil horario, zona, tipo de medición (obligatorios) + sensor, tipo de evento, fuente y rango de valores |
| Tabla y mapa | Tabla con búsqueda y paginación; mapa pydeck con puntos por sensor y capa de calor de PM2.5 |
| Etiqueta de origen | En cada KPI, gráfico, tabla y mapa, con el color de la tecnología |
| Consultas por tecnología | PostgreSQL (17 + EXPLAIN ANALYZE), MongoDB (15 + CRUD), HDFS (operaciones, fsck, 5 MapReduce), Spark (10 + 8 análisis) |
| Extras | Página de streaming en vivo y página de comparación tecnológica con los gráficos de E1–E7 |

## 9.8 Reproducibilidad

Todas las cifras de esta sección salen de scripts del repositorio que escriben sus resultados en `docs/resultados/`
(ver el README, «Instalación desde cero»). Los datos reales se descargan sin clave (`descargar_openmeteo.py`,
`descargar_openaq_archivo.py`) y los sintéticos usan la semilla 2026, así que otra máquina obtiene los mismos conteos.
