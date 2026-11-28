# Hallazgos y resultados de Apache Spark — SmartCity

**Curso/proyecto:** SmartCity, proyecto II semestre 2026
**Responsable:** Pareja B — MongoDB y Apache Spark
**Período de los datos:** 2023–2024
**Fuente de las cifras:** salidas de `spark-submit` de la ejecución completa del 2026-11-27 en este entorno (OpenAQ del archivo público S3, Open-Meteo, generador con semilla 2026); cifras comprobadas a nivel funcional, no auditoría independiente de las fuentes de origen.
**Estado:** 10 consultas Spark SQL ejecutadas (10/10); 8 rutinas de B4.2 ejecutadas (8/8); resultados cargados en PostgreSQL; limitaciones al final.

## 1. Arquitectura y volumen procesado

El flujo probado consiste en leer documentos de MongoDB (`smartcity_db.lectura`), transformarlos mediante Spark DataFrames y almacenar las mediciones resultantes en HDFS como Parquet particionado por año y mes. Los datos generados provienen de una combinación de insumos de OpenAQ, Open-Meteo y simulación SmartCity, identificados mediante el campo `origen`.

| Indicador | Resultado |
|---|---:|
| Documentos de MongoDB procesados por el ETL | 1.068.584 |
| Filas resultantes del ETL | 4.380.538 |
| Sensores con mediciones | 38 |
| Zonas de estudio | 5 |
| Período | 2023–2024 |
| Espacio lógico de Parquet ambiental en HDFS, sin contar réplica | 15,5 MB |

**Ruta principal:** `hdfs://namenode:8020/smartcity/curated/ambiente`.
**Ruta de resultados:** `hdfs://namenode:8020/smartcity/analytics/`.
**Ruta del reporte Spark SQL SP-Q10:** `hdfs://namenode:8020/smartcity/analytics/reporte_q10`.

## 2. Análisis descriptivo

Se generaron **85 grupos** con cantidad de mediciones, sensores activos, media, desviación estándar, mínimo y máximo por zona, indicador y procedencia. Como ejemplo, para PM2.5 sintético en Zona Norte se obtuvo un promedio de **11,786** y un máximo de **141,6**. Los valores extremos justifican complementar el promedio con análisis de dispersión y anomalías.

**Salida:** `analitica/descriptivo`.

## 3. Análisis temporal

Se generaron **163 registros** de promedios mensuales de PM2.5 y **240 registros** del perfil de temperatura por hora del día. Se mantuvo la separación por origen para no combinar los datos de OpenAQ, Open-Meteo y datos sintéticos sin advertencia.

Ejemplo, Zona Centro en enero de 2023: PM2.5 de origen OpenAQ **13,896**, frente a PM2.5 sintético **14,642**. En el mismo mes, la comparación entre orígenes requiere considerar que corresponden a sensores distintos.

**Salidas:** `analitica/temporal_pm25_mensual`, `analitica/temporal_temperatura_hora`.

## 4. Análisis correlacional

Se calcularon **210 filas** de correlaciones de Pearson para seis variables (PM2.5, PM10, temperatura, humedad, precipitación y viento), por zona y grupo de procedencia. Las observaciones se emparejan por zona y hora antes de correlacionarlas.

| Relación | Zona Centro, fuentes externas | Zona Este, fuentes externas |
|---|---:|---:|
| PM2.5 – PM10 | 0,8532 | 0,3651 |
| PM2.5 – viento | -0,2014 | -0,1793 |
| PM2.5 – temperatura | 0,0564 | 0,0013 |

Los coeficientes sugieren una relación positiva fuerte entre PM2.5 y PM10 en Centro y una relación negativa débil entre PM2.5 y viento en ambas zonas. **Correlación no demuestra causalidad.** Para PM2.5 externo, Norte, Sur y Oeste carecen de pares de observaciones en el período; los valores `NULL` son una limitación de cobertura.

**Salida:** `analitica/correlacional_matriz`.

## 5. Análisis predictivo (Spark MLlib)

Se entrenaron **siete modelos** de regresión lineal sobre promedios diarios: entrenamiento en **2023** y prueba posterior en **2024**. Se produjeron **2.355 estimaciones** de 2024. Se evaluó el error contra una línea base que predice el promedio de entrenamiento.

| Zona | Procedencia | RMSE | R² | Mejora sobre baseline |
|---|---|---:|---:|---|
| Centro | Fuentes externas | 4,418 | -0,144 | No (baseline 4,209) |
| Este | Fuentes externas | 7,950 | -0,692 | No (baseline 6,205) |
| Centro | Sintético | 9,378 | 0,023 | Sí, marginal |
| Este | Sintético | 5,456 | 0,010 | Sí, marginal |
| Norte | Sintético | 6,304 | 0,018 | Sí, marginal |
| Oeste | Sintético | 4,877 | 0,045 | Sí, marginal |
| Sur | Sintético | 7,023 | 0,022 | Sí, marginal |

Los dos modelos con fuentes externas tienen peores errores que la línea base, y los modelos sintéticos muestran un poder explicativo bajo. **No se considera que los modelos sean predictores precisos.** Las predicciones de 2024 emplearon el clima observado en 2024; no son un pronóstico futuro operativo.

**Salidas:** `analitica/predictivo_metricas`, `analitica/predictivo_estimaciones_2024`.

## 6. Análisis espacial

El análisis consideró **608.534 mediciones** de PM2.5. Se generaron **10 filas por zona y origen** (incluyendo combinaciones sin observaciones) y **20 filas por sensor y origen**. Las agregaciones por zona y sensor reproducen exactamente las 608.534 lecturas, sin sensores con coordenadas faltantes ni zonas inconsistentes.

Promedio de PM2.5 por zona para sensores sintéticos: Centro **13,066**, Norte **11,786**, Sur **10,725**, Este **9,247**, Oeste **8,946**. En el conjunto identificado como OpenAQ hay observaciones de PM2.5 únicamente en Centro (**8.523**, promedio 11,056) y Este (**9.593**, promedio 9,345). No se deben interpolar ni inventar valores para las tres zonas faltantes.

**Salidas:** `analitica/espacial_zonas_pm25`, `analitica/espacial_sensores_pm25`.

## 7. Análisis exploratorio de horarios asociados a movilidad

Se generaron **336 filas** de perfil horario, **7 comparaciones** de franjas pico frente a valle nocturno y un **Top 3**. Para días laborables se compararon 07:00–09:59 y 17:00–19:59 contra 02:00–05:59 (hora local de Londres); son franjas definidas previamente y no medidas mediante datos de tráfico.

| Zona | Procedencia | Diferencia PM2.5 pico – valle | Variación relativa |
|---|---|---:|---:|
| Norte | Sintético | +2,910 | +25,62 % |
| Sur | Sintético | +2,648 | +25,68 % |
| Oeste | Sintético | +2,120 | +24,45 % |
| Centro | OpenAQ | -0,238 | -2,11 % |
| Este | OpenAQ | +0,264 | +2,86 % |

**Advertencia metodológica esencial:** estas cifras describen la diferencia ambiental entre horarios, **no identifican niveles, aforos ni horas de mayor tráfico real**. El requisito de movilidad con datos de tráfico queda pendiente si así lo exige la revisión docente.

**Salidas:** `analitica/movilidad_proxy_perfil_horario`, `analitica/movilidad_proxy_pico_valle`, `analitica/movilidad_proxy_top3_pico_valle`.

## 8. Detección de anomalías

Se registraron **134.063 detecciones** con tres métodos: **13.852** documentos MongoDB marcados como inválidos por la limpieza, **38.278** mediciones del Parquet curado con `|Z| >= 3` y **81.933** fuera de la valla IQR de su sensor. **29.926** mediciones las detectan a la vez Z-score e IQR. Se generaron **273 filas de resumen** y rankings para **38 sensores**. Ambas poblaciones se procesan separadamente: los documentos excluidos por la limpieza se examinan desde MongoDB; el Z-score se calcula sobre mediciones retenidas en HDFS.

Los sensores con más detecciones (los tres métodos) fueron S-018 (7.637), S-017 (6.742), S-019 (6.701), S-030 (6.275) y S-032 (6.164). El conteo corresponde a **detecciones de métodos**, no a incidentes independientes verificados. Valores extremos requieren revisión y no prueban por sí mismos una avería del sensor.

**Salidas:** `analitica/anomalias_detalle`, `analitica/anomalias_resumen`, `analitica/anomalias_sensores`.

**IQR por sensor:** `metodo_deteccion = "iqr"` marca un valor < Q1 − 1,5·IQR o > Q3 + 1,5·IQR de su sensor e indicador (con al menos 30 valores e IQR > 0). Detecta más del doble que Z-score (81.933 frente a 38.278) porque la valla IQR no depende de la desviación estándar, que los propios picos inflan; el 78 % de lo que detecta Z-score también lo detecta IQR (29.926, `SMARTCITY_B42_ANOMALIAS_ZSCORE_E_IQR`). IQR usa percentiles exactos (`percentile`), así que el conteo es el mismo en cada ejecución (con `percentile_approx` variaba unas décimas de por mil entre corridas).

## 9. Análisis comparativo por zona y año

Se obtuvieron **160 filas** de perfil anual por indicador, **10 filas** para comparación 2023–2024 de PM2.5 por zona y origen, y un **Top 3** de incrementos para las zonas sintéticas.

| Zona (sintético) | PM2.5 2023 | PM2.5 2024 | Cambio |
|---|---:|---:|---:|
| Norte | 11,168 | 12,402 | +11,05 % |
| Sur | 10,221 | 11,229 | +9,86 % |
| Oeste | 8,605 | 9,205 | +6,97 % |
| Centro | 12,768 | 13,362 | +4,65 % |
| Este | 9,528 | 9,024 | -5,29 % |

Para OpenAQ, Centro pasó de **11,387** a **10,745** (-5,64 %), y Este de **9,732** a **9,110** (-6,39 %). Las coberturas son desiguales: Centro **246 días en 2023 / 264 en 2024**, Este **186 / 301**. Estas diferencias no constituyen por sí solas evidencia de una mejoría de calidad del aire, porque pueden reflejar sesgo de muestreo.

**Salidas:** `analitica/comparativo_perfil_anual`, `analitica/comparativo_pm25_2023_2024`, `analitica/comparativo_top3_incremento_interanual`.

## 10. Spark SQL y reporte consolidado

Las consultas **SP-Q01 a SP-Q10** se ejecutaron con éxito individualmente. SP-Q10 consolidó **4.380.538 mediciones**, **38 sensores** y cinco zonas, sin mediciones de origen desconocido, y generó un Parquet de reporte con archivo `_SUCCESS`. El resultado de cada consulta queda en HDFS (`consultas_spark/SP-Qxx`); los KPI por zona y día llegan a PostgreSQL con `carga_pg.py` (`spark_kpi_zona`, 35.104 filas).

## 11. Limitaciones y siguientes tareas

1. **Trazabilidad de fuentes:** diferenciar siempre `openaq`, `open-meteo` y `sintetico`; una etiqueta de procedencia no sustituye una auditoría de calidad independiente de cada valor. El generador puede introducir valores extraordinarios en las series producidas para SmartCity.
2. **Cobertura histórica:** PM2.5 proveniente de OpenAQ solo está disponible en Centro y Este para el período analizado. Las zonas restantes utilizan PM2.5 sintético en estas pruebas.
3. **Movilidad:** el análisis de horarios pico es un proxy ambiental; falta un conjunto de datos de tráfico si se desea responder literalmente a la pregunta de circulación vehicular.
4. **Anomalías:** ✅ IQR por sensor ejecutado (§8). Las detecciones son de métodos, no incidentes verificados.
5. **Integración PostgreSQL:** ✅ `spark/jobs/carga_pg.py` carga en las tablas de la pareja A con staging + `INSERT … ON CONFLICT` (idempotente, conteo Spark = PostgreSQL). Cargado el 2026-11-27: `medicion_horaria` 2.456.672 filas ambientales + 175.440 de tráfico y energía (2.632.112 en total), `agg_ambiente_hora_zona` 87.720, `spark_kpi_zona` 35.104 y `spark_anomalia` 104.137 (se reemplaza completa en cada carga); vistas materializadas refrescadas.
6. **Optimización B4.3:** programa `optimizar_spark.py` y metodología en `docs/resultados/spark_optimizacion.md`; faltan las cifras de una ejecución y las capturas de la Spark UI.
7. **Dashboard B5/D:** ✅ las consultas SP-Q01…Q10 y los 8 análisis se ejecutan y visualizan desde «Consultas por tecnología»; el Top 3 de hora pico es el gráfico Spark de la vista principal.
8. **Reproducibilidad:** fijar dependencias NumPy/SciPy en la imagen Spark de Docker; las instalaciones manuales dentro del contenedor se pierden si se recrea.
9. **Rutas finales:** ✅ desde el 2026-11-27 el ETL escribe en `/smartcity/curated/` y los análisis y consultas en `/smartcity/analytics/` (`spark/jobs/rutas.py`); `sandbox/` queda para pruebas y el experimento B4.3.

**Estado de entrega:** evidencia funcional de diez consultas y ocho rutinas de análisis; no equivale todavía a completar todas las integraciones, optimizaciones y requisitos de datos del proyecto.
