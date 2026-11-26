# Hallazgos y resultados de Apache Spark — SmartCity

**Curso/proyecto:** SmartCity, proyecto II semestre 2026
**Responsable:** Pareja B — MongoDB y Apache Spark
**Período de los datos:** 2023–2024
**Fuente de las cifras:** salidas de `spark-submit` compartidas durante las pruebas; cifras comprobadas a nivel funcional, no auditoría independiente de las fuentes de origen.
**Estado:** 10 consultas Spark SQL ejecutadas; 8 rutinas de B4.2 ejecutadas, con limitaciones descritas al final.

## 1. Arquitectura y volumen procesado

El flujo probado consiste en leer documentos de MongoDB (`smartcity_db.lectura`), transformarlos mediante Spark DataFrames y almacenar las mediciones resultantes en HDFS como Parquet particionado por año y mes. Los datos generados provienen de una combinación de insumos de OpenAQ, Open-Meteo y simulación SmartCity, identificados mediante el campo `origen`.

| Indicador | Resultado |
|---|---:|
| Documentos de MongoDB procesados por el ETL | 1.068.777 |
| Filas resultantes del ETL | 4.389.705 |
| Sensores con mediciones | 38 |
| Zonas de estudio | 5 |
| Período | 2023–2024 |
| Espacio lógico de Parquet ambiental en HDFS, sin contar réplica | ~16 MB |

**Ruta principal:** `hdfs://namenode:8020/smartcity/sandbox/etl_ambiente_completa`.
**Ruta de resultados:** `hdfs://namenode:8020/smartcity/sandbox/analitica/`.
**Ruta del reporte Spark SQL SP-Q10:** `hdfs://namenode:8020/smartcity/sandbox/reporte_spark_q10`.

## 2. Análisis descriptivo

Se generaron **85 grupos** con cantidad de mediciones, sensores activos, media, desviación estándar, mínimo y máximo por zona, indicador y procedencia. Como ejemplo, para PM2.5 sintético en Zona Norte se obtuvo un promedio de **11,786** y un máximo de **141,6**. Los valores extremos justifican complementar el promedio con análisis de dispersión y anomalías.

**Salida:** `analitica/descriptivo`.

## 3. Análisis temporal

Se generaron **164 registros** de promedios mensuales de PM2.5 y **240 registros** del perfil de temperatura por hora del día. Se mantuvo la separación por origen para no combinar los datos de OpenAQ, Open-Meteo y datos sintéticos sin advertencia.

Ejemplo, Zona Centro en enero de 2023: PM2.5 de origen OpenAQ **13,896**, frente a PM2.5 sintético **14,642**. En el mismo mes, la comparación entre orígenes requiere considerar que corresponden a sensores distintos.

**Salidas:** `analitica/temporal_pm25_mensual`, `analitica/temporal_temperatura_hora`.

## 4. Análisis correlacional

Se calcularon **210 filas** de correlaciones de Pearson para seis variables (PM2.5, PM10, temperatura, humedad, precipitación y viento), por zona y grupo de procedencia. Las observaciones se emparejan por zona y hora antes de correlacionarlas.

| Relación | Zona Centro, fuentes externas | Zona Este, fuentes externas |
|---|---:|---:|
| PM2.5 – PM10 | 0,8480 | 0,3727 |
| PM2.5 – viento | -0,1943 | -0,1724 |
| PM2.5 – temperatura | 0,0598 | 0,0078 |

Los coeficientes sugieren una relación positiva fuerte entre PM2.5 y PM10 en Centro y una relación negativa débil entre PM2.5 y viento en ambas zonas. **Correlación no demuestra causalidad.** Para PM2.5 externo, Norte, Sur y Oeste carecen de pares de observaciones en el período; los valores `NULL` son una limitación de cobertura.

**Salida:** `analitica/correlacional_matriz`.

## 5. Análisis predictivo (Spark MLlib)

Se entrenaron **siete modelos** de regresión lineal sobre promedios diarios: entrenamiento en **2023** y prueba posterior en **2024**. Se produjeron **2.382 estimaciones** de 2024. Se evaluó el error contra una línea base que predice el promedio de entrenamiento.

| Zona | Procedencia | RMSE | R² | Mejora sobre baseline |
|---|---|---:|---:|---|
| Centro | Fuentes externas | 4,264 | -0,136 | No |
| Este | Fuentes externas | 7,845 | -0,607 | No |
| Centro | Sintético | 9,386 | 0,025 | Sí, marginal |
| Este | Sintético | 5,260 | 0,029 | Sí, marginal |
| Norte | Sintético | 6,304 | 0,018 | Sí, marginal |
| Oeste | Sintético | 4,877 | 0,045 | Sí, marginal |
| Sur | Sintético | 7,023 | 0,022 | Sí, marginal |

Los dos modelos con fuentes externas tienen peores errores que la línea base, y los modelos sintéticos muestran un poder explicativo bajo. **No se considera que los modelos sean predictores precisos.** Las predicciones de 2024 emplearon el clima observado en 2024; no son un pronóstico futuro operativo.

**Salidas:** `analitica/predictivo_metricas`, `analitica/predictivo_estimaciones_2024`.

## 6. Análisis espacial

El análisis consideró **610.018 mediciones** de PM2.5. Se generaron **10 filas por zona y origen** (incluyendo combinaciones sin observaciones) y **20 filas por sensor y origen**. Las agregaciones por zona y sensor reproducen exactamente las 610.018 lecturas, sin sensores con coordenadas faltantes ni zonas inconsistentes.

Promedio de PM2.5 por zona para sensores sintéticos: Centro **13,076**, Norte **11,786**, Sur **10,725**, Este **9,380**, Oeste **8,946**. En el conjunto identificado como OpenAQ hay observaciones de PM2.5 únicamente en Centro (**10.020**) y Este (**9.580**). No se deben interpolar ni inventar valores para las tres zonas faltantes.

**Salidas:** `analitica/espacial_zonas_pm25`, `analitica/espacial_sensores_pm25`.

## 7. Análisis exploratorio de horarios asociados a movilidad

Se generaron **336 filas** de perfil horario, **7 comparaciones** de franjas pico frente a valle nocturno y un **Top 3**. Para días laborables se compararon 07:00–09:59 y 17:00–19:59 contra 02:00–05:59 (hora local de Londres); son franjas definidas previamente y no medidas mediante datos de tráfico.

| Zona | Procedencia | Diferencia PM2.5 pico – valle | Variación relativa |
|---|---|---:|---:|
| Norte | Sintético | +2,910 | +25,62 % |
| Sur | Sintético | +2,648 | +25,68 % |
| Oeste | Sintético | +2,120 | +24,45 % |
| Centro | OpenAQ | -0,199 | -1,80 % |
| Este | OpenAQ | +0,397 | +4,30 % |

**Advertencia metodológica esencial:** estas cifras describen la diferencia ambiental entre horarios, **no identifican niveles, aforos ni horas de mayor tráfico real**. El requisito de movilidad con datos de tráfico queda pendiente si así lo exige la revisión docente.

**Salidas:** `analitica/movilidad_proxy_perfil_horario`, `analitica/movilidad_proxy_pico_valle`, `analitica/movilidad_proxy_top3_pico_valle`.

## 8. Detección de anomalías

Se registraron **49.255 detecciones**: **10.762** casos marcados en documentos MongoDB y **38.493** mediciones del Parquet curado con `|Z| >= 3`. Se generaron **120 filas de resumen** y rankings para **38 sensores**. Ambas poblaciones se procesan separadamente: los documentos excluidos por la limpieza se examinan desde MongoDB; el Z-score se calcula sobre mediciones retenidas en HDFS.

Los sensores con más detecciones fueron S-018 (2.304), S-017 (2.089), S-019 (2.061), S-038 (1.926) y S-036 (1.916). El conteo corresponde a **detecciones de métodos**, no a incidentes independientes verificados. Valores extremos requieren revisión y no prueban por sí mismos una avería del sensor.

**Salidas:** `analitica/anomalias_detalle`, `analitica/anomalias_resumen`, `analitica/anomalias_sensores`.

**Pendiente según TODO:** complementar el análisis con método **IQR por sensor** y contrastar los resultados contra Z-score.

## 9. Análisis comparativo por zona y año

Se obtuvieron **160 filas** de perfil anual por indicador, **10 filas** para comparación 2023–2024 de PM2.5 por zona y origen, y un **Top 3** de incrementos para las zonas sintéticas.

| Zona (sintético) | PM2.5 2023 | PM2.5 2024 | Cambio |
|---|---:|---:|---:|
| Norte | 11,168 | 12,402 | +11,05 % |
| Sur | 10,221 | 11,229 | +9,86 % |
| Oeste | 8,605 | 9,205 | +6,97 % |
| Centro | 12,786 | 13,365 | +4,53 % |
| Este | 9,391 | 9,372 | -0,20 % |

Para OpenAQ, Centro pasó de **11,413** a **10,531** (-7,73 %), y Este de **9,810** a **9,096** (-7,28 %). Las coberturas son desiguales: Centro **263 días en 2023 / 277 en 2024**, Este **169 / 302**. Estas diferencias no constituyen por sí solas evidencia de una mejoría de calidad del aire, porque pueden reflejar sesgo de muestreo.

**Salidas:** `analitica/comparativo_perfil_anual`, `analitica/comparativo_pm25_2023_2024`, `analitica/comparativo_top3_incremento_interanual`.

## 10. Spark SQL y reporte consolidado

Las consultas **SP-Q01 a SP-Q10** se ejecutaron con éxito individualmente. SP-Q10 consolidó **4.389.705 mediciones**, **38 sensores** y cinco zonas, sin mediciones de origen desconocido, y generó un Parquet de reporte con archivo `_SUCCESS`. No obstante, actualmente la prueba de exportación se dirige a HDFS `sandbox`, no a la tabla final `spark_kpi_zona` de PostgreSQL.

## 11. Limitaciones y siguientes tareas

1. **Trazabilidad de fuentes:** diferenciar siempre `openaq`, `open-meteo` y `sintetico`; una etiqueta de procedencia no sustituye una auditoría de calidad independiente de cada valor. El generador puede introducir valores extraordinarios en las series producidas para SmartCity.
2. **Cobertura histórica:** PM2.5 proveniente de OpenAQ solo está disponible en Centro y Este para el período analizado. Las zonas restantes utilizan PM2.5 sintético en estas pruebas.
3. **Movilidad:** el análisis de horarios pico es un proxy ambiental; falta un conjunto de datos de tráfico si se desea responder literalmente a la pregunta de circulación vehicular.
4. **Anomalías:** implementar y contrastar **IQR por sensor** además de Z-score y marcas.
5. **Integración PostgreSQL:** comprobar las tablas reales de la pareja A y construir `spark/jobs/carga_pg.py`, con inserción idempotente y conteos verificables. No inventar esquemas antes de inspeccionarlos.
6. **Optimización B4.3:** comparar CSV/Parquet, mostrar `PartitionFilters` en `explain()`, broadcast JOIN y uso de caché/AQE; documentar tiempos y configuración en `docs/resultados/spark_optimizacion.md`.
7. **Dashboard B5/D:** conectar consultas y resultados almacenados a la interfaz Streamlit; ejecutar las consultas por tecnología desde ella.
8. **Reproducibilidad:** fijar dependencias NumPy/SciPy en la imagen Spark de Docker; las instalaciones manuales dentro del contenedor se pierden si se recrea.
9. **Rutas finales:** el prototipo utiliza `sandbox`; antes de la entrega acordar con el equipo las rutas oficiales bajo `/smartcity/curated/` y `/smartcity/analytics/`.

**Estado de entrega:** evidencia funcional de diez consultas y ocho rutinas de análisis; no equivale todavía a completar todas las integraciones, optimizaciones y requisitos de datos del proyecto.
