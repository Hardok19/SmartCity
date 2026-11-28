# 5. Modelo relacional (PostgreSQL)

## 5.1 Qué datos van a PostgreSQL y por qué

PostgreSQL guarda los datos **estructurados y estables** cuya consistencia importa: el catálogo de sensores, zonas, organismos, usuarios, categorías y tipos, más los **resultados limpios y agregados** que produce Spark. Las razones:

- **Integridad referencial:** un sensor no puede apuntar a una zona, un tipo o un organismo inexistente, y no se puede borrar un organismo que todavía tiene sensores (`ON DELETE RESTRICT`). MongoDB no hace cumplir estas reglas.
- **Restricciones declarativas:** `CHECK`, `NOT NULL` y `UNIQUE` (coordenadas en rango, estado ∈ {activo, inactivo}, correo con formato válido, fecha de baja solo para sensores inactivos). Ningún dato malo entra al catálogo, venga de donde venga.
- **Consultas relacionales y analíticas sobre datos limpios:** JOIN entre 4–5 tablas, CTE (incluida una recursiva), funciones de ventana, vistas materializadas e índices B-tree/BRIN con el planificador de costos. El dashboard las consulta en milisegundos (A2.5).
- **Transacciones:** la carga desde Spark (B3.4) es atómica: o entra todo el lote o nada.

Lo que **no** va a PostgreSQL: las lecturas crudas de esquema variable (MongoDB) y el histórico masivo crudo (HDFS). Así la base relacional se mantiene pequeña, consistente y rápida.

Diagrama E-R: [docs/diagramas/er_postgres.md](../diagramas/er_postgres.md).

## 5.2 Normalización hasta 3FN

| Tabla | 1FN | 2FN | 3FN |
|---|---|---|---|
| `ubicaciones` | Atributos atómicos; las coordenadas son dos columnas, no un texto «lat,lon». | PK simple (`id_ubicacion`): no hay dependencias parciales. | `distrito` y el centroide dependen de `nombre_zona`, pero `nombre_zona` es **clave candidata** (`UNIQUE`). Una dependencia que pasa por una clave candidata no viola 3FN. |
| `categorias_sensor` | Sí | PK simple | Sí (solo el nombre) |
| `tipos_sensor` | Sí | PK simple | `id_categoria` depende solo del tipo. |
| `sensores` | Sí | PK simple | **Corrección aplicada:** antes la tabla guardaba `tipo_sensor` **e** `id_categoria`. Como el tipo determina la categoría (`calidad_aire → Calidad del aire`), se formaba la dependencia transitiva `id_sensor → tipo_sensor → id_categoria`. Se movió la categoría a `tipos_sensor` y `sensores` solo referencia el tipo (`004_restricciones.sql` migra las bases creadas antes). Las demás columnas (estado, fechas, origen, posición propia) describen al sensor y no a otra entidad. |
| `organismos` | Sí | PK simple | `responsable` es un atributo del organismo. |
| `usuarios` | Sí | PK simple | `id_organismo` es una FK y no se repiten datos del organismo. |
| `medicion_horaria` | Sí (un valor por fila: formato largo, no una columna por variable) | PK compuesta (`id_sensor, timestamp, variable`). `valor` y `origen` dependen de la PK completa. | La zona **no** se guarda: se obtiene del sensor por JOIN. Guardarla repetiría un dato que depende de `id_sensor` y no de la PK completa. |
| `calendario` | Sí | PK simple (`fecha`) | `anio, mes, dia_semana…` se derivan de la fecha. Es una dimensión de solo lectura que se materializa para facilitar las consultas sobre fechas (feriados y fines de semana). |
| Tablas `spark_*` y `agg_*` | Sí | PK compuestas | Son **resultados agregados** (no se editan): se guardan precalculados a propósito, como en un *data mart*, para que el dashboard no recalcule. |

## 5.3 Llaves, restricciones e índices

- **PK:** todas las tablas. `id_sensor` es un código natural (`S-001`) porque es el mismo identificador en MongoDB, HDFS y Spark: así se pueden unir las cuatro tecnologías.
- **FK:** las 10 relaciones usan `ON DELETE RESTRICT` (`004_restricciones.sql`).
- **CHECK:** latitud y longitud en rango, `fecha_instalacion <= CURRENT_DATE`, `fecha_baja >= fecha_instalacion` y solo si el sensor está inactivo, `frecuencia_min > 0`, `origen` y `estado` en listas cerradas, formato de correo, humedad entre 0 y 100, valores no negativos (salvo temperatura) y no NaN.
- **Índices** ([005_indices.sql](../../sql/ddl/005_indices.sql), cada uno con su justificación): índices en todas las FK (PostgreSQL no los crea solo); el compuesto `(id_sensor, timestamp DESC)` para «últimas lecturas»; `(variable, timestamp DESC)`; **BRIN** sobre `timestamp`, porque los datos se insertan en orden temporal; y el **parcial** `tipo_sensor WHERE estado = 'activo'`.
- **Particionamiento:** `medicion_horaria PARTITION BY RANGE (timestamp)`, una partición por mes (`fn_crear_particiones`) más `DEFAULT`. Una consulta de un mes lee solo esa partición (el `EXPLAIN` muestra una sola `medicion_horaria_AAAA_MM`). Además, borrar o archivar un mes es un `DROP TABLE` instantáneo, no un `DELETE` masivo.

## 5.4 Vistas, funciones y procedimientos

| Objeto | Uso |
|---|---|
| `v_sensor_detalle` | Sensor + tipo + categoría + zona + organismo (PG-Q09, mapa del dashboard) |
| `mv_actividad_sensor` | Mediciones, primera y última lectura por sensor (PG-Q08, `fn_sensores_sin_actividad`) |
| `mv_kpi_diario_zona` | PM2.5, temperatura y humedad diarios por zona (PG-Q10, PG-P04, PG-P07): 0,47 ms frente a 1.854 ms de la consulta en vivo (×3.900, A2.5) |
| `fn_sensores_por_organismo()` | Conteo por organismo, incluidos los que no tienen sensores |
| `fn_sensores_sin_actividad(desde)` | Distingue «nunca envió datos» de «dejó de enviar» |
| `fn_kpi_ambiente(desde, hasta, zona)` | KPI de ambiente por período (PG-Q10) |
| `fn_crear_particiones(desde, hasta)` | Crea las particiones mensuales que falten |
| `sp_refrescar_mv()` | `REFRESH MATERIALIZED VIEW CONCURRENTLY` de las dos MV. Lo llama Spark al final de cada carga |

## 5.5 Consultas

PG-Q01…Q10 (obligatorias) y PG-P01…P07 (preguntas del enunciado) están en [sql/queries/](../../sql/queries/). Cada archivo trae una cabecera con id, título, técnicas y parámetros. El dashboard las ejecuta desde `conectores_postgres_hadoop.CONSULTAS_PG`, con un botón de `EXPLAIN ANALYZE`.

| Técnica exigida | Dónde |
|---|---|
| SELECT, WHERE, ORDER BY, DISTINCT | Q02, Q03 (`COUNT(DISTINCT)`), Q09 (`STRING_AGG(DISTINCT)`) |
| INNER / LEFT JOIN | Q02 (INNER), Q04 y Q08 (LEFT) |
| GROUP BY, HAVING, COUNT, SUM, AVG, MIN, MAX | Q01, Q05 (HAVING), P01 (AVG/MIN/MAX), P05 (SUM) |
| Subconsultas | Q05 (subconsulta en HAVING), Q07 (`NOT EXISTS`), P07 |
| CTE | Q06, Q08, P03, P04, P06; **recursiva** en P07 (rachas de días consecutivos) |
| Funciones de ventana | Q06 (`SUM OVER` acumulado), Q08 (`RANK`, `SUM OVER ()`), P03 (`ROW_NUMBER`), P04 (media móvil con `AVG OVER (ROWS BETWEEN 6 PRECEDING…)` y `LAG`) |
| Consultas sobre fechas | P01–P07: rangos, `EXTRACT(HOUR … AT TIME ZONE 'Europe/London')`, JOIN con `calendario` (laborables y feriados) |
| Consultas que usan índices | Q07 y `lecturas_pg` (índice compuesto), P01–P06 (poda de particiones). Evidencia en [postgres_optimizacion.md](../resultados/postgres_optimizacion.md) |
| Vistas / funciones | Q09 (vista), Q10 (función), P02 (función) |
| `generate_series`, `FILTER`, `ROLLUP` | Q06, Q03 |
