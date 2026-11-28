# Interfaz entre los conectores y el dashboard

El dashboard (`streamlit run dashboard.py`) no contiene consultas: llama funciones de dos módulos.

| Módulo | Pareja | Estado |
|---|---|---|
| `conectores_mongo_spark.py` | B (MongoDB + Spark) | Implementado |
| `conectores_postgres_hadoop.py` | A (PostgreSQL + HDFS + MapReduce) | Implementado. `conectores_postgres.py` queda como capa de compatibilidad con la versión anterior |

[dashboard_app/fuentes.py](../../dashboard_app/fuentes.py) importa el módulo A si existe. Cada sección comprueba si la función que necesita está definida (`hasattr`) y, si lo está, la usa. **No hay que editar el dashboard** para integrar A: basta con exponer las funciones de esta página con estos nombres y formatos. Todas reciben `comun.Filtros` y se llaman con caché de 10 minutos; los errores se muestran como aviso sin detener el resto de la página.

## Reglas comunes (de `contrato.py` y `comun.py`)

- Lecturas → DataFrame con exactamente `COLUMNAS_ESTANDAR = [zona, timestamp, id_sensor, tipo_metrica, valor, fuente]`, `timestamp` en UTC, validado con `validar_zonas` y `validar_tipo_metrica`.
- `tipo_metrica` es la **métrica concreta** (D-01, 2026-11-27): `pm25, pm10, co, no2, o3, co2, temperatura, humedad, precipitacion, viento, trafico, energia`, con su unidad en `contrato.UNIDADES`. `calidad_aire` se acepta como alias de PM2.5. Para filtrar catálogos de sensores por métrica se usa `contrato.tipos_sensor_de()` (pm25 → `calidad_aire`, temperatura → `clima`).
- `Filtros.tipos_metrica` vacío = todas las métricas. La barra lateral manda la métrica elegida en «Tipo de medición» (o ninguna con «Todas las mediciones»).
- `fuente` ∈ `FUENTES_VALIDAS` (`"PostgreSQL"`, `"MongoDB"`, `"HDFS"`, `"Spark"`). El dashboard colorea la etiqueta de origen según este valor.
- Consultas → `dict[str, comun.Consulta]`. El dashboard muestra `titulo`, `descripcion` y `codigo`, crea un control por cada entrada de `parametros` (según el tipo del valor por defecto: bool, int, float, date o str) y ejecuta `funcion(**parametros)`, que debe devolver un DataFrame.

**Compatibilidad con la versión actual de A.** `CONSULTAS_PG` puede ser también una función que devuelva `{id: {"titulo", "codigo"}}`; el dashboard la convierte en `Consulta` y ejecuta cada una con `ejecutar_consulta(id)`. Las coordenadas `DECIMAL` de PostgreSQL se convierten a `float` antes de dibujar el mapa. Si `sensores(f)` falla, el mapa usa las posiciones de MongoDB.

## Funciones de la pareja A que el dashboard ya sabe usar

| Función | Devuelve | Dónde aparece |
|---|---|---|
| `estado() -> dict` | `{"PostgreSQL": bool, "HDFS": bool, "latencia_ms": {"PostgreSQL": float, "HDFS": float}, "detalle": {"PostgreSQL": str, "HDFS": str}}` (`latencia_ms` y `detalle` son opcionales) | Barra lateral: semáforo de servicios |
| `kpis_iot(f) -> dict` | `{"sensores": int, "activos": int, "inactivos": int}` | Vista principal → IoT (2 tarjetas) |
| `sensores(f) -> DataFrame` | Columnas mínimas `id_sensor, zona, lat, lon, estado`; `pm25` opcional para la capa de calor | Mapa. Mientras no exista, el mapa usa las posiciones de MongoDB (`sensores_mongo`) |
| `lecturas_pg(f) -> DataFrame` | `COLUMNAS_ESTANDAR`, `fuente="PostgreSQL"` | Tabla «Detalle de lecturas» (se concatena con las de B) |
| `lecturas_hdfs(f) -> DataFrame` | `COLUMNAS_ESTANDAR`, `fuente="HDFS"` | Tabla «Detalle de lecturas» |
| `CONSULTAS_PG: dict[str, Consulta]` | PG-Q01…Q10, PG-P01…P07 (`tecnologia="PostgreSQL"`, `lenguaje="sql"`) | Consultas por tecnología → PostgreSQL |
| `CONSULTAS_HDFS: dict[str, Consulta]` | HD-OPS, HD-MR01…MR05 (`tecnologia="HDFS"`) | Consultas por tecnología → HDFS |
| `explain(id, **params) -> str` | Texto del `EXPLAIN ANALYZE` | Botón «EXPLAIN ANALYZE» junto a cada consulta PG |

También se usan `resumen_hdfs()` (estado del clúster desde JMX) y las operaciones *sandbox* `hdfs_ls/cat/put/get/rm/mkdir/fsck`, `mr_ejecutar` y `mr_resultado`, en «Consultas por tecnología → HDFS».

## Funciones de la pareja B (referencia)

`estado`, `CONSULTAS_MONGO` (MG-Q01…Q10, MG-P01…P05), `CONSULTAS_SPARK` (SP-Q01…Q10), `ejecutar`, `crud`, `lecturas_mongo`, `kpis_ambiente`, `kpis_iot_mongo`, `serie_temporal`, `comparativa_zonas`, `distribucion_tipos`, `sensores_mongo`, `alertas`, `eventos`, `incidentes`, `spark_top_zonas_pico`, `spark_analisis`, `spark_correlacion`, `spark_pronostico`, `recalcular_analisis`, `lecturas_trafico_energia`, `kpis_movilidad`, `kpis_energia`. Ver los *docstrings* en [conectores_mongo_spark.py](../../conectores_mongo_spark.py).

## Resultados de Spark: HDFS y PostgreSQL

Las funciones `spark_*` leen los Parquet de HDFS con `docker exec smartcity_namenode hdfs dfs -get` (no requieren WebHDFS ni editar el archivo hosts). `carga_pg.py` (B3.4) copia los mismos resultados a `medicion_horaria`, `agg_ambiente_hora_zona`, `spark_kpi_zona` y `spark_anomalia`, que el dashboard consulta con `lecturas_pg` y las consultas PG-*.
