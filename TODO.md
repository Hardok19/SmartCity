# TODO — SmartCity Analytics (CE-3191, II-2026)

> **Dominio:** Ambiente → calidad del aire (PM2.5, PM10, CO, NO₂, O₃) + clima (temperatura, humedad, precipitación).
> **Fuentes reales:** OpenAQ (calidad del aire) + Open-Meteo (clima histórico). **Entrega: 25-nov-2026.**
>
> **Cómo usar este archivo:**
> 1. Toma la primera tarea `[ ]` de **tu pareja** cuyas dependencias estén `[x]`.
> 2. Sigue los **Pasos** y valida con **Aceptación**.
> 3. Márcala `[x] — ✅ AAAA-MM-DD · nota corta` y actualiza la tabla de progreso.
> 4. Commit en la rama de tu pareja (`pareja-a` / `pareja-b`) y PR a `main`. Cambios a archivos compartidos
>    (`contrato.py`, `comun.py`, `docker/docker-compose.yml`) **solo** con aviso a la otra pareja.
>
> Estados: `[ ]` pendiente · `[~]` en progreso · `[x]` hecha · `[!]` bloqueada (motivo) · `[-]` descartada (motivo)
> Responsables: **A** = Pareja A (PostgreSQL + Hadoop) · **B** = Pareja B (MongoDB + Spark) · **AB** = ambas.

---

## Reparto

| Pareja | Tecnologías | Alcance | Entrega |
|---|---|---|---|
| **A** | PostgreSQL + Hadoop | Modelo relacional (E-R, 3FN, PK/FK, índices, vistas, MV, funciones, particiones) + HDFS (estructura, carga, comandos `hdfs dfs`, bloques/réplicas, tolerancia a fallos) + MapReduce | `conectores_postgres_hadoop.py` |
| **B** | MongoDB + Spark | Modelo documental (esquema flexible, validadores, índices, CRUD, Aggregation Framework) + Spark (ETL, DataFrames, Spark SQL, 8 análisis, carga de resultados a PostgreSQL) | `conectores_mongo_spark.py` |
| **AB** | Streamlit + documentación | Dashboard único que importa ambos módulos (KPIs, filtros, 4 gráficos, tabla, mapa, etiqueta de origen) + comparación tecnológica + documento técnico + atributos + defensa | `dashboard.py` |

### Flujo de datos acordado

```
OpenAQ ──┐                                   ┌─► MongoDB (lecturas, eventos, alertas, incidentes)  [B]
         ├─► data/raw/ (CSV/JSON) ───────────┤
Open-Meteo┘                                  └─► HDFS /smartcity/raw/...                            [A]
                                                        │
MongoDB ──(export JSON Lines)──► HDFS /smartcity/raw/mongo_exports/  [B escribe, A documenta]
                                                        │
                              Spark (ETL + análisis)  ◄─┘                                           [B]
                                 │            │
          HDFS /smartcity/curated, /analytics (Parquet)   PostgreSQL: medicion_horaria, agg_*, spark_*  [tablas: A · datos: B]
                                 │
                       dashboard.py  ◄── conectores_postgres_hadoop.py + conectores_mongo_spark.py      [AB]
```

**Dependencias cruzadas importantes** (si una se atrasa, bloquea a la otra pareja):
- A → B: catálogo de sensores/zonas en PostgreSQL (F1.4) y tablas destino de Spark (A2.4).
- B → A: exportaciones de Mongo en HDFS (B2.6) para los jobs MapReduce de eventos (A4.2).
- AB: `comun.py` (F1.3) antes de que cualquiera escriba funciones para el dashboard.

---

## Progreso

| Fase | Nombre | Resp. | Tareas | Hechas | Estado |
|---|---|---|---|---|---|
| F0 | Entorno Docker (ya existente) | AB | 3 | 3 | 🟢 |
| F1 | Acuerdos comunes y datos maestros | AB | 6 | 6 | 🟢 |
| F2 | Adquisición de datos (OpenAQ + Open-Meteo) | A/B | 4 | 4 | 🟢 |
| A1 | PostgreSQL: modelo y DDL | A | 5 | 5 | 🟢 |
| A2 | PostgreSQL: consultas, vistas, funciones, optimización | A | 6 | 6 | 🟢 |
| A3 | HDFS: estructura, carga y operaciones | A | 5 | 5 | 🟢 |
| A4 | Hadoop: MapReduce | A | 4 | 4 | 🟢 |
| A5 | Módulo `conectores_postgres_hadoop.py` | A | 4 | 4 | 🟢 |
| B1 | MongoDB: modelo, validadores e índices | B | 3 | 3 | 🟢 |
| B2 | MongoDB: carga, CRUD, consultas y export | B | 6 | 6 | 🟢 |
| B3 | Spark: entorno y ETL | B | 4 | 3 | 🟡 |
| B4 | Spark: SQL obligatorias y análisis | B | 4 | 3 | 🟡 |
| B5 | Módulo `conectores_mongo_spark.py` | B | 4 | 4 | 🟢 |
| D | Dashboard Streamlit | AB | 8 | 7 | 🟡 |
| C | Comparación tecnológica (experimentos) | AB | 3 | 3 | 🟢 |
| E | Documentación, defensa y entrega | AB | 8 | 3 | 🟡 |
| R | Reto adicional: streaming | B | 1 | 1 | 🟢 |

Leyenda: ⚪ sin empezar · 🟡 en curso · 🟢 completa · 🔴 bloqueada.

**Ruta crítica:** F1 → F2 → (A1 ∥ B1) → (A3 ∥ B2) → B3 → B4 → D → E.
A2/A4 y B2-consultas pueden avanzar en paralelo en cuanto haya datos cargados. El esqueleto del dashboard (D1–D2) puede empezar tras F1.3 con datos falsos.

---

## Decisiones (registrar aquí; justificar en el documento técnico)

| ID | Decisión | Estado |
|---|---|---|
| D-01 | `contrato.py` no tiene tipos para clima ni distingue contaminantes (todo es `calidad_aire`). **Propuesta:** agregar a `TIPOS_METRICA` `pm25, pm10, co, no2, o3, temperatura, humedad, precipitacion` con `UNIDADES` `µg/m3, µg/m3, µg/m3, µg/m3, µg/m3, °C, %, mm` (se conserva `calidad_aire` como categoría). | ✅ 2026-11-27 · aplicado (F1.1) con `co2` y `viento` además; `CATEGORIA_POR_METRICA` + `tipos_sensor_de()` |
| D-02 | Ciudad de referencia: `contrato.py` exige 5 zonas (Norte/Sur/Centro/Este/Oeste). Elegir **una ciudad real con ≥ 5 estaciones OpenAQ con PM2.5 y ≥ 2 años de historia** (verificar en explore.openaq.org) y asignar cada estación a una zona según su posición respecto al centro. Open-Meteo se consulta en el centroide de cada zona. | ⏳ F1.2 |
| D-03 | KPIs de Movilidad y Energía del enunciado quedan fuera del dominio Ambiente. **Recomendado:** series sintéticas mínimas (`trafico`, `energia`, ya previstas en `contrato.py`) generadas por B con perfil horario y documentadas como sintéticas, para que el dashboard muestre todos los KPIs (criterio de 14 pts). Alternativa: tarjetas "fuera de dominio" con justificación (riesgo de perder puntos). | ✅ 2026-11-27 · adoptado: `trafico` (`generar_trafico_sintetico.py`) y `energia` (`generar_energia_sintetica.py`) simulados, 87.720 filas cada uno (GENERACION_SINTETICA.md §6) |
| D-04 | Spark desde Streamlit: el driver en Windows conectado al master en Docker suele fallar (red y versión de Python). **Recomendado:** los jobs corren con `spark-submit` dentro de `smartcity_spark_master` (repo montado) y escriben resultados en PostgreSQL/HDFS; el dashboard los lee y tiene un botón "Recalcular" que lanza el job con `docker exec`. | ✅ 2026-11-25 · adoptado: repo montado en `/opt/smartcity` y `spark-submit` vía `docker exec` (ver `docs/GUIA_SPARK.md`) |
| D-05 | El compose usa 1 DataNode con `dfs.replication=1`: no se puede demostrar replicación ni tolerancia a fallos. **Propuesta:** 3 DataNodes y `replication=2`, más ResourceManager + NodeManager para MapReduce. | ✅ 2026-11-27 · aplicado en F0.3 (3 DataNodes, replicación 2, YARN); desde el 2026-11-27 también los Parquet de Spark (todos los jobs usan `get_spark()`, antes 6 escribían con réplica 3) |
| D-06 | Datos sintéticos para alcanzar los volúmenes mínimos (PG ≥ 100k, Mongo ≥ 500k, Spark ≥ 1 M, recomendado > 5 M total): sensores IoT virtuales por zona, anclados a los valores reales de OpenAQ/Open-Meteo + ruido + anomalías. Semilla fija `2026`. | ⏳ F1.5 |

---

## F0 — Entorno Docker (ya existe en `docker/`)

- [x] **F0.1** `docker/docker-compose.yml` con PostgreSQL 16, MongoDB 7, HDFS (bde2020 namenode + datanode) y Spark 3.5 (master + worker) — ✅ previo
- [x] **F0.2** Guía `DOCKER_SETUP.md` y scripts init (`postgres-init/01_schema.sql`, `mongo-init/01_init.js`) — ✅ previo
- [x] **F0.3** Ajustes del compose para lo que pide el enunciado (D-04, D-05) — **A** (Hadoop) + **B** (Spark), un solo PR — ✅ 2026-11-27 · parte A: 3 DataNodes + replicación 2, ResourceManager + NodeManager (imagen con Python 3, `docker/hadoop/Dockerfile.nodemanager`), `mem_limit`; verificado: 3 DataNodes vivos, 1 nodo YARN, `pi 2 10` OK en 19 s. PostgreSQL expuesto en 5433 (el 5432 lo ocupa un PostgreSQL de Windows)
  - **Pasos:**
    1. **A:** `datanode1..3` (cada uno con su volumen, `hostname` propio, puertos 9864/9865/9866), `HDFS_CONF_dfs_replication=2`; agregar `resourcemanager`, `nodemanager` (y opcional `historyserver`) con imágenes `bde2020/hadoop-*:2.0.0-hadoop3.2.1-java8` y variables `YARN_CONF_*`/`MAPRED_CONF_*` en `hadoop.env`.
    2. **A:** comprobar `docker exec smartcity_nodemanager python3 --version` (lo necesita Hadoop Streaming). Si no hay Python: plan B = jobs MapReduce en Java (compilar dentro del contenedor). Anotar resultado.
    3. **B:** montar el repo en `spark-master` y `spark-worker` (`../:/opt/smartcity`), segundo worker opcional (`SPARK_WORKER_MEMORY=2G`), y el conector de Mongo (`--packages org.mongodb.spark:mongo-spark-connector_2.12:10.4.x`) + driver JDBC de PostgreSQL.
    4. **Ambas:** probar con ~8 GB de RAM libres; si no alcanza, limitar memoria (`mem_limit`) y documentarlo.
  - **Aceptación:** `hdfs dfsadmin -report` muestra 3 DataNodes vivos; `yarn node -list` 1 nodo; job de ejemplo `pi 2 10` termina; UI :8080 muestra los workers ALIVE.

---

## F1 — Acuerdos comunes y datos maestros (AB, 9–11 nov)

- [x] **F1.1** Cerrar el contrato de datos (D-01) — **AB** — ✅ 2026-11-27 · `TIPOS_METRICA` + `UNIDADES` con pm25, pm10, co, no2, o3, co2, temperatura, humedad, precipitacion, viento (mismos códigos que `variables_medicion`, Mongo y el ETL); `calidad_aire` queda como alias de PM2.5; cambio anotado en la cabecera. Adaptados `mongo_contrato.py`, `lecturas_mongo`, `lecturas_pg`, `lecturas_hdfs`, filtros por tipo de sensor y la barra lateral («Todas las mediciones»). `tests/test_contrato.py`. ⚠ avisar a la pareja A
  - Revisar `contrato.py` juntos, aplicar la ampliación de `TIPOS_METRICA`/`UNIDADES` y anotar el cambio en un comentario de cabecera. A partir de aquí **toda** función que devuelva lecturas al dashboard devuelve `COLUMNAS_ESTANDAR` en UTC y pasa `validar_zonas` + `validar_tipo_metrica`.
  - **Aceptación:** ambas parejas aprueban el PR; `python -c "import contrato"` funciona.
- [x] **F1.2** Ciudad y zonas (D-02) — **AB** — ✅ 2026-11-23 · config/zonas.yaml y config/estaciones_openaq.yaml creados
  - Elegir ciudad, listar estaciones OpenAQ candidatas (id, nombre, lat, lon, parámetros, rango de fechas) y asignarlas a las 5 zonas. Guardar en `config/zonas.yaml` (zona, distrito, lat/lon del centroide, población aprox.) y `config/estaciones_openaq.yaml`.
  - **Aceptación:** cada zona tiene ≥ 1 estación real con PM2.5; mapa rápido de verificación.
- [x] **F1.3** Módulo compartido `comun.py` (raíz) — **AB** — ✅ 2026-11-27 · `Filtros` (frozen, con `horas`, `desde_utc/hasta_utc`, `aplicar()`), `Consulta`, `CONFIG` desde `.env` + `.env.example`; B ya lo usa, falta que A lo adopte (aviso a A por ser archivo compartido)
  - `@dataclass(frozen=True) Filtros`: `fecha_inicio, fecha_fin, perfil_horario` (madrugada 0–5, mañana 6–11, tarde 12–17, noche 18–23, hora_pico 6–8 y 16–19), `zonas, tipos_metrica, id_sensor, tipo_evento, fuente, valor_min, valor_max`.
  - `@dataclass Consulta`: `id` (`PG-Q01`, `MG-Q05`, `HD-MR02`, `SP-Q07`…), `tecnologia` (una de `FUENTES_VALIDAS`), `titulo`, `descripcion`, `codigo` (SQL/pipeline/mapper/SQL Spark para mostrar en la UI), `lenguaje`, `parametros: dict` (nombre → valor por defecto), `funcion: Callable[..., pd.DataFrame]`.
  - `config.py` o sección en `comun.py` con credenciales leídas de `.env` (crear `.env.example` con los valores de `DOCKER_SETUP.md`).
  - **Aceptación:** ambos módulos de conectores importan `Filtros` y `Consulta` de aquí; nadie define los suyos.
- [x] **F1.4** Catálogo maestro de sensores — **A** (lo consume B) — ✅ 2026-11-23 · scripts/generar_maestros.py → data/sensores.csv (ruta real: data/, no data/maestros/)
  - Generar `data/maestros/sensores.csv` (semilla 2026): estaciones OpenAQ reales (`origen=openaq`) + sensores IoT virtuales por zona (calidad del aire y clima; D-06) + los de `trafico`/`energia` si se aprueba D-03. Columnas: `id_sensor` (`S-001`… mismo id en PG, Mongo y HDFS), `nombre, tipo_metrica, categoria, zona, lat, lon, fecha_instalacion, estado, organismo`. ~5 % `inactivo` y ~3 % que **nunca** envían datos (para PG-Q07).
  - **Aceptación:** CSV commiteado (es pequeño); B lo usa en su generador.
- [x] **F1.5** Plan de volúmenes y datos sintéticos (D-03, D-06) — **AB** — ✅ 2026-11-27 · `docs/datos/VOLUMENES.md`: real vs sintético, conteos medidos tras la corrida completa (PG 2.632.112 · Mongo 1.068.584 · Spark 4.380.538 · ≈ 8,1 M total), tamaños medidos (HDFS raw 448 MB, PG 427 MB, Mongo 387 MB): «bueno» (0,5–1 GB) sin contar benchmarks
  - Tabla en `docs/datos/VOLUMENES.md`: qué es real, qué es sintético, cuántas filas/documentos se esperan por tecnología y cómo se alcanzan los mínimos (PG ≥ 100k, Mongo ≥ 500k, Spark ≥ 1 M, meta > 5 M total, tamaño ≥ 500 MB).
- [x] **F1.6** Estructura del repo, dependencias y estilo — **AB** — ✅ 2026-11-27 · `pip install -r requirements.txt` en un venv limpio (Python 3.13) OK y `pytest` 43 pasan / 28 omitidas con ese entorno (52 tras F1.1) (`docs/resultados/qa_final.md`); se agregó `markdown` (armado del PDF)
  - Carpetas: `config/ data/{raw,interim,maestros} sql/{ddl,queries,demo} mongo/{queries} hadoop/{hdfs,mapreduce} spark/{jobs,sql} scripts/ docs/{datos,tecnico,diagramas,resultados,defensa,atributos} tests/`. `data/raw` y `data/interim` en `.gitignore`.
  - `requirements.txt` (fijar `pyspark==3.5.*` igual a la imagen): `pandas, numpy, pyarrow, requests, python-dotenv, pyyaml, psycopg2-binary, sqlalchemy, pymongo, hdfs, docker, pyspark, streamlit, plotly, pydeck, pytest`.
  - **Aceptación:** `pip install -r requirements.txt` en un venv limpio funciona.

---

## F2 — Adquisición de datos (10–12 nov)

> Scripts idempotentes en `scripts/descargar_*.py` (si el archivo ya existe no se descarga de nuevo), con registro
> en `data/raw/MANIFEST.json` (url, fecha, bytes). Respetar límites de las APIs (pausa entre requests, reintento en 429).

- [x] **F2.1** OpenAQ — **B** — `scripts/descargar_openaq.py` — ✅ 2026-11-23 · scripts/descargar_openaq.py · 2026-11-27: `scripts/descargar_openaq_archivo.py` (misma salida sin clave, archivo público S3; 168.824 filas de las 5 estaciones, ~1 min; PM2.5 solo en Centro y Este)
  - Por cada estación de `config/estaciones_openaq.yaml`: mediciones horarias de `pm25, pm10, co, no2, o3` (las disponibles). Opción 1: API v3 (requiere API key gratuita, en `.env`). Opción 2: archivo público en S3 `openaq-data-archive` (sin key, CSV.gz por estación/día).
  - Guardar en `data/raw/openaq/<estacion>/<anio>.csv`. Anotar licencia y fecha de descarga.
  - **Aceptación:** conteo de filas por estación y rango de fechas; % de nulos.
- [x] **F2.2** Open-Meteo — **A** — `scripts/descargar_openmeteo.py` — ✅ 2026-11-23 · scripts/descargar_openmeteo.py
  - API de archivo histórico (`archive-api.open-meteo.com`) por centroide de zona: `temperature_2m, relative_humidity_2m, precipitation, wind_speed_10m` horario, mismo rango que OpenAQ, `timezone=UTC`, bloques de 1 año. Opcional: API de calidad del aire de Open-Meteo como respaldo si alguna zona queda sin estación OpenAQ.
  - Guardar `data/raw/openmeteo/zona_<zona>_<anio>.json`.
  - **Aceptación:** 5 zonas × años esperados, sin huecos grandes (script de validación).
- [x] **F2.3** Perfilado de fuentes — **AB** — `docs/datos/FUENTES.md` — ✅ 2026-11-27 · `FUENTES.md` (descripción, URL, licencia, comando, limpieza, destino y por qué; descarga sin clave y convención de hora) + `perfil_fuentes.md` regenerado con OpenAQ (10 archivos, 168.824 filas), Open-Meteo (10 archivos, 0 nulos), tráfico y energía
  - Por fuente: descripción, URL, licencia, cómo se descarga (comando), filas, columnas, nulos, min/max, frecuencia, tamaño en disco, limpieza aplicada y **en qué tecnología termina y por qué**.
- [x] **F2.4** Generador sintético — **B** — `scripts/generar_sinteticos.py` — ✅ 2026-11-23 · scripts/generar_sinteticos.py + docs/datos/GENERACION_SINTETICA.md
  - Sensores IoT virtuales de `sensores.csv`: lectura cada 15 min = valor real horario de su zona (OpenAQ/Open-Meteo interpolado) + sesgo propio + ruido gaussiano; ~1 % anomalías (picos, negativos, sensor pegado), huecos de 1–6 h, y desde cierta fecha un campo nuevo (`firmware 2.x` agrega `o3`, `bateria_pct`) → evolución de esquema. Más `trafico`/`energia` si D-03.
  - Documentar fórmulas y parámetros en `docs/datos/GENERACION_SINTETICA.md` (el enunciado lo exige).
  - **Aceptación:** reproducible (misma semilla → mismos datos); volúmenes de F1.5 alcanzados.

---

## Pareja A — PostgreSQL + Hadoop

### A1 — PostgreSQL: modelo y DDL (10–13 nov)

- [x] **A1.1** Diagrama E-R y justificación 3FN — `docs/diagramas/er_postgres.md` (Mermaid `erDiagram`) + `docs/tecnico/05_modelo_relacional.md` — ✅ 2026-11-27 · `docs/diagramas/er_postgres.md` (Mermaid) + `docs/tecnico/05_modelo_relacional.md` (3FN por tabla); corregida la dependencia transitiva tipo_sensor→categoría con `tipos_sensor`
  - Mínimo del enunciado: `sensor(id_sensor, nombre, tipo_sensor, fecha_instalacion, estado)`, `ubicacion(id_ubicacion, nombre_zona, distrito, coordenadas)`, `categoria_sensor`, `usuario(id_usuario, nombre, correo)`, `organismo(id_organismo, nombre, responsable)`.
  - Recomendado para 3FN y consultas: `zona` y `distrito` como tablas propias (evita la dependencia transitiva `ubicacion → zona → distrito`), `tipo_sensor` (FK a categoría), `variable` (código, unidad, umbral de alerta), `estacion_openaq` (id externo), `calendario` (fecha, día de semana, feriado), `medicion_horaria` (datos limpios desde Spark), `agg_ambiente_hora_zona`, `spark_kpi_zona`, `spark_anomalia`, `benchmark_resultado`.
  - Por tabla: por qué está en 1FN/2FN/3FN y por qué estos datos van en PostgreSQL (integridad referencial, catálogos estables) — **material de defensa**.
  - **Aceptación:** diagrama renderiza en GitHub; cada entidad mínima tiene todos sus atributos.
- [x] **A1.2** DDL y restricciones — `sql/ddl/001_tablas.sql`, `002_restricciones.sql` — ✅ 2026-11-27 · `001_tablas.sql` + `004_restricciones.sql` (CHECK de coordenadas, fechas, correo, origen, humedad; NOT NULL; todas las FK `ON DELETE RESTRICT`; `COMMENT ON`; migración de bases anteriores); `pg_init.py` ejecutado 3 veces sin error
  - Migrar y completar `docker/postgres-init/01_schema.sql`: PK, FK (`ON DELETE RESTRICT`), `CHECK` (estado ∈ contrato, lat −90..90, lon −180..180, `fecha_instalacion <= current_date`, correo con `@`), `UNIQUE`, `NOT NULL`, `COMMENT ON`. Idempotente (`IF NOT EXISTS`). Script `scripts/pg_init.py` que ejecuta los `.sql` en orden (no depender solo de `docker-entrypoint-initdb.d`, que corre una sola vez).
  - **Aceptación:** ejecutar `pg_init` dos veces sin error; `\d+ sensor` muestra las restricciones.
- [x] **A1.3** Particionamiento — `sql/ddl/003_particiones.sql` — ✅ 2026-11-27 · partición mensual 2023-01…2024-12 + DEFAULT con `fn_crear_particiones()` (`002_extensiones.sql`); `EXPLAIN` de un mes toca solo `medicion_horaria_2024_03`
  - `medicion_horaria PARTITION BY RANGE (timestamp)` mensual + partición `DEFAULT`; función `fn_crear_particiones(desde, hasta)`.
  - **Aceptación:** `EXPLAIN` de una consulta de un mes toca una sola partición.
- [x] **A1.4** Índices justificados — `sql/ddl/004_indices.sql` — ✅ 2026-11-27 · `005_indices.sql`: índices en todas las FK, (id_sensor, timestamp DESC), (variable, timestamp DESC), BRIN y parcial, cada uno con `-- justifica:`
  - Todas las FK; `medicion_horaria (id_sensor, timestamp DESC)`; **BRIN** en `timestamp` (datos insertados en orden temporal → índice diminuto); parcial `sensor (tipo_sensor) WHERE estado='activo'`. Cada uno con comentario `-- justifica: …`.
- [x] **A1.5** Carga de maestros — `scripts/pg_seed.py` — ✅ 2026-11-27 · `scripts/pg_cargar_maestros.py` (idempotente): 5 zonas, 10 organismos (2 sin sensores), 50 sensores (+T/E de D-03, posición propia), 40 usuarios ficticios; categorías, tipos, 12 variables y calendario 2023–2024 con feriados en DDL; FK válidas
  - Desde `config/zonas.yaml` y `data/maestros/sensores.csv`: zonas, distritos, ubicaciones, categorías, tipos, variables, organismos (~8, plausibles: Municipalidad, Ministerio de Salud, MINAE, IMN…), usuarios (~40, correos ficticios), sensores, calendario. Idempotente (`ON CONFLICT DO NOTHING`).
  - **Aceptación:** conteos impresos, todas las FK válidas.

### A2 — PostgreSQL: consultas, vistas, funciones y optimización (13–17 nov)

- [x] **A2.1** Vistas y vistas materializadas — `sql/ddl/005_vistas.sql` — ✅ 2026-11-27 · `v_sensor_detalle`, `mv_actividad_sensor`, `mv_kpi_diario_zona` (UNIQUE para REFRESH CONCURRENTLY)
  - `v_sensor_detalle` (sensor + tipo + categoría + ubicación + zona + organismo); `mv_kpi_diario_zona` (PM2.5, temperatura, humedad por zona-día) y `mv_actividad_sensor` (nº mediciones, primera/última) con índice `UNIQUE` para `REFRESH … CONCURRENTLY`.
- [x] **A2.2** Funciones y procedimientos — `sql/ddl/006_funciones.sql` — ✅ 2026-11-27 · `fn_sensores_por_organismo`, `fn_sensores_sin_actividad` (nunca vs. dejó de enviar), `fn_kpi_ambiente`, `fn_crear_particiones`, `sp_refrescar_mv()` (lo llama carga_pg)
  - `fn_sensores_por_organismo()`, `fn_sensores_sin_actividad(p_desde)` ("nunca enviaron" vs "dejaron de enviar"), `fn_kpi_ambiente(p_desde, p_hasta, p_zona)`, `sp_refrescar_mv()`.
- [x] **A2.3** Consultas obligatorias PG-Q01…Q10 — `sql/queries/PG-Qxx_<nombre>.sql` con encabezado (`id, titulo, tecnicas, parametros`) — ✅ 2026-11-27 · 17 archivos `sql/queries/PG-*.sql` (Q01–Q10 + P01–P07 con CTE recursiva, ventanas, subconsultas, fechas, ROLLUP, generate_series, calendario); las 17 devuelven filas con los datos completos (P04: Centro la zona más contaminada, 13,26 µg/m³; P07: racha de 18 días)
  | ID | Consulta | Técnicas a mostrar |
  |---|---|---|
  | PG-Q01 | Sensores por tipo | `GROUP BY`, `ORDER BY` |
  | PG-Q02 | Sensores por ubicación | `INNER JOIN` ubicación-zona |
  | PG-Q03 | Activos vs inactivos | `COUNT … FILTER`, `DISTINCT` |
  | PG-Q04 | Sensores por organismo | `LEFT JOIN` (organismos sin sensores) |
  | PG-Q05 | Ubicaciones con más sensores | `HAVING`, subconsulta |
  | PG-Q06 | Sensores instalados por año | `EXTRACT`, `generate_series` (años vacíos) |
  | PG-Q07 | Sensores sin actividad | `NOT EXISTS` / `LEFT JOIN … IS NULL` |
  | PG-Q08 | Ranking de zonas monitoreadas | **CTE** + `RANK() OVER` |
  | PG-Q09 | Consulta con vista | `v_sensor_detalle` / MV |
  | PG-Q10 | Consulta con función | `fn_sensores_por_organismo()` / `fn_kpi_ambiente()` |
  - Preguntas de ejemplo adaptadas al dominio (PG-P01…P04): temperatura promedio por zona; sensores que dejaron de enviar información; hora de mayor contaminación (`EXTRACT(hour)`, `ROW_NUMBER`); zona con mayor contaminación (promedio móvil con `AVG() OVER (ROWS BETWEEN …)`, `LAG` día a día).
  - Incluir al menos una `WITH RECURSIVE` (p. ej. jerarquía ciudad → distrito → zona → ubicación) — la rúbrica premia CTEs y window functions (7 pts).
  - **Aceptación:** las 14 consultas devuelven ≥ 1 fila con parámetros por defecto (requiere B3.4 para las de mediciones).
- [x] **A2.4** Tablas destino de Spark listas y documentadas para **B** — `medicion_horaria`, `agg_ambiente_hora_zona`, `spark_kpi_zona`, `spark_anomalia` (columnas, tipos, PK) — **bloquea B3.4**. — ✅ 2026-11-27 · tablas destino en `002_extensiones.sql`; usadas por `carga_pg.py` (B3.4)
- [x] **A2.5** Optimización con `EXPLAIN ANALYZE` — `docs/resultados/postgres_optimizacion.md` — ✅ 2026-11-27 · `scripts/pg_optimizacion.py` re-ejecutado con 2.632.112 filas: índice B-tree ×283 (114 → 0,4 ms), poda de particiones ×10, MV ×3.900 (1.854 → 0,47 ms), BRIN 176× más chico que B-tree pero algo más lento en 3 meses (51 frente a 42 ms)
  - 4 casos: con/sin índice (`SET enable_indexscan=off`), poda de particiones, BRIN vs B-tree (tamaño y tiempo), MV vs consulta en vivo. Guardar tiempos en `benchmark_resultado`.
- [x] **A2.6** Script de demo en vivo — `sql/demo/demo_defensa.sql` (plan B si la UI falla durante la defensa). — ✅ 2026-11-27 · `sql/demo/demo_defensa.sql` probado: muestra FK, CHECK y ON DELETE RESTRICT rechazando datos, consultas y EXPLAIN

### A3 — HDFS: estructura, carga y operaciones (11–15 nov)

- [x] **A3.1** Estructura — `hadoop/hdfs/01_estructura.sh` — ✅ 2026-11-27 · `hadoop/hdfs/01_estructura.sh` ejecutado; justificación en `docs/tecnico/07_estrategia_big_data.md`
  - `/smartcity/raw/{openaq,openmeteo,sinteticos,mongo_exports}`, `/smartcity/curated`, `/smartcity/analytics`, `/smartcity/mapreduce`, `/smartcity/sandbox`; particiones estilo Hive `anio=/mes=`. Justificar en `docs/tecnico/07_estrategia_big_data.md`.
- [x] **A3.2** Carga de crudos — `hadoop/hdfs/02_cargar_raw.sh` (`docker cp` + `hdfs dfs -put -f`) — ✅ 2026-11-27 · OpenAQ (10), Open-Meteo (10), tráfico, energía y export de Mongo (4 colecciones, conteo Mongo = HDFS); `/smartcity/raw` = 448 MB lógicos / 896 MB con réplica
  - Unir archivos pequeños antes de subir (p. ej. un CSV por mes) → **problema de archivos pequeños** (argumento de defensa). Sin gzip en los CSV grandes (no es divisible); justificar.
  - Nota: WebHDFS desde Windows redirige a `datanodeN:9864`; agregar esos nombres a `C:\Windows\System32\drivers\etc\hosts` → `127.0.0.1` o subir siempre vía `docker exec`.
  - **Aceptación:** `hdfs dfs -du -h /smartcity/raw` = tamaños locales.
- [x] **A3.3** Demostración de comandos — `hadoop/hdfs/03_demo.sh` → `docs/resultados/hdfs_demo.txt` — ✅ 2026-11-27 · `hadoop/hdfs/03_demo.sh` → `docs/resultados/hdfs_demo.txt` (mkdir/put/ls/cat/get/rm/du/stat/setrep, fsck con réplicas por DataNode, 1 vs 3 bloques)
  - `-mkdir, -put, -ls, -cat | head, -get, -rm, -du -h, -stat, -setrep`; `hdfs fsck <archivo> -files -blocks -locations` (bloques y en qué DataNode está cada réplica); subir el mismo archivo con `-D dfs.blocksize=33554432` para comparar nº de bloques.
- [x] **A3.4** Tolerancia a fallos — `hadoop/hdfs/04_tolerancia.ps1` → `docs/resultados/hdfs_tolerancia.md` — ✅ 2026-11-27 · `hadoop/hdfs/04_tolerancia.ps1` → `docs/resultados/hdfs_tolerancia.md`: lectura OK con datanode2 apagado, detectado a los 97 s, re-replicado a los 152 s, reingreso 7 s
  - `fsck` inicial → `docker stop smartcity_datanode2` → leer el archivo completo (sigue disponible) → `dfsadmin -report` (2 vivos) y `fsck` (re-replicación) → `docker start`. Medir tiempos. **Momento fuerte de la demo.**
- [x] **A3.5** Documentar Hadoop — `docs/tecnico/07_estrategia_big_data.md`: almacenamiento distribuido, bloques, replicación, tolerancia a fallos, escalabilidad (agregar DataNodes), localidad de datos, por qué este problema justifica un sistema distribuido (con cifras propias), qué pasa si el volumen crece ×100. — ✅ 2026-11-27 · `docs/tecnico/07_estrategia_big_data.md` (bloques, replicación, tolerancia, escalabilidad, localidad, ×100)

### A4 — Hadoop: MapReduce (14–18 nov)

- [x] **A4.1** Infraestructura de jobs — `hadoop/mapreduce/run_job.sh <job> <input> <output>` (Hadoop Streaming con Python; borra la salida previa). Cada job con `mapper.py`, `reducer.py`, `README.md` y prueba local `cat muestra | python mapper.py | sort | python reducer.py`. — ✅ 2026-11-27 · `hadoop/mapreduce/run_job.sh` (en el NameNode) + `ejecutar.sh` (anfitrión, anota tiempos en `docs/resultados/mapreduce_tiempos.csv`) + README
- [x] **A4.2** Los 5 jobs de las preguntas del enunciado — ✅ 2026-11-27 · mr01–mr05 sobre el export real (445 MB) en YARN, 17–26 s c/u (`mapreduce_tiempos.csv`): 38 sensores; S-018 el de más lecturas (34.990); temperatura histórica 11,60 °C; 1.428 eventos (= Mongo); 4.929 filas sensor-día-variable en marzo 2024
  | Job | Pregunta | Entrada |
  |---|---|---|
  | `mr01` | ¿Cuántas mediciones existen por sensor? | raw OpenAQ + sintéticos (combiner = reducer) |
  | `mr02` | ¿Qué sensor produjo más datos? | salida de mr01 (1 reducer, top-N) |
  | `mr03` | Temperatura promedio histórica (global, por zona/año) | raw Open-Meteo (emitir suma y conteo, no promedios) |
  | `mr04` | Cantidad de eventos por zona | `mongo_exports/evento` (JSON Lines, **depende de B2.6**) |
  | `mr05` | Comportamiento de los sensores en un período | raw, `-cmdenv DESDE= HASTA=` → promedio diario por sensor |
  - **Aceptación:** los 5 terminan en YARN (:8088), tiempos anotados en `docs/resultados/mapreduce_tiempos.md`.
- [x] **A4.3** Ejecución desde Python — funciones `mr_ejecutar(job)` (vía `docker exec`, SDK `docker`) y `mr_resultado(job)` (lee la salida de HDFS como DataFrame). — ✅ 2026-11-27 · `mr_ejecutar(job)` y `mr_resultado(job)` en `conectores_postgres_hadoop.py`
- [x] **A4.4** Pruebas unitarias de mappers/reducers — `tests/test_mapreduce.py` con `data/samples/`. — ✅ 2026-11-27 · `tests/test_mapreduce.py` con `data/samples/` (5 pruebas, comparan contra cálculo independiente)

### A5 — Módulo `conectores_postgres_hadoop.py` (12–19 nov)

- [x] **A5.1** Conexiones: `estado() -> dict` (`{"PostgreSQL": bool, "HDFS": bool}` + latencia), engine SQLAlchemy cacheado, cliente WebHDFS, `ejecutar_en_namenode(cmd) -> (codigo, salida)`. — ✅ 2026-11-27 · `conectores_postgres_hadoop.py`: `estado()` PG + HDFS (JMX) con latencia, engine SQLAlchemy cacheado, `ejecutar_en_namenode()`; `conectores_postgres.py` queda como compatibilidad
- [x] **A5.2** Registro de consultas: `CONSULTAS_PG` y `CONSULTAS_HDFS: dict[str, Consulta]` (de `comun.py`): PG-Q01…Q10, PG-P01…P04, HD-OPS (ls/cat/fsck), HD-MR01…MR05; `ejecutar(id, **params) -> DataFrame`; `explain(id, **params) -> str`. — ✅ 2026-11-27 · `CONSULTAS_PG` (17, leídas de sql/queries) y `CONSULTAS_HDFS` (HD-OPS-LS/DU/FSCK + HD-MR01…05) como `dict[str, Consulta]`; `ejecutar()`, `explain()`
- [x] **A5.3** Funciones para el dashboard (todas reciben `Filtros`): — ✅ 2026-11-27 · `sensores`, `kpis_iot`, `lecturas_pg` (todos los filtros), `lecturas_hdfs` (mr05), `resumen_hdfs` (JMX) y `hdfs_ls/cat/put/get/rm/mkdir/fsck` con su comando (escritura solo en sandbox)
  - `sensores(f)` → catálogo con lat/lon y estado (pasa `validar_estado`) — mapa y KPIs IoT.
  - `kpis_iot(f)` → nº sensores, activos/inactivos.
  - `lecturas_pg(f)` → `COLUMNAS_ESTANDAR` desde `medicion_horaria`, `fuente="PostgreSQL"`.
  - `lecturas_hdfs(f)` / `resumen_hdfs()` → `fuente="HDFS"` (salidas MapReduce, estado del clúster desde JMX: DataNodes vivos, capacidad, bloques).
  - Operaciones sandbox: `hdfs_ls, hdfs_cat, hdfs_put, hdfs_get, hdfs_rm, hdfs_mkdir, hdfs_fsck` devolviendo también el comando `hdfs dfs` equivalente (para mostrarlo en la UI).
- [x] **A5.4** Pruebas — `tests/test_conectores_pg_hadoop.py`: cada función devuelve las columnas del contrato, zonas válidas, `timestamp` UTC; las 14 + 6 consultas corren sin error. — ✅ 2026-11-27 · `tests/test_conectores_pg_hadoop.py`: 30 pruebas pasan contra PostgreSQL y HDFS reales (encontró y se corrigió un error de lectura de cabeceras)

---

## Pareja B — MongoDB + Spark

### B1 — MongoDB: modelo, validadores e índices (10–12 nov)

- [x] **B1.1** Modelo documental y justificación NoSQL — `docs/diagramas/modelo_documental.md` + `docs/tecnico/06_modelo_documental.md` — ✅ 2026-11-27
  - Colecciones que exige el enunciado (ampliando `mediciones/eventos/alertas` de `mongo-init/01_init.js`):
    - `lecturas` — `id_sensor` (= PG), `tipo_metrica`, `timestamp` (Date, UTC), `ubicacion {zona, geo: {type:"Point", coordinates:[lon,lat]}}`, `origen` (`openaq`/`sintetico`), `mediciones {…}` **distinto por tipo** (aire `{pm25, pm10, co, no2, o3?}`; clima `{temperatura, humedad, precipitacion, viento?}`), opcionales `dispositivo {firmware, bateria_pct}`, `calidad_dato {valido, flags[]}`.
    - `eventos` — contaminación alta, lluvia intensa, ola de calor, falla de sensor; `detalles{}` distinto por tipo.
    - `alertas` — `regla, variable, valor, umbral, nivel, id_sensor, zona, emitida, estado, acciones[]`.
    - `incidentes` — **texto libre** (quemas, humo, incendios forestales, inundaciones, olores…): `categoria, titulo, descripcion, historial[], reportes_ciudadanos[]`.
  - Justificar: embebido vs referencia, por qué sería incómodo en relacional (columnas dispersas/NULL, migraciones por cada tipo/firmware nuevo, arrays anidados), y cuándo **no** usar Mongo.
- [x] **B1.2** Validadores e índices — `mongo/01_colecciones.js`, `mongo/02_indices.js` (+ script Python equivalente idempotente) — ✅ 2026-11-27 · `scripts/mongo_admin.py` + los dos `.js` equivalentes (el 01 prueba que una lectura sin `sensor_id` se rechaza; el 02 lista `getIndexes()`) · validado contra Mongo real: rechaza la lectura sin `sensor_id` y `getIndexes()` lista todos
  - `$jsonSchema` flexible (solo exige `id_sensor, tipo_metrica, timestamp, ubicacion`), `validationLevel: moderate`.
  - Índices: `{id_sensor:1, timestamp:-1}`, `{tipo_metrica:1, "ubicacion.zona":1, timestamp:-1}`, `2dsphere` en `ubicacion.geo`, `text` en `incidentes {titulo, descripcion}`, parcial en `alertas {estado:"abierta"}`; opcional TTL en una colección temporal.
  - **Aceptación:** insertar un documento sin `id_sensor` falla; `getIndexes()` los lista.
- [x] **B1.3** Umbrales de alerta en `config/umbrales.yaml` (PM2.5 35,4 / 55,4 / 150,4 µg/m³; temperatura; precipitación) — usados por alertas, Spark y streaming. — ✅ 2026-11-23 · config/umbrales.yaml

### B2 — MongoDB: carga, CRUD, consultas y export (11–17 nov)

- [x] **B2.1** Carga de lecturas — `scripts/mongo_cargar.py`: OpenAQ real + sintéticos (F2.4), `insert_many(ordered=False)` en lotes de 10k, barra de progreso, `--limite` para pruebas. — ✅ 2026-11-25 · 2026-11-27: `lectura` = 1.068.584 documentos (≥ 500k), 0 rechazados, 61 s (17.789 doc/s)
  - **Aceptación:** `db.lecturas.countDocuments()` ≥ 500.000; tiempo de carga anotado.
- [x] **B2.2** Eventos, alertas e incidentes **derivados de las lecturas** — ✅ 2026-11-23 · generados en `scripts/generar_sinteticos.py` (alertas por umbral, eventos contaminación/falla/lluvia/ola de calor, 3.000 incidentes); 2026-11-27: 1.190 alertas y 1.428 eventos en Mongo — (alertas = lecturas que superan umbrales, coherentes con los datos); incidentes con plantillas de texto en español por categoría.
- [x] **B2.3** Demo CRUD — `mongo/03_crud_demo.js` + función Python: `insertOne, insertMany, find` (filtro + proyección + sort + limit), `findOne, updateOne` (`$set` + `$push`), `updateMany` (`$gt/$lt/$or`), `deleteOne` sobre una colección `demo_crud` (nunca sobre `lecturas`). Cada operación devuelve resultado + comando mongosh (para la UI). — ✅ 2026-11-23 · mongo/03_crud_demo.js · 2026-11-27: `conectores_mongo_spark.crud()` probado contra Mongo real (7 operaciones)
- [x] **B2.4** Consultas obligatorias MG-Q01…Q10 — `mongo/queries/MG-Qxx.js` (mismo pipeline que en Python) — ✅ 2026-11-27 · 15 `.js` + mismos pipelines parametrizados en `conectores_mongo_spark.CONSULTAS_MONGO`; MG-Q09 con `$lookup`, MG-Q05 con `$geoNear` si `radio_km > 0`; la medición < 3 s está en `tests/test_conectores_mongo_spark.py` (falta correrla con Mongo arriba)
  | ID | Consulta | Técnica |
  |---|---|---|
  | MG-Q01 | Lecturas de un sensor | `find` + proyección + sort (usa índice) |
  | MG-Q02 | Lecturas en un rango de fechas | `$match` `$gte/$lt` |
  | MG-Q03 | Máximos y mínimos | `$objectToArray` + `$unwind` + `$group $max/$min` |
  | MG-Q04 | Eventos críticos | `$in` severidad + `$sort` |
  | MG-Q05 | Alertas por zona geográfica | `$group` por zona + `$geoNear`/`$geoWithin` |
  | MG-Q06 | Promedio por zona | `$group $avg` |
  | MG-Q07 | Promedio por tipo de sensor | `$group` por `tipo_metrica` |
  | MG-Q08 | Incidentes por categoría | `$group` + `$text` opcional |
  | MG-Q09 | Agregación avanzada | `$facet` + `$bucket` + `$dateTrunc` + `$setWindowFields` (promedio móvil) + `$lookup` |
  | MG-Q10 | Filtros múltiples | `$and/$or/$in/$exists` (campos firmware 2.x) |
  - Preguntas del enunciado MG-P01…P05: temperatura promedio por zona, sensores sobre el límite, mediciones por sensor, zonas con más contaminación, evolución de una variable.
  - **Aceptación:** todas con parámetros por defecto válidos devuelven filas en < 3 s.
- [x] **B2.5** Experimento de índices — `explain("executionStats")` de MG-Q01/Q02/Q05 con y sin índice (`hint({$natural:1})`): `totalDocsExamined`, tiempo, `IXSCAN` vs `COLLSCAN` → `docs/resultados/mongo_indices.md`. — ✅ 2026-11-27 · con 1.068.584 lecturas: MG-Q01 50 frente a 1.068.584 documentos examinados (533 ms → < 1 ms), MG-Q02 1.550 frente a 1.068.584 (×227); MG-Q05 hace COLLSCAN también con índice (filtro sin el prefijo de `ix_zona_emitida`, 1.190 alertas), explicado en el informe
- [x] **B2.6** Export Mongo → HDFS — job Spark (conector Mongo) que escribe JSON Lines en `/smartcity/raw/mongo_exports/<coleccion>/fecha=…/` (alternativa: `mongoexport` + `hdfs dfs -put`). Conteos iguales a Mongo. **Lo necesita A4.2 (mr04).** — ✅ 2026-11-27 · `subir_crudos_hdfs.py --fuentes mongo` (vía `02_cargar_raw.sh`): lectura 1.068.584, evento 1.428, alerta 1.190, incidente 3.000 en `fecha=2026-11-27/`, conteo Mongo = líneas HDFS en las 4

### B3 — Spark: entorno y ETL (12–16 nov)

- [x] **B3.1** Entorno Spark (D-04) — `spark/jobs/common.py` con `get_spark()` (master `spark://spark-master:7077`, `fs.defaultFS=hdfs://namenode:8020`, conector Mongo, JDBC PostgreSQL, `spark.sql.session.timeZone=UTC`) + `scripts/spark_submit.ps1 <job>` que ejecuta `docker exec smartcity_spark_master spark-submit /opt/smartcity/spark/jobs/<job>.py`. — ✅ 2026-11-27 · `common.py` (+ `rutas.py` compartido con el dashboard, `leer_mongo()`) y `spark_submit.ps1`; JDBC de PG queda para B3.4 · prueba de humo OK en el clúster: `SMARTCITY_B31_RANGE_COUNT=1000000`, paralelismo 2
  - **Aceptación:** job trivial `spark.range(1_000_000).count()` aparece en la UI :8080 ejecutado por los workers; lee un archivo de HDFS y una colección de Mongo.
- [~] **B3.2** ETL ambiente — `spark/jobs/etl_ambiente.py` — 🟡 2026-11-27 · `etl_ambiente.py` (Mongo → Parquet, 4.380.538 filas, 15,5 MB, esquema explícito) → `/smartcity/curated/ambiente`; `etl_ambiente_hdfs.py` (raw OpenAQ/Open-Meteo/mongo_exports: 4.957.616 → 4.397.164 filas, 11,3 % descartado, dedup, `anio/mes`) → `/smartcity/curated/ambiente_hdfs`; rutas finales ✅ (`rutas.py`: `curated/` y `analytics/`, `sandbox/` solo para pruebas); falta el join con los maestros de PG (hoy usa `sensores.csv`)
  - raw OpenAQ + Open-Meteo + `mongo_exports/lecturas` (HDFS) → esquemas explícitos (`StructType`, nunca `inferSchema`) → limpieza (nulos, negativos, duplicados por sensor+timestamp, fuera de rango) → formato largo `zona, timestamp, id_sensor, tipo_metrica, valor, fuente` (igual al contrato) → join con zonas/sensores de PG (broadcast) → `/smartcity/curated/ambiente/` Parquet particionado por `anio/mes`.
  - **Aceptación:** ≥ 1 M filas procesadas; % descartado documentado; job usa los workers.
- [x] **B3.3** ETL de `trafico`/`energia` sintéticos (solo si D-03 aprobado), mismo patrón. — ✅ 2026-11-27 · ejecutado en el clúster: 175.440 filas (0 % descartado) en `curated/trafico_energia`
- [x] **B3.4** Carga a PostgreSQL — `spark/jobs/carga_pg.py` (**depende de A2.4**) — ✅ 2026-11-27 · `medicion_horaria` 2.456.672 ambientales + 175.440 tráfico/energía (2.632.112), `agg_ambiente_hora_zona` 87.720, `spark_kpi_zona` 35.104, `spark_anomalia` 104.137 (se reemplaza completa en cada carga); conteo Spark = PostgreSQL; idempotente; refresca las MV
  - Agregados por sensor-variable-hora → `medicion_horaria` (vía tabla staging + `INSERT … ON CONFLICT DO NOTHING`), por zona-hora → `agg_ambiente_hora_zona`; al final `CALL sp_refrescar_mv()`.
  - **Aceptación:** conteos Spark = PostgreSQL; re-ejecutar no duplica; `medicion_horaria` ≥ 100k filas (mínimo PG del enunciado).

### B4 — Spark: SQL obligatorias y análisis (15–19 nov)

- [x] **B4.1** Consultas obligatorias SP-Q01…Q10 — `spark/sql/SP-Qxx.sql` sobre vistas temporales (`ambiente`, `lecturas`, `eventos`, `zona`, `sensor`) — ✅ 2026-11-27 · 10/10 ejecutadas (2026-11-25); SP-Q09 con `PIVOT`; `ejecutar_consultas.py --exportar` guarda cada resultado para el dashboard; SP-Q10 → `spark_kpi_zona` depende de B3.4
  | ID | Consulta | Técnica |
  |---|---|---|
  | SP-Q01 | Sensores que generan más información | `GROUP BY` + `ORDER BY` |
  | SP-Q02 | Distribución geográfica de eventos | join eventos × zona |
  | SP-Q03 | Evolución temporal de indicadores | `date_trunc` mes/semana |
  | SP-Q04 | Correlación entre variables | `corr()` PM2.5 vs humedad/precipitación/viento/temperatura |
  | SP-Q05 | Ranking de zonas con mayor actividad | `RANK() OVER` |
  | SP-Q06 | Tendencias por período | `LAG()` y variación % |
  | SP-Q07 | Detección de anomalías | z-score con `AVG/STDDEV OVER (PARTITION BY sensor)` |
  | SP-Q08 | Agrupamiento por categorías | `ROLLUP/CUBE` categoría-tipo-zona |
  | SP-Q09 | Comparación histórica entre años | `PIVOT` por año |
  | SP-Q10 | Reporte consolidado | KPIs por zona → `spark_kpi_zona` (PG) + `/smartcity/analytics/reporte/` |
- [x] **B4.2** Los 8 análisis obligatorios — `spark/jobs/analitica.py` (DataFrame API), resultados en `/smartcity/analytics/<tipo>/` y tablas `spark_*` — ✅ 2026-11-27 · 8/8 re-ejecutados con los datos completos, resultados en `analytics/` + `hallazgos_spark.md` actualizado; anomalías con IQR: 13.852 marcadas + 38.278 z-score + 81.933 IQR (29.926 en ambos); tablas `spark_*` cargadas (B3.4)
  - **Descriptivo** (sensores más activos, estadísticos por variable) · **Temporal** (evolución mensual de PM2.5, curva diaria de temperatura) · **Correlacional** (matriz PM2.5–PM10–temperatura–humedad–precipitación–viento) · **Predictivo** (MLlib `LinearRegression`/`GBTRegressor`: PM2.5 diario por zona según clima; reportar RMSE/R² y pronóstico) · **Espacial** (zonas más contaminadas, hotspots) · **Movilidad** (horas pico de contaminación por zona × día de semana; con tráfico real si D-03) · **Anomalías** (z-score + IQR por sensor) · **Comparación** (perfil por zona; **"Top 3 zonas con mayor incremento de PM2.5 en hora pico vs valle"** → gráfico Spark del dashboard).
  - **Aceptación:** `docs/resultados/hallazgos_spark.md` con números concretos.
- [~] **B4.3** Optimización Spark (evidencia) — `docs/resultados/spark_optimizacion.md`: CSV vs Parquet (tiempo y bytes), *partition pruning* (`explain()` → `PartitionFilters`), broadcast vs sort-merge join, `cache()`, AQE on/off. Capturas de la Spark UI. — 🟡 2026-11-27 · ejecutado (3 repeticiones, 10 variantes, controles OK): broadcast −46 %, AQE −39 %, caché solo conviene con > 2 reutilizaciones, particiones y Parquet sin diferencia de tiempo pero con `PartitionFilters` y Parquet 17× más chico (9,5 MB → 573 KB); `spark_optimizacion.csv` + `spark_optimizacion_planes.txt`; faltan las capturas de la Spark UI (las toma el equipo)
- [x] **B4.4** Documentar — `docs/tecnico/08_procesamiento.md`: flujo, cada job (entrada → transformaciones → salida), transformaciones vs acciones, evaluación perezosa, particionamiento, **por qué Spark y no PostgreSQL** para esto. — ✅ 2026-11-27

### B5 — Módulo `conectores_mongo_spark.py` (12–19 nov)

- [x] **B5.1** Conexiones: `estado() -> dict` (`{"MongoDB": bool, "Spark": bool}`), cliente Mongo cacheado; lanzador de jobs Spark vía `docker exec` (D-04). — ✅ 2026-11-27 · + `leer_parquet_hdfs()` (docker exec + tar, sin WebHDFS)
- [x] **B5.2** Registro de consultas: `CONSULTAS_MONGO` (MG-Q01…Q10, MG-P01…P05, código = pipeline JSON) y `CONSULTAS_SPARK` (SP-Q01…Q10, código = SQL); `ejecutar(id, **params) -> DataFrame`; `crud(operacion) -> (resultado, comando_mongosh)`. — ✅ 2026-11-27 · el código mostrado es el `.js` de `mongo/queries/`
- [x] **B5.3** Funciones para el dashboard (todas reciben `Filtros`): — ✅ 2026-11-27 · todas implementadas (+ `serie_temporal`, `comparativa_zonas`, `distribucion_tipos`, `sensores_mongo`); interfaz para A en `docs/tecnico/INTERFAZ_DASHBOARD.md`
  - `lecturas_mongo(f)` → `COLUMNAS_ESTANDAR`, `fuente="MongoDB"`.
  - `kpis_ambiente(f)` → temperatura promedio, PM2.5 promedio, CO, humedad, nº alertas ambientales.
  - `kpis_iot_mongo(f)` → mediciones recibidas, alertas generadas, datos por minuto/hora/día, lecturas por segundo.
  - `incidentes(f)`, `eventos(f)`, `alertas(f)` → tablas para la UI.
  - `spark_top_zonas_pico()`, `spark_analisis(tipo)`, `spark_correlacion()`, `spark_pronostico()` → leídos de PG/HDFS, `fuente="Spark"`.
  - KPIs de Movilidad/Energía (`kpis_movilidad`, `kpis_energia`) si D-03.
- [x] **B5.4** Pruebas — `tests/test_conectores_mongo_spark.py`: contrato respetado en todas las funciones; las 25 consultas corren sin error. — ✅ 2026-11-27 · con Mongo/HDFS/Spark cargados: `pytest` 90 pasan, 0 omitidas

### R — Reto adicional: streaming

- [x] **R1** Spark Structured Streaming con alertas por umbral — ✅ 2026-11-27 · `scripts/streaming_productor.py` (lotes JSON a HDFS `/smartcity/streaming/entrada`, `--episodio`/`--congestion`) + `spark/jobs/streaming_alertas.py` (ventana 1 min con watermark, trigger 10 s, checkpoint en HDFS, upsert en Mongo `stream_ventana` y `alerta`, histórico en HDFS) + página «Streaming en vivo» del dashboard (refresco cada 5 s, iniciar/detener job, enviar lotes). Probado: solo la zona con episodio generó alertas, sin duplicados. Documentado en 08_procesamiento.md §8.6

---

## D — Dashboard Streamlit (AB, 12–21 nov) — `dashboard.py`

> La UI **no** contiene SQL ni pipelines: solo importa `conectores_postgres_hadoop` y `conectores_mongo_spark`.
> Colores por tecnología: PostgreSQL `#336791`, MongoDB `#47A248`, HDFS `#F5B800`, Spark `#E25A1C`.

- [x] **D1** Esqueleto (puede empezar con DataFrames falsos que respeten el contrato): `st.set_page_config(layout="wide")`, barra lateral con `estado()` de las 4 tecnologías (caché 30 s); si un servicio cae, su sección muestra un aviso y el resto sigue. `st.cache_resource` para conexiones, `st.cache_data(ttl=600)` para consultas. — ✅ 2026-11-27 · `dashboard.py` + `dashboard_app/`; módulo A opcional, se integra solo al existir
- [x] **D2** Componentes: `etiqueta_origen(fuente)` (**obligatoria en cada KPI, gráfico, tabla y mapa**), `tarjeta_kpi`, gráficos Plotly (serie, barras, dona, heatmap), tabla con búsqueda y paginación, mapa pydeck (`ScatterplotLayer` sensores + `HeatmapLayer` PM2.5). — ✅ 2026-11-27 · `dashboard_app/componentes.py`
- [x] **D3** Filtros globales con `Filtros` de `comun.py`: obligatorios rango de fechas + perfil horario, zona(s), tipo de medición; recomendados sensor, tipo de evento, fuente, rango de valores. **Aceptación:** cambiar cada filtro cambia KPIs y gráficos (prueba documentada). — ✅ 2026-11-27 · `scripts/prueba_filtros.py` → `docs/resultados/prueba_filtros.md`: 42/42 combinaciones cambian (PostgreSQL, MongoDB y Spark), 0 sin datos; encontró y se corrigieron 3 filtros ignorados
- [x] **D4** Vista principal (una sola pantalla con secciones): KPIs Ambiente (B) + IoT (A: sensores; B: mediciones/alertas/ritmo) + Movilidad/Energía según D-03. — ✅ 2026-11-27 · KPIs del enunciado completos: Ambiente 5/5 (+CO₂), Energía 5/5, IoT 6/6 (sensores y activos/inactivos desde PostgreSQL), Movilidad 4/5 + «tiempo promedio de recorrido» = **sin dato** (decisión: ninguna fuente mide tiempos de viaje; justificar en el documento técnico)
- [x] **D5** 4 gráficos: serie temporal (variable del filtro), comparativa por zonas (barras), distribución por tipo de sensor (dona), **resultado Spark** "Top 3 zonas con mayor incremento de PM2.5 en hora pico" (+ botón "Recalcular"). Tabla de detalle y mapa. — ✅ 2026-11-27 · el mapa usa posiciones de Mongo hasta que A publique `sensores(f)`
- [x] **D6** Sección "Consultas por tecnología": selector PostgreSQL / MongoDB / HDFS / Spark → lista de `Consulta` → muestra título, descripción, **código** (`st.code`), widgets de parámetros, botón Ejecutar → tabla + gráfico + tiempo. Extras: EXPLAIN (PG), CRUD (Mongo), explorador/operaciones/fsck/MapReduce (HDFS), análisis (Spark). — ✅ 2026-11-27 · las 4 tecnologías: PostgreSQL (17 consultas + EXPLAIN ANALYZE), MongoDB (15 + CRUD), HDFS (HD-OPS + 5 MapReduce; panel con estado del clúster, explorador, fsck, subir/borrar en sandbox, lanzar jobs en YARN), Spark (10 + análisis)
- [x] **D7** Página de comparación tecnológica (matriz con justificaciones, gráficos de experimentos, diagrama de arquitectura, respuestas cortas a las 7 preguntas del profesor). — ✅ 2026-11-27 · `pagina_comparacion.py`: matriz y respuestas leídas de los documentos (una sola versión), 7 gráficos E1–E7 con hallazgos calculados en vivo, diagramas (PNG o Mermaid), optimización por motor
- [~] **D8** Pulido y QA: textos en español, formato numérico `1.234.567`, unidades, estados vacíos, Home < 5 s con caché; capturas en `docs/resultados/img/` para el documento. — 🟡 2026-11-27 · recorrido automático de todas las páginas con los datos completos sin errores; Home 0,5 s con caché (✅), 30 s la primera carga con la caché vacía; corregido el estado vacío de la tabla de detalle; faltan las capturas (las toma el equipo junto con el documento); caché del dashboard alargada a 12 h

---

## C — Comparación tecnológica: "la parte más importante" (AB, 18–20 nov)

- [x] **C1** — ✅ 2026-11-27 · `scripts/benchmark_comparacion.py` + `spark/jobs/benchmark_spark.py`: E0–E7 a 100k / 1 M / 5 M, sumas de control idénticas en los 3 motores, mediana de 3 → `benchmark_resultado`, `docs/resultados/benchmarks.csv` y `benchmarks.md` (≈ 35 min). Hallazgo: Spark ×11 más lento que PG con 100k y más rápido con 5 M (E1, E3)
  Experimentos con el **mismo dato lógico** (lecturas zona-hora) en cada motor, a 2–3 escalas (100k / 1 M / 5 M si la máquina lo permite); calentamiento + 3 repeticiones, mediana → `benchmark_resultado` + `docs/resultados/benchmarks.csv`
  | ID | Experimento | Hipótesis | Resp. |
  |---|---|---|---|
  | E1 | Agregación promedio por zona-hora: PG vs Mongo vs Spark | PG/Mongo ganan en pequeño; Spark escala mejor | AB |
  | E2 | Búsqueda puntual (últimas lecturas de un sensor) con índice vs Spark full scan | Spark no sirve para consultas puntuales | AB |
  | E3 | Join relacional (sensor-tipo-categoría-zona-organismo): PG vs `$lookup` | PG superior en relaciones | AB |
  | E4 | Agregar un tipo de sensor nuevo con campos nuevos | Mongo 0 migraciones; PG `ALTER TABLE`/JSONB | B |
  | E5 | Spark con 1 vs 2 workers / 1 vs 2 cores | el tiempo baja con más recursos | B |
  | E6 | HDFS con un DataNode caído (A3.4) | datos siguen disponibles | A |
  | E7 | Índices en PG (A2.5) y Mongo (B2.5) | órdenes de magnitud en consultas selectivas | AB |
- [x] **C2** — ✅ 2026-11-27 · matriz con justificación por celda y 4 matices críticos, tabla dato → tecnología → alternativa descartada, ×100, conclusión
  Matriz comparativa — `docs/tecnico/04_justificacion_tecnologica.md`: la matriz del enunciado con **justificación por celda** apoyada en E1–E7 y matices donde no estemos de acuerdo (análisis crítico, 7 pts); tabla "tipo de dato del proyecto → tecnología → por qué → alternativa descartada"; sección "¿Qué pasa si los datos crecen ×100?".
- [x] **C3** Gráficos de los experimentos (Plotly para la UI + PNG para el documento). — ✅ 2026-11-27 · `dashboard_app/graficos_experimentos.py` (7 figuras en la UI) + `scripts/graficos_benchmarks.py` (PNG; corregidas las etiquetas de E1 en el eje logarítmico). Los PNG para el documento los genera el equipo

---

## E — Documentación, defensa y entrega (AB, 17–25 nov)

- [~] **E1** — 🟡 2026-11-27 · las 10 secciones en `docs/tecnico/` con las cifras de la corrida completa; el documento final lo arma el equipo
  Documento técnico `docs/tecnico/00…10_*.md` con las 10 secciones del enunciado: descripción del problema · fuentes de datos · arquitectura · justificación tecnológica · modelo relacional (A) · modelo documental (B) · estrategia Big Data (A) · procesamiento (B) · resultados · conclusiones. Cada decisión justificada (tabla de Decisiones). Ensamblar en DOCX/PDF (pandoc o Word).
- [x] **E2** — ✅ 2026-11-27 · Mermaid: `arquitectura.md`, `flujo_datos.md`, `estructura_hdfs.md`, `despliegue_docker.md` (+ E-R, documental y flujo Spark) → 7 PNG en `docs/diagramas/img/` con `scripts/exportar_diagramas.py`
  Diagramas finales en `docs/diagramas/`: arquitectura, flujo de datos entre las 4 tecnologías, E-R, modelo documental, estructura HDFS, despliegue Docker (PNG exportados).
- [x] **E3** — ✅ 2026-11-27 · `docs/atributos/ATRIBUTOS.md` (AF con costos Docker vs nube, decisiones económicas, gestión; AC con estrategias de aprendizaje; impacto y retroalimentación) + `DICCIONARIO_DATOS.md` (atributos, tipos, fuentes, restricciones; generado de la base); sin reflexiones individuales (decisión del equipo)
  Documento de atributos — `docs/atributos/ATRIBUTOS.md`: **AF** (costos open source en Docker vs servicios gestionados en la nube, decisiones económicas, gestión del tiempo y del equipo en parejas) y **AC** (tecnologías aprendidas y estrategias de aprendizaje); para cada uno: aplicación, impacto en la sociedad, retroalimentación — ≥ 1 párrafo cada punto, con la experiencia real de cada integrante.
- [ ] **E4** — a cargo del equipo (2026-11-27: se retiró el borrador `docs/defensa/GUION_DEMO.md`)
  Guion de demo — `docs/defensa/GUION_DEMO.md`: conexión PG → consulta PG → consulta Mongo → HDFS (comandos + fsck + nodo caído) / MapReduce → análisis Spark → dashboard; quién habla en cada paso, clics exactos, tiempos; checklist pre-demo (servicios arriba, datos cargados, caché caliente); plan B (`sql/demo/demo_defensa.sql`, capturas, video).
- [ ] **E5** — a cargo del equipo (2026-11-27: se retiró el borrador `docs/defensa/PREGUNTAS_RESPUESTAS.md`; la pestaña «Preguntas del profesor» de la página de comparación lo lee si vuelve a existir)
  Preguntas y respuestas — `docs/defensa/PREGUNTAS_RESPUESTAS.md`: las 7 del enunciado (¿por qué PG?, ¿por qué Mongo?, ¿qué resuelve Hadoop?, ¿por qué Spark y no PG?, ¿×100 volumen?, ¿datos muy variables?, ¿valor de las 4?) + ≥ 15 previsibles (3FN, BRIN, archivos pequeños, gzip no divisible, NameNode como punto único de fallo, transformaciones vs acciones, ¿por qué no solo PG con JSONB?…) con **números de nuestros experimentos**. Cada pareja domina las preguntas de sus tecnologías y conoce las de la otra.
- [x] **E6** — ✅ 2026-11-27 · instalación desde cero en 7 pasos, servicios, estructura, documentación y licencias de las fuentes
  `README.md` final: descripción, arquitectura, requisitos, instalación desde cero, orden de ejecución de los scripts, URLs, estructura del repo, créditos y licencias de las fuentes.
- [~] **E7** — 🟡 2026-11-27 · `docs/resultados/qa_final.md`: clon + venv limpio OK; instalación completa con datos siguiendo el README (descarga sin clave → Mongo → HDFS → Spark → PostgreSQL → MapReduce → experimentos); `pytest` sin omitidas; filtros 42/42; UI sin errores; el ensayo cronometrado lo hace el equipo
  QA final: clonar en otra carpeta y correr todo desde cero siguiendo el README; verificar volúmenes (PG ≥ 100k, Mongo ≥ 500k, Spark ≥ 1 M, total y tamaño); recorrer cada consulta en la UI sin errores; ensayo cronometrado de la defensa.
- [~] **E8** — 🟡 2026-11-27 · `scripts/empaquetar_entrega.py` (zip con `git ls-files`, sin datos ni `.env`); falta el commit final y la etiqueta `v1.0-entrega`
  Entrega: documento técnico, documento de atributos, código (zip sin `data/`, `.venv`, `.env`, o enlace al repo), etiqueta git `v1.0-entrega`.

---

## Revisión contra el enunciado (2026-11-27)

Cobertura del enunciado (`docs/CE3101_Enunciado_Proyecto_II_Semestre_2026.pdf`) que el TODO no detallaba o que sigue abierta:

| Requisito del enunciado | Estado | Dónde / qué falta |
|---|---|---|
| ≥ 2 fuentes de datos, documentar fuente, licencia y descarga | ✅ | OpenAQ + Open-Meteo; `FUENTES.md` con licencias y comandos; `perfil_fuentes.md` con las cifras medidas |
| Volumen ≥ 100 MB y PG ≥ 100k / Mongo ≥ 500k / Spark ≥ 1 M | ✅ | PG 2,63 M · Mongo 1,07 M · Spark 4,38 M; ≈ 450 MB de crudos en HDFS (medido) |
| PG: subconsultas, CTE, ventanas, fechas, índices, vistas, MV, funciones, particiones | ✅ | A1–A2 (17 consultas, CTE recursiva en PG-P07, particiones mensuales, BRIN, 2 MV, 4 funciones, 1 procedimiento) |
| PG: preguntas de ejemplo (zona con más vehículos, hora de mayor congestión) | ✅ | PG-P05 y PG-P06 sobre el tráfico cargado por Spark en `medicion_horaria` |
| Mongo: CRUD, filtros, operadores, proyecciones, orden, Aggregation Framework, índices | ✅ | B1–B2 |
| Hadoop: HDFS (mkdir/put/ls/cat/get/rm), bloques, replicación, tolerancia, MapReduce **o Hive** | ✅ | A3–A4 (demo, tolerancia medida, 5 jobs en YARN sobre el export real) |
| Spark: 8 análisis + 10 consultas, DataFrames + SQL | ✅ | B4.1–B4.3 ejecutados con los datos completos |
| Integración: Spark → PostgreSQL; sensores → Mongo → HDFS → Spark | ✅ | Mongo → HDFS (conteos iguales) → Spark → PostgreSQL (2.632.112 filas en `medicion_horaria`) |
| Dashboard: vista única, KPIs de 4 grupos, 4 gráficos (1 de Spark), filtros, tabla, mapa, etiqueta de origen | ✅ | «Tiempo promedio de recorrido» = sin dato (decidido); prueba de filtros D3: 42/42 con datos reales |
| Dashboard: seleccionar tecnología y ejecutar sus consultas obligatorias | ✅ | PostgreSQL, MongoDB, HDFS/MapReduce y Spark |
| Matriz comparativa justificada con experimentos | ✅ | C1–C3: E0–E7 a 3 escalas + matriz con justificación por celda + gráficos PNG |
| Documento técnico (10 secciones) y diagramas | 🟡 | Las 10 secciones y los 7 diagramas PNG listos; el documento final lo arma el equipo (E1) |
| Documento de atributos (AF, AC) | ✅ | E3 (sin reflexiones individuales, por decisión del equipo) |
| Defensa demostrativa en vivo y preguntas del profesor | 🟡 | Guion, preguntas y ensayo a cargo del equipo (E4, E5, E7) |
| Reto adicional (opcional): streaming con alertas por umbral | ✅ | R1 |

---

## Checklist de la rúbrica (verificar antes de entregar)

- [x] (7) Matriz comparativa con justificación por celda y evidencia experimental — C
- [ ] (14) Dashboard: vista principal, todos los KPIs, 4 gráficos (uno de Spark), filtros, tabla, mapa, etiqueta de origen en todo — D
- [x] (15) Las 10 consultas de PG, Mongo y Spark + las 5 de Hadoop ejecutables desde la UI — A5, B5, D6
- [ ] (10) Documento técnico con las 10 secciones y diagramas — E1, E2
- [ ] (2) Código limpio, modular, documentado y funcional — todos
- [x] (7) PostgreSQL: CTEs (incl. recursiva), funciones de ventana, vistas, MV, particiones, índices justificados — A1, A2
- [x] (7) MongoDB: modelado justificado y agregaciones avanzadas (`$facet`, `$bucket`, `$setWindowFields`, `$lookup`, `$geoNear`) — B1, B2
- [x] (7) HDFS: estructura, comandos, bloques/réplicas, tolerancia a fallos — A3
- [ ] (7) Spark: DataFrames + Spark SQL distribuidos, ETL, 8 análisis, optimizaciones — B3, B4
- [ ] (7) Interfaz agradable con KPIs y filtros funcionales — D
- [ ] (15) Defensa: guion, Q&A con números, plan B — E4, E5
- [x] (2) Documento de atributos AF y AC — E3
