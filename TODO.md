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

| Fase | Nombre | Resp. | Tareas | Hechas | Estado | Fechas |
|---|---|---|---|---|---|---|
| F0 | Entorno Docker (ya existente) | AB | 3 | 2 | 🟡 | ✔ previo |
| F1 | Acuerdos comunes y datos maestros | AB | 6 | 0 | ⚪ | 9–11 nov |
| F2 | Adquisición de datos (OpenAQ + Open-Meteo) | A/B | 4 | 0 | ⚪ | 10–12 nov |
| A1 | PostgreSQL: modelo y DDL | A | 5 | 0 | ⚪ | 10–13 nov |
| A2 | PostgreSQL: consultas, vistas, funciones, optimización | A | 6 | 0 | ⚪ | 13–17 nov |
| A3 | HDFS: estructura, carga y operaciones | A | 5 | 0 | ⚪ | 11–15 nov |
| A4 | Hadoop: MapReduce | A | 4 | 0 | ⚪ | 14–18 nov |
| A5 | Módulo `conectores_postgres_hadoop.py` | A | 4 | 0 | ⚪ | 12–19 nov |
| B1 | MongoDB: modelo, validadores e índices | B | 3 | 0 | ⚪ | 10–12 nov |
| B2 | MongoDB: carga, CRUD, consultas y export | B | 6 | 0 | ⚪ | 11–17 nov |
| B3 | Spark: entorno y ETL | B | 4 | 0 | ⚪ | 12–16 nov |
| B4 | Spark: SQL obligatorias y análisis | B | 4 | 0 | ⚪ | 15–19 nov |
| B5 | Módulo `conectores_mongo_spark.py` | B | 4 | 0 | ⚪ | 12–19 nov |
| D | Dashboard Streamlit | AB | 8 | 0 | ⚪ | 12–21 nov |
| C | Comparación tecnológica (experimentos) | AB | 3 | 0 | ⚪ | 18–20 nov |
| E | Documentación, defensa y entrega | AB | 8 | 0 | ⚪ | 17–25 nov |

Leyenda: ⚪ sin empezar · 🟡 en curso · 🟢 completa · 🔴 bloqueada.

**Ruta crítica:** F1 → F2 → (A1 ∥ B1) → (A3 ∥ B2) → B3 → B4 → D → E.
A2/A4 y B2-consultas pueden avanzar en paralelo en cuanto haya datos cargados. El esqueleto del dashboard (D1–D2) puede empezar tras F1.3 con datos falsos.

---

## Decisiones (registrar aquí; justificar en el documento técnico)

| ID | Decisión | Estado |
|---|---|---|
| D-01 | `contrato.py` no tiene tipos para clima ni distingue contaminantes (todo es `calidad_aire`). **Propuesta:** agregar a `TIPOS_METRICA` `pm25, pm10, co, no2, o3, temperatura, humedad, precipitacion` con `UNIDADES` `µg/m3, µg/m3, µg/m3, µg/m3, µg/m3, °C, %, mm` (se conserva `calidad_aire` como categoría). | ⏳ acordar en F1.1 |
| D-02 | Ciudad de referencia: `contrato.py` exige 5 zonas (Norte/Sur/Centro/Este/Oeste). Elegir **una ciudad real con ≥ 5 estaciones OpenAQ con PM2.5 y ≥ 2 años de historia** (verificar en explore.openaq.org) y asignar cada estación a una zona según su posición respecto al centro. Open-Meteo se consulta en el centroide de cada zona. | ⏳ F1.2 |
| D-03 | KPIs de Movilidad y Energía del enunciado quedan fuera del dominio Ambiente. **Recomendado:** series sintéticas mínimas (`trafico`, `energia`, ya previstas en `contrato.py`) generadas por B con perfil horario y documentadas como sintéticas, para que el dashboard muestre todos los KPIs (criterio de 14 pts). Alternativa: tarjetas "fuera de dominio" con justificación (riesgo de perder puntos). | ⏳ F1.5 |
| D-04 | Spark desde Streamlit: el driver en Windows conectado al master en Docker suele fallar (red y versión de Python). **Recomendado:** los jobs corren con `spark-submit` dentro de `smartcity_spark_master` (repo montado) y escriben resultados en PostgreSQL/HDFS; el dashboard los lee y tiene un botón "Recalcular" que lanza el job con `docker exec`. | ⏳ B3.1 |
| D-05 | El compose usa 1 DataNode con `dfs.replication=1`: no se puede demostrar replicación ni tolerancia a fallos. **Propuesta:** 3 DataNodes y `replication=2`, más ResourceManager + NodeManager para MapReduce. | ⏳ F0.3 |
| D-06 | Datos sintéticos para alcanzar los volúmenes mínimos (PG ≥ 100k, Mongo ≥ 500k, Spark ≥ 1 M, recomendado > 5 M total): sensores IoT virtuales por zona, anclados a los valores reales de OpenAQ/Open-Meteo + ruido + anomalías. Semilla fija `2026`. | ⏳ F1.5 |

---

## F0 — Entorno Docker (ya existe en `docker/`)

- [x] **F0.1** `docker/docker-compose.yml` con PostgreSQL 16, MongoDB 7, HDFS (bde2020 namenode + datanode) y Spark 3.5 (master + worker) — ✅ previo
- [x] **F0.2** Guía `DOCKER_SETUP.md` y scripts init (`postgres-init/01_schema.sql`, `mongo-init/01_init.js`) — ✅ previo
- [ ] **F0.3** Ajustes del compose para lo que pide el enunciado (D-04, D-05) — **A** (Hadoop) + **B** (Spark), un solo PR
  - **Pasos:**
    1. **A:** `datanode1..3` (cada uno con su volumen, `hostname` propio, puertos 9864/9865/9866), `HDFS_CONF_dfs_replication=2`; agregar `resourcemanager`, `nodemanager` (y opcional `historyserver`) con imágenes `bde2020/hadoop-*:2.0.0-hadoop3.2.1-java8` y variables `YARN_CONF_*`/`MAPRED_CONF_*` en `hadoop.env`.
    2. **A:** comprobar `docker exec smartcity_nodemanager python3 --version` (lo necesita Hadoop Streaming). Si no hay Python: plan B = jobs MapReduce en Java (compilar dentro del contenedor). Anotar resultado.
    3. **B:** montar el repo en `spark-master` y `spark-worker` (`../:/opt/smartcity`), segundo worker opcional (`SPARK_WORKER_MEMORY=2G`), y el conector de Mongo (`--packages org.mongodb.spark:mongo-spark-connector_2.12:10.4.x`) + driver JDBC de PostgreSQL.
    4. **Ambas:** probar con ~8 GB de RAM libres; si no alcanza, limitar memoria (`mem_limit`) y documentarlo.
  - **Aceptación:** `hdfs dfsadmin -report` muestra 3 DataNodes vivos; `yarn node -list` 1 nodo; job de ejemplo `pi 2 10` termina; UI :8080 muestra los workers ALIVE.

---

## F1 — Acuerdos comunes y datos maestros (AB, 9–11 nov)

- [ ] **F1.1** Cerrar el contrato de datos (D-01) — **AB**
  - Revisar `contrato.py` juntos, aplicar la ampliación de `TIPOS_METRICA`/`UNIDADES` y anotar el cambio en un comentario de cabecera. A partir de aquí **toda** función que devuelva lecturas al dashboard devuelve `COLUMNAS_ESTANDAR` en UTC y pasa `validar_zonas` + `validar_tipo_metrica`.
  - **Aceptación:** ambas parejas aprueban el PR; `python -c "import contrato"` funciona.
- [ ] **F1.2** Ciudad y zonas (D-02) — **AB**
  - Elegir ciudad, listar estaciones OpenAQ candidatas (id, nombre, lat, lon, parámetros, rango de fechas) y asignarlas a las 5 zonas. Guardar en `config/zonas.yaml` (zona, distrito, lat/lon del centroide, población aprox.) y `config/estaciones_openaq.yaml`.
  - **Aceptación:** cada zona tiene ≥ 1 estación real con PM2.5; mapa rápido de verificación.
- [ ] **F1.3** Módulo compartido `comun.py` (raíz) — **AB**
  - `@dataclass(frozen=True) Filtros`: `fecha_inicio, fecha_fin, perfil_horario` (madrugada 0–5, mañana 6–11, tarde 12–17, noche 18–23, hora_pico 6–8 y 16–19), `zonas, tipos_metrica, id_sensor, tipo_evento, fuente, valor_min, valor_max`.
  - `@dataclass Consulta`: `id` (`PG-Q01`, `MG-Q05`, `HD-MR02`, `SP-Q07`…), `tecnologia` (una de `FUENTES_VALIDAS`), `titulo`, `descripcion`, `codigo` (SQL/pipeline/mapper/SQL Spark para mostrar en la UI), `lenguaje`, `parametros: dict` (nombre → valor por defecto), `funcion: Callable[..., pd.DataFrame]`.
  - `config.py` o sección en `comun.py` con credenciales leídas de `.env` (crear `.env.example` con los valores de `DOCKER_SETUP.md`).
  - **Aceptación:** ambos módulos de conectores importan `Filtros` y `Consulta` de aquí; nadie define los suyos.
- [ ] **F1.4** Catálogo maestro de sensores — **A** (lo consume B)
  - Generar `data/maestros/sensores.csv` (semilla 2026): estaciones OpenAQ reales (`origen=openaq`) + sensores IoT virtuales por zona (calidad del aire y clima; D-06) + los de `trafico`/`energia` si se aprueba D-03. Columnas: `id_sensor` (`S-001`… mismo id en PG, Mongo y HDFS), `nombre, tipo_metrica, categoria, zona, lat, lon, fecha_instalacion, estado, organismo`. ~5 % `inactivo` y ~3 % que **nunca** envían datos (para PG-Q07).
  - **Aceptación:** CSV commiteado (es pequeño); B lo usa en su generador.
- [ ] **F1.5** Plan de volúmenes y datos sintéticos (D-03, D-06) — **AB**
  - Tabla en `docs/datos/VOLUMENES.md`: qué es real, qué es sintético, cuántas filas/documentos se esperan por tecnología y cómo se alcanzan los mínimos (PG ≥ 100k, Mongo ≥ 500k, Spark ≥ 1 M, meta > 5 M total, tamaño ≥ 500 MB).
- [ ] **F1.6** Estructura del repo, dependencias y estilo — **AB**
  - Carpetas: `config/ data/{raw,interim,maestros} sql/{ddl,queries,demo} mongo/{queries} hadoop/{hdfs,mapreduce} spark/{jobs,sql} scripts/ docs/{datos,tecnico,diagramas,resultados,defensa,atributos} tests/`. `data/raw` y `data/interim` en `.gitignore`.
  - `requirements.txt` (fijar `pyspark==3.5.*` igual a la imagen): `pandas, numpy, pyarrow, requests, python-dotenv, pyyaml, psycopg2-binary, sqlalchemy, pymongo, hdfs, docker, pyspark, streamlit, plotly, pydeck, pytest`.
  - **Aceptación:** `pip install -r requirements.txt` en un venv limpio funciona.

---

## F2 — Adquisición de datos (10–12 nov)

> Scripts idempotentes en `scripts/descargar_*.py` (si el archivo ya existe no se descarga de nuevo), con registro
> en `data/raw/MANIFEST.json` (url, fecha, bytes). Respetar límites de las APIs (pausa entre requests, reintento en 429).

- [ ] **F2.1** OpenAQ — **B** — `scripts/descargar_openaq.py`
  - Por cada estación de `config/estaciones_openaq.yaml`: mediciones horarias de `pm25, pm10, co, no2, o3` (las disponibles). Opción 1: API v3 (requiere API key gratuita, en `.env`). Opción 2: archivo público en S3 `openaq-data-archive` (sin key, CSV.gz por estación/día).
  - Guardar en `data/raw/openaq/<estacion>/<anio>.csv`. Anotar licencia y fecha de descarga.
  - **Aceptación:** conteo de filas por estación y rango de fechas; % de nulos.
- [ ] **F2.2** Open-Meteo — **A** — `scripts/descargar_openmeteo.py`
  - API de archivo histórico (`archive-api.open-meteo.com`) por centroide de zona: `temperature_2m, relative_humidity_2m, precipitation, wind_speed_10m` horario, mismo rango que OpenAQ, `timezone=UTC`, bloques de 1 año. Opcional: API de calidad del aire de Open-Meteo como respaldo si alguna zona queda sin estación OpenAQ.
  - Guardar `data/raw/openmeteo/zona_<zona>_<anio>.json`.
  - **Aceptación:** 5 zonas × años esperados, sin huecos grandes (script de validación).
- [ ] **F2.3** Perfilado de fuentes — **AB** — `docs/datos/FUENTES.md`
  - Por fuente: descripción, URL, licencia, cómo se descarga (comando), filas, columnas, nulos, min/max, frecuencia, tamaño en disco, limpieza aplicada y **en qué tecnología termina y por qué**.
- [ ] **F2.4** Generador sintético — **B** — `scripts/generar_sinteticos.py`
  - Sensores IoT virtuales de `sensores.csv`: lectura cada 15 min = valor real horario de su zona (OpenAQ/Open-Meteo interpolado) + sesgo propio + ruido gaussiano; ~1 % anomalías (picos, negativos, sensor pegado), huecos de 1–6 h, y desde cierta fecha un campo nuevo (`firmware 2.x` agrega `o3`, `bateria_pct`) → evolución de esquema. Más `trafico`/`energia` si D-03.
  - Documentar fórmulas y parámetros en `docs/datos/GENERACION_SINTETICA.md` (el enunciado lo exige).
  - **Aceptación:** reproducible (misma semilla → mismos datos); volúmenes de F1.5 alcanzados.

---

## Pareja A — PostgreSQL + Hadoop

### A1 — PostgreSQL: modelo y DDL (10–13 nov)

- [ ] **A1.1** Diagrama E-R y justificación 3FN — `docs/diagramas/er_postgres.md` (Mermaid `erDiagram`) + `docs/tecnico/05_modelo_relacional.md`
  - Mínimo del enunciado: `sensor(id_sensor, nombre, tipo_sensor, fecha_instalacion, estado)`, `ubicacion(id_ubicacion, nombre_zona, distrito, coordenadas)`, `categoria_sensor`, `usuario(id_usuario, nombre, correo)`, `organismo(id_organismo, nombre, responsable)`.
  - Recomendado para 3FN y consultas: `zona` y `distrito` como tablas propias (evita la dependencia transitiva `ubicacion → zona → distrito`), `tipo_sensor` (FK a categoría), `variable` (código, unidad, umbral de alerta), `estacion_openaq` (id externo), `calendario` (fecha, día de semana, feriado), `medicion_horaria` (datos limpios desde Spark), `agg_ambiente_hora_zona`, `spark_kpi_zona`, `spark_anomalia`, `benchmark_resultado`.
  - Por tabla: por qué está en 1FN/2FN/3FN y por qué estos datos van en PostgreSQL (integridad referencial, catálogos estables) — **material de defensa**.
  - **Aceptación:** diagrama renderiza en GitHub; cada entidad mínima tiene todos sus atributos.
- [ ] **A1.2** DDL y restricciones — `sql/ddl/001_tablas.sql`, `002_restricciones.sql`
  - Migrar y completar `docker/postgres-init/01_schema.sql`: PK, FK (`ON DELETE RESTRICT`), `CHECK` (estado ∈ contrato, lat −90..90, lon −180..180, `fecha_instalacion <= current_date`, correo con `@`), `UNIQUE`, `NOT NULL`, `COMMENT ON`. Idempotente (`IF NOT EXISTS`). Script `scripts/pg_init.py` que ejecuta los `.sql` en orden (no depender solo de `docker-entrypoint-initdb.d`, que corre una sola vez).
  - **Aceptación:** ejecutar `pg_init` dos veces sin error; `\d+ sensor` muestra las restricciones.
- [ ] **A1.3** Particionamiento — `sql/ddl/003_particiones.sql`
  - `medicion_horaria PARTITION BY RANGE (timestamp)` mensual + partición `DEFAULT`; función `fn_crear_particiones(desde, hasta)`.
  - **Aceptación:** `EXPLAIN` de una consulta de un mes toca una sola partición.
- [ ] **A1.4** Índices justificados — `sql/ddl/004_indices.sql`
  - Todas las FK; `medicion_horaria (id_sensor, timestamp DESC)`; **BRIN** en `timestamp` (datos insertados en orden temporal → índice diminuto); parcial `sensor (tipo_sensor) WHERE estado='activo'`. Cada uno con comentario `-- justifica: …`.
- [ ] **A1.5** Carga de maestros — `scripts/pg_seed.py`
  - Desde `config/zonas.yaml` y `data/maestros/sensores.csv`: zonas, distritos, ubicaciones, categorías, tipos, variables, organismos (~8, plausibles: Municipalidad, Ministerio de Salud, MINAE, IMN…), usuarios (~40, correos ficticios), sensores, calendario. Idempotente (`ON CONFLICT DO NOTHING`).
  - **Aceptación:** conteos impresos, todas las FK válidas.

### A2 — PostgreSQL: consultas, vistas, funciones y optimización (13–17 nov)

- [ ] **A2.1** Vistas y vistas materializadas — `sql/ddl/005_vistas.sql`
  - `v_sensor_detalle` (sensor + tipo + categoría + ubicación + zona + organismo); `mv_kpi_diario_zona` (PM2.5, temperatura, humedad por zona-día) y `mv_actividad_sensor` (nº mediciones, primera/última) con índice `UNIQUE` para `REFRESH … CONCURRENTLY`.
- [ ] **A2.2** Funciones y procedimientos — `sql/ddl/006_funciones.sql`
  - `fn_sensores_por_organismo()`, `fn_sensores_sin_actividad(p_desde)` ("nunca enviaron" vs "dejaron de enviar"), `fn_kpi_ambiente(p_desde, p_hasta, p_zona)`, `sp_refrescar_mv()`.
- [ ] **A2.3** Consultas obligatorias PG-Q01…Q10 — `sql/queries/PG-Qxx_<nombre>.sql` con encabezado (`id, titulo, tecnicas, parametros`)
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
- [ ] **A2.4** Tablas destino de Spark listas y documentadas para **B** — `medicion_horaria`, `agg_ambiente_hora_zona`, `spark_kpi_zona`, `spark_anomalia` (columnas, tipos, PK) — **bloquea B3.4**.
- [ ] **A2.5** Optimización con `EXPLAIN ANALYZE` — `docs/resultados/postgres_optimizacion.md`
  - 4 casos: con/sin índice (`SET enable_indexscan=off`), poda de particiones, BRIN vs B-tree (tamaño y tiempo), MV vs consulta en vivo. Guardar tiempos en `benchmark_resultado`.
- [ ] **A2.6** Script de demo en vivo — `sql/demo/demo_defensa.sql` (plan B si la UI falla durante la defensa).

### A3 — HDFS: estructura, carga y operaciones (11–15 nov)

- [ ] **A3.1** Estructura — `hadoop/hdfs/01_estructura.sh`
  - `/smartcity/raw/{openaq,openmeteo,sinteticos,mongo_exports}`, `/smartcity/curated`, `/smartcity/analytics`, `/smartcity/mapreduce`, `/smartcity/sandbox`; particiones estilo Hive `anio=/mes=`. Justificar en `docs/tecnico/07_estrategia_big_data.md`.
- [ ] **A3.2** Carga de crudos — `hadoop/hdfs/02_cargar_raw.sh` (`docker cp` + `hdfs dfs -put -f`)
  - Unir archivos pequeños antes de subir (p. ej. un CSV por mes) → **problema de archivos pequeños** (argumento de defensa). Sin gzip en los CSV grandes (no es divisible); justificar.
  - Nota: WebHDFS desde Windows redirige a `datanodeN:9864`; agregar esos nombres a `C:\Windows\System32\drivers\etc\hosts` → `127.0.0.1` o subir siempre vía `docker exec`.
  - **Aceptación:** `hdfs dfs -du -h /smartcity/raw` = tamaños locales.
- [ ] **A3.3** Demostración de comandos — `hadoop/hdfs/03_demo.sh` → `docs/resultados/hdfs_demo.txt`
  - `-mkdir, -put, -ls, -cat | head, -get, -rm, -du -h, -stat, -setrep`; `hdfs fsck <archivo> -files -blocks -locations` (bloques y en qué DataNode está cada réplica); subir el mismo archivo con `-D dfs.blocksize=33554432` para comparar nº de bloques.
- [ ] **A3.4** Tolerancia a fallos — `hadoop/hdfs/04_tolerancia.ps1` → `docs/resultados/hdfs_tolerancia.md`
  - `fsck` inicial → `docker stop smartcity_datanode2` → leer el archivo completo (sigue disponible) → `dfsadmin -report` (2 vivos) y `fsck` (re-replicación) → `docker start`. Medir tiempos. **Momento fuerte de la demo.**
- [ ] **A3.5** Documentar Hadoop — `docs/tecnico/07_estrategia_big_data.md`: almacenamiento distribuido, bloques, replicación, tolerancia a fallos, escalabilidad (agregar DataNodes), localidad de datos, por qué este problema justifica un sistema distribuido (con cifras propias), qué pasa si el volumen crece ×100.

### A4 — Hadoop: MapReduce (14–18 nov)

- [ ] **A4.1** Infraestructura de jobs — `hadoop/mapreduce/run_job.sh <job> <input> <output>` (Hadoop Streaming con Python; borra la salida previa). Cada job con `mapper.py`, `reducer.py`, `README.md` y prueba local `cat muestra | python mapper.py | sort | python reducer.py`.
- [ ] **A4.2** Los 5 jobs de las preguntas del enunciado
  | Job | Pregunta | Entrada |
  |---|---|---|
  | `mr01` | ¿Cuántas mediciones existen por sensor? | raw OpenAQ + sintéticos (combiner = reducer) |
  | `mr02` | ¿Qué sensor produjo más datos? | salida de mr01 (1 reducer, top-N) |
  | `mr03` | Temperatura promedio histórica (global, por zona/año) | raw Open-Meteo (emitir suma y conteo, no promedios) |
  | `mr04` | Cantidad de eventos por zona | `mongo_exports/evento` (JSON Lines, **depende de B2.6**) |
  | `mr05` | Comportamiento de los sensores en un período | raw, `-cmdenv DESDE= HASTA=` → promedio diario por sensor |
  - **Aceptación:** los 5 terminan en YARN (:8088), tiempos anotados en `docs/resultados/mapreduce_tiempos.md`.
- [ ] **A4.3** Ejecución desde Python — funciones `mr_ejecutar(job)` (vía `docker exec`, SDK `docker`) y `mr_resultado(job)` (lee la salida de HDFS como DataFrame).
- [ ] **A4.4** Pruebas unitarias de mappers/reducers — `tests/test_mapreduce.py` con `data/samples/`.

### A5 — Módulo `conectores_postgres_hadoop.py` (12–19 nov)

- [ ] **A5.1** Conexiones: `estado() -> dict` (`{"PostgreSQL": bool, "HDFS": bool}` + latencia), engine SQLAlchemy cacheado, cliente WebHDFS, `ejecutar_en_namenode(cmd) -> (codigo, salida)`.
- [ ] **A5.2** Registro de consultas: `CONSULTAS_PG` y `CONSULTAS_HDFS: dict[str, Consulta]` (de `comun.py`): PG-Q01…Q10, PG-P01…P04, HD-OPS (ls/cat/fsck), HD-MR01…MR05; `ejecutar(id, **params) -> DataFrame`; `explain(id, **params) -> str`.
- [ ] **A5.3** Funciones para el dashboard (todas reciben `Filtros`):
  - `sensores(f)` → catálogo con lat/lon y estado (pasa `validar_estado`) — mapa y KPIs IoT.
  - `kpis_iot(f)` → nº sensores, activos/inactivos.
  - `lecturas_pg(f)` → `COLUMNAS_ESTANDAR` desde `medicion_horaria`, `fuente="PostgreSQL"`.
  - `lecturas_hdfs(f)` / `resumen_hdfs()` → `fuente="HDFS"` (salidas MapReduce, estado del clúster desde JMX: DataNodes vivos, capacidad, bloques).
  - Operaciones sandbox: `hdfs_ls, hdfs_cat, hdfs_put, hdfs_get, hdfs_rm, hdfs_mkdir, hdfs_fsck` devolviendo también el comando `hdfs dfs` equivalente (para mostrarlo en la UI).
- [ ] **A5.4** Pruebas — `tests/test_conectores_pg_hadoop.py`: cada función devuelve las columnas del contrato, zonas válidas, `timestamp` UTC; las 14 + 6 consultas corren sin error.

---

## Pareja B — MongoDB + Spark

### B1 — MongoDB: modelo, validadores e índices (10–12 nov)

- [ ] **B1.1** Modelo documental y justificación NoSQL — `docs/diagramas/modelo_documental.md` + `docs/tecnico/06_modelo_documental.md`
  - Colecciones que exige el enunciado (ampliando `mediciones/eventos/alertas` de `mongo-init/01_init.js`):
    - `lecturas` — `id_sensor` (= PG), `tipo_metrica`, `timestamp` (Date, UTC), `ubicacion {zona, geo: {type:"Point", coordinates:[lon,lat]}}`, `origen` (`openaq`/`sintetico`), `mediciones {…}` **distinto por tipo** (aire `{pm25, pm10, co, no2, o3?}`; clima `{temperatura, humedad, precipitacion, viento?}`), opcionales `dispositivo {firmware, bateria_pct}`, `calidad_dato {valido, flags[]}`.
    - `eventos` — contaminación alta, lluvia intensa, ola de calor, falla de sensor; `detalles{}` distinto por tipo.
    - `alertas` — `regla, variable, valor, umbral, nivel, id_sensor, zona, emitida, estado, acciones[]`.
    - `incidentes` — **texto libre** (quemas, humo, incendios forestales, inundaciones, olores…): `categoria, titulo, descripcion, historial[], reportes_ciudadanos[]`.
  - Justificar: embebido vs referencia, por qué sería incómodo en relacional (columnas dispersas/NULL, migraciones por cada tipo/firmware nuevo, arrays anidados), y cuándo **no** usar Mongo.
- [ ] **B1.2** Validadores e índices — `mongo/01_colecciones.js`, `mongo/02_indices.js` (+ script Python equivalente idempotente)
  - `$jsonSchema` flexible (solo exige `id_sensor, tipo_metrica, timestamp, ubicacion`), `validationLevel: moderate`.
  - Índices: `{id_sensor:1, timestamp:-1}`, `{tipo_metrica:1, "ubicacion.zona":1, timestamp:-1}`, `2dsphere` en `ubicacion.geo`, `text` en `incidentes {titulo, descripcion}`, parcial en `alertas {estado:"abierta"}`; opcional TTL en una colección temporal.
  - **Aceptación:** insertar un documento sin `id_sensor` falla; `getIndexes()` los lista.
- [ ] **B1.3** Umbrales de alerta en `config/umbrales.yaml` (PM2.5 35,4 / 55,4 / 150,4 µg/m³; temperatura; precipitación) — usados por alertas, Spark y streaming.

### B2 — MongoDB: carga, CRUD, consultas y export (11–17 nov)

- [ ] **B2.1** Carga de lecturas — `scripts/mongo_cargar.py`: OpenAQ real + sintéticos (F2.4), `insert_many(ordered=False)` en lotes de 10k, barra de progreso, `--limite` para pruebas.
  - **Aceptación:** `db.lecturas.countDocuments()` ≥ 500.000; tiempo de carga anotado.
- [ ] **B2.2** Eventos, alertas e incidentes **derivados de las lecturas** (alertas = lecturas que superan umbrales, coherentes con los datos); incidentes con plantillas de texto en español por categoría.
- [ ] **B2.3** Demo CRUD — `mongo/03_crud_demo.js` + función Python: `insertOne, insertMany, find` (filtro + proyección + sort + limit), `findOne, updateOne` (`$set` + `$push`), `updateMany` (`$gt/$lt/$or`), `deleteOne` sobre una colección `demo_crud` (nunca sobre `lecturas`). Cada operación devuelve resultado + comando mongosh (para la UI).
- [ ] **B2.4** Consultas obligatorias MG-Q01…Q10 — `mongo/queries/MG-Qxx.js` (mismo pipeline que en Python)
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
- [ ] **B2.5** Experimento de índices — `explain("executionStats")` de MG-Q01/Q02/Q05 con y sin índice (`hint({$natural:1})`): `totalDocsExamined`, tiempo, `IXSCAN` vs `COLLSCAN` → `docs/resultados/mongo_indices.md`.
- [ ] **B2.6** Export Mongo → HDFS — job Spark (conector Mongo) que escribe JSON Lines en `/smartcity/raw/mongo_exports/<coleccion>/fecha=…/` (alternativa: `mongoexport` + `hdfs dfs -put`). Conteos iguales a Mongo. **Lo necesita A4.2 (mr04).**

### B3 — Spark: entorno y ETL (12–16 nov)

- [ ] **B3.1** Entorno Spark (D-04): `spark/jobs/common.py` con `get_spark()` (master `spark://spark-master:7077`, `fs.defaultFS=hdfs://namenode:8020`, conector Mongo, JDBC PostgreSQL, `spark.sql.session.timeZone=UTC`) + `scripts/spark_submit.ps1 <job>` que ejecuta `docker exec smartcity_spark_master spark-submit /opt/smartcity/spark/jobs/<job>.py`.
  - **Aceptación:** job trivial `spark.range(1_000_000).count()` aparece en la UI :8080 ejecutado por los workers; lee un archivo de HDFS y una colección de Mongo.
- [ ] **B3.2** ETL ambiente — `spark/jobs/etl_ambiente.py`
  - raw OpenAQ + Open-Meteo + `mongo_exports/lecturas` (HDFS) → esquemas explícitos (`StructType`, nunca `inferSchema`) → limpieza (nulos, negativos, duplicados por sensor+timestamp, fuera de rango) → formato largo `zona, timestamp, id_sensor, tipo_metrica, valor, fuente` (igual al contrato) → join con zonas/sensores de PG (broadcast) → `/smartcity/curated/ambiente/` Parquet particionado por `anio/mes`.
  - **Aceptación:** ≥ 1 M filas procesadas; % descartado documentado; job usa los workers.
- [ ] **B3.3** ETL de `trafico`/`energia` sintéticos (solo si D-03 aprobado), mismo patrón.
- [ ] **B3.4** Carga a PostgreSQL — `spark/jobs/carga_pg.py` (**depende de A2.4**)
  - Agregados por sensor-variable-hora → `medicion_horaria` (vía tabla staging + `INSERT … ON CONFLICT DO NOTHING`), por zona-hora → `agg_ambiente_hora_zona`; al final `CALL sp_refrescar_mv()`.
  - **Aceptación:** conteos Spark = PostgreSQL; re-ejecutar no duplica; `medicion_horaria` ≥ 100k filas (mínimo PG del enunciado).

### B4 — Spark: SQL obligatorias y análisis (15–19 nov)

- [ ] **B4.1** Consultas obligatorias SP-Q01…Q10 — `spark/sql/SP-Qxx.sql` sobre vistas temporales (`ambiente`, `lecturas`, `eventos`, `zona`, `sensor`)
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
- [ ] **B4.2** Los 8 análisis obligatorios — `spark/jobs/analitica.py` (DataFrame API), resultados en `/smartcity/analytics/<tipo>/` y tablas `spark_*`
  - **Descriptivo** (sensores más activos, estadísticos por variable) · **Temporal** (evolución mensual de PM2.5, curva diaria de temperatura) · **Correlacional** (matriz PM2.5–PM10–temperatura–humedad–precipitación–viento) · **Predictivo** (MLlib `LinearRegression`/`GBTRegressor`: PM2.5 diario por zona según clima; reportar RMSE/R² y pronóstico) · **Espacial** (zonas más contaminadas, hotspots) · **Movilidad** (horas pico de contaminación por zona × día de semana; con tráfico real si D-03) · **Anomalías** (z-score + IQR por sensor) · **Comparación** (perfil por zona; **"Top 3 zonas con mayor incremento de PM2.5 en hora pico vs valle"** → gráfico Spark del dashboard).
  - **Aceptación:** `docs/resultados/hallazgos_spark.md` con números concretos.
- [ ] **B4.3** Optimización Spark (evidencia) — `docs/resultados/spark_optimizacion.md`: CSV vs Parquet (tiempo y bytes), *partition pruning* (`explain()` → `PartitionFilters`), broadcast vs sort-merge join, `cache()`, AQE on/off. Capturas de la Spark UI.
- [ ] **B4.4** Documentar — `docs/tecnico/08_procesamiento.md`: flujo, cada job (entrada → transformaciones → salida), transformaciones vs acciones, evaluación perezosa, particionamiento, **por qué Spark y no PostgreSQL** para esto.

### B5 — Módulo `conectores_mongo_spark.py` (12–19 nov)

- [ ] **B5.1** Conexiones: `estado() -> dict` (`{"MongoDB": bool, "Spark": bool}`), cliente Mongo cacheado; lanzador de jobs Spark vía `docker exec` (D-04).
- [ ] **B5.2** Registro de consultas: `CONSULTAS_MONGO` (MG-Q01…Q10, MG-P01…P05, código = pipeline JSON) y `CONSULTAS_SPARK` (SP-Q01…Q10, código = SQL); `ejecutar(id, **params) -> DataFrame`; `crud(operacion) -> (resultado, comando_mongosh)`.
- [ ] **B5.3** Funciones para el dashboard (todas reciben `Filtros`):
  - `lecturas_mongo(f)` → `COLUMNAS_ESTANDAR`, `fuente="MongoDB"`.
  - `kpis_ambiente(f)` → temperatura promedio, PM2.5 promedio, CO, humedad, nº alertas ambientales.
  - `kpis_iot_mongo(f)` → mediciones recibidas, alertas generadas, datos por minuto/hora/día, lecturas por segundo.
  - `incidentes(f)`, `eventos(f)`, `alertas(f)` → tablas para la UI.
  - `spark_top_zonas_pico()`, `spark_analisis(tipo)`, `spark_correlacion()`, `spark_pronostico()` → leídos de PG/HDFS, `fuente="Spark"`.
  - KPIs de Movilidad/Energía (`kpis_movilidad`, `kpis_energia`) si D-03.
- [ ] **B5.4** Pruebas — `tests/test_conectores_mongo_spark.py`: contrato respetado en todas las funciones; las 25 consultas corren sin error.

---

## D — Dashboard Streamlit (AB, 12–21 nov) — `dashboard.py`

> La UI **no** contiene SQL ni pipelines: solo importa `conectores_postgres_hadoop` y `conectores_mongo_spark`.
> Colores por tecnología: PostgreSQL `#336791`, MongoDB `#47A248`, HDFS `#F5B800`, Spark `#E25A1C`.

- [ ] **D1** Esqueleto (puede empezar con DataFrames falsos que respeten el contrato): `st.set_page_config(layout="wide")`, barra lateral con `estado()` de las 4 tecnologías (caché 30 s); si un servicio cae, su sección muestra un aviso y el resto sigue. `st.cache_resource` para conexiones, `st.cache_data(ttl=600)` para consultas.
- [ ] **D2** Componentes: `etiqueta_origen(fuente)` (**obligatoria en cada KPI, gráfico, tabla y mapa**), `tarjeta_kpi`, gráficos Plotly (serie, barras, dona, heatmap), tabla con búsqueda y paginación, mapa pydeck (`ScatterplotLayer` sensores + `HeatmapLayer` PM2.5).
- [ ] **D3** Filtros globales con `Filtros` de `comun.py`: obligatorios rango de fechas + perfil horario, zona(s), tipo de medición; recomendados sensor, tipo de evento, fuente, rango de valores. **Aceptación:** cambiar cada filtro cambia KPIs y gráficos (prueba documentada).
- [ ] **D4** Vista principal (una sola pantalla con secciones): KPIs Ambiente (B) + IoT (A: sensores; B: mediciones/alertas/ritmo) + Movilidad/Energía según D-03.
- [ ] **D5** 4 gráficos: serie temporal (variable del filtro), comparativa por zonas (barras), distribución por tipo de sensor (dona), **resultado Spark** "Top 3 zonas con mayor incremento de PM2.5 en hora pico" (+ botón "Recalcular"). Tabla de detalle y mapa.
- [ ] **D6** Sección "Consultas por tecnología": selector PostgreSQL / MongoDB / HDFS / Spark → lista de `Consulta` → muestra título, descripción, **código** (`st.code`), widgets de parámetros, botón Ejecutar → tabla + gráfico + tiempo. Extras: EXPLAIN (PG), CRUD (Mongo), explorador/operaciones/fsck/MapReduce (HDFS), análisis (Spark).
- [ ] **D7** Página de comparación tecnológica (matriz con justificaciones, gráficos de experimentos, diagrama de arquitectura, respuestas cortas a las 7 preguntas del profesor).
- [ ] **D8** Pulido y QA: textos en español, formato numérico `1.234.567`, unidades, estados vacíos, Home < 5 s con caché; capturas en `docs/resultados/img/` para el documento.

---

## C — Comparación tecnológica: "la parte más importante" (AB, 18–20 nov)

- [ ] **C1** Experimentos con el **mismo dato lógico** (lecturas zona-hora) en cada motor, a 2–3 escalas (100k / 1 M / 5 M si la máquina lo permite); calentamiento + 3 repeticiones, mediana → `benchmark_resultado` + `docs/resultados/benchmarks.csv`
  | ID | Experimento | Hipótesis | Resp. |
  |---|---|---|---|
  | E1 | Agregación promedio por zona-hora: PG vs Mongo vs Spark | PG/Mongo ganan en pequeño; Spark escala mejor | AB |
  | E2 | Búsqueda puntual (últimas lecturas de un sensor) con índice vs Spark full scan | Spark no sirve para consultas puntuales | AB |
  | E3 | Join relacional (sensor-tipo-categoría-zona-organismo): PG vs `$lookup` | PG superior en relaciones | AB |
  | E4 | Agregar un tipo de sensor nuevo con campos nuevos | Mongo 0 migraciones; PG `ALTER TABLE`/JSONB | B |
  | E5 | Spark con 1 vs 2 workers / 1 vs 2 cores | el tiempo baja con más recursos | B |
  | E6 | HDFS con un DataNode caído (A3.4) | datos siguen disponibles | A |
  | E7 | Índices en PG (A2.5) y Mongo (B2.5) | órdenes de magnitud en consultas selectivas | AB |
- [ ] **C2** Matriz comparativa — `docs/tecnico/04_justificacion_tecnologica.md`: la matriz del enunciado con **justificación por celda** apoyada en E1–E7 y matices donde no estemos de acuerdo (análisis crítico, 7 pts); tabla "tipo de dato del proyecto → tecnología → por qué → alternativa descartada"; sección "¿Qué pasa si los datos crecen ×100?".
- [ ] **C3** Gráficos de los experimentos (Plotly para la UI + PNG para el documento).

---

## E — Documentación, defensa y entrega (AB, 17–25 nov)

- [ ] **E1** Documento técnico `docs/tecnico/00…10_*.md` con las 10 secciones del enunciado: descripción del problema · fuentes de datos · arquitectura · justificación tecnológica · modelo relacional (A) · modelo documental (B) · estrategia Big Data (A) · procesamiento (B) · resultados · conclusiones. Cada decisión justificada (tabla de Decisiones). Ensamblar en DOCX/PDF (pandoc o Word).
- [ ] **E2** Diagramas finales en `docs/diagramas/`: arquitectura, flujo de datos entre las 4 tecnologías, E-R, modelo documental, estructura HDFS, despliegue Docker (PNG exportados).
- [ ] **E3** Documento de atributos — `docs/atributos/ATRIBUTOS.md`: **AF** (costos open source en Docker vs servicios gestionados en la nube, decisiones económicas, gestión del tiempo y del equipo en parejas) y **AC** (tecnologías aprendidas y estrategias de aprendizaje); para cada uno: aplicación, impacto en la sociedad, retroalimentación — ≥ 1 párrafo cada punto, con la experiencia real de cada integrante.
- [ ] **E4** Guion de demo — `docs/defensa/GUION_DEMO.md`: conexión PG → consulta PG → consulta Mongo → HDFS (comandos + fsck + nodo caído) / MapReduce → análisis Spark → dashboard; quién habla en cada paso, clics exactos, tiempos; checklist pre-demo (servicios arriba, datos cargados, caché caliente); plan B (`sql/demo/demo_defensa.sql`, capturas, video).
- [ ] **E5** Preguntas y respuestas — `docs/defensa/PREGUNTAS_RESPUESTAS.md`: las 7 del enunciado (¿por qué PG?, ¿por qué Mongo?, ¿qué resuelve Hadoop?, ¿por qué Spark y no PG?, ¿×100 volumen?, ¿datos muy variables?, ¿valor de las 4?) + ≥ 15 previsibles (3FN, BRIN, archivos pequeños, gzip no divisible, NameNode como punto único de fallo, transformaciones vs acciones, ¿por qué no solo PG con JSONB?…) con **números de nuestros experimentos**. Cada pareja domina las preguntas de sus tecnologías y conoce las de la otra.
- [ ] **E6** `README.md` final: descripción, arquitectura, requisitos, instalación desde cero, orden de ejecución de los scripts, URLs, estructura del repo, créditos y licencias de las fuentes.
- [ ] **E7** QA final: clonar en otra carpeta y correr todo desde cero siguiendo el README; verificar volúmenes (PG ≥ 100k, Mongo ≥ 500k, Spark ≥ 1 M, total y tamaño); recorrer cada consulta en la UI sin errores; ensayo cronometrado de la defensa.
- [ ] **E8** Entrega: documento técnico, documento de atributos, código (zip sin `data/`, `.venv`, `.env`, o enlace al repo), etiqueta git `v1.0-entrega`.

---

## Checklist de la rúbrica (verificar antes de entregar)

- [ ] (7) Matriz comparativa con justificación por celda y evidencia experimental — C
- [ ] (14) Dashboard: vista principal, todos los KPIs, 4 gráficos (uno de Spark), filtros, tabla, mapa, etiqueta de origen en todo — D
- [ ] (15) Las 10 consultas de PG, Mongo y Spark + las 5 de Hadoop ejecutables desde la UI — A5, B5, D6
- [ ] (10) Documento técnico con las 10 secciones y diagramas — E1, E2
- [ ] (2) Código limpio, modular, documentado y funcional — todos
- [ ] (7) PostgreSQL: CTEs (incl. recursiva), funciones de ventana, vistas, MV, particiones, índices justificados — A1, A2
- [ ] (7) MongoDB: modelado justificado y agregaciones avanzadas (`$facet`, `$bucket`, `$setWindowFields`, `$lookup`, `$geoNear`) — B1, B2
- [ ] (7) HDFS: estructura, comandos, bloques/réplicas, tolerancia a fallos — A3
- [ ] (7) Spark: DataFrames + Spark SQL distribuidos, ETL, 8 análisis, optimizaciones — B3, B4
- [ ] (7) Interfaz agradable con KPIs y filtros funcionales — D
- [ ] (15) Defensa: guion, Q&A con números, plan B — E4, E5
- [ ] (2) Documento de atributos AF y AC — E3
