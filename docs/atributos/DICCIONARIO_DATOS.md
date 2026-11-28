# Diccionario de datos — SmartCity Analytics

Generado por `scripts/generar_diccionario.py` el 2026-11-27 21:04 UTC. Para cada atributo: tipo, si admite nulos, restricciones y fuente.

## 1. PostgreSQL (`smartcity_db`, esquema `public`)

Generado en vivo desde el catálogo de la base. `medicion_horaria` está particionada por mes (24 particiones 2023-01…2024-12 + `DEFAULT`); las particiones heredan estas columnas y restricciones.


### `agg_ambiente_hora_zona`

Agregado zona-hora calculado por Spark (B3.4).  
**Fuente:** Spark `carga_pg.py` (agregado zona-hora)

| Atributo | Tipo | Nulo | Restricciones | Descripción |
|---|---|---|---|---|
| `zona` | varchar(100) | no | FK → ubicaciones(nombre_zona) ON DELETE RESTRICT<br>PK (zona, timestamp) |  |
| `timestamp` | timestamptz | no | PK (zona, timestamp) |  |
| `pm25_promedio` | double precision | sí | — |  |
| `temperatura_promedio` | double precision | sí | — |  |
| `humedad_promedio` | double precision | sí | — |  |
| `total_mediciones` | bigint | no | DEFAULT 0 |  |

### `benchmark_resultado`

Tiempos de los experimentos (mediana de repeticiones).  
**Fuente:** `pg_optimizacion.py` (A2.5) y `benchmark_comparacion.py` (C1)

| Atributo | Tipo | Nulo | Restricciones | Descripción |
|---|---|---|---|---|
| `id` | integer | no | PK<br>DEFAULT nextval('benchmark_resultado_id_seq'::regclass) |  |
| `experimento` | varchar(20) | no | — |  |
| `tecnologia` | varchar(20) | no | CHECK (((tecnologia)::text = ANY ((ARRAY['PostgreSQL'::character varying, 'MongoDB'::character varying, 'HDFS'::character varying, 'Spark'::character varying])::text[]))) |  |
| `variante` | varchar(80) | no | — |  |
| `escala_filas` | bigint | sí | — |  |
| `segundos` | double precision | no | CHECK ((segundos >= (0)::double precision)) |  |
| `detalle` | text | sí | — |  |
| `medido_en` | timestamptz | no | DEFAULT now() |  |

### `calendario`

Dimensión de fechas 2023–2024 con feriados de Inglaterra (Londres).  
**Fuente:** Generado en `006_calendario.sql` (feriados de Inglaterra 2023–2024)

| Atributo | Tipo | Nulo | Restricciones | Descripción |
|---|---|---|---|---|
| `fecha` | date | no | PK |  |
| `anio` | smallint | no | — |  |
| `mes` | smallint | no | CHECK (((mes >= 1) AND (mes <= 12))) |  |
| `dia` | smallint | no | CHECK (((dia >= 1) AND (dia <= 31))) |  |
| `dia_semana` | smallint | no | CHECK (((dia_semana >= 1) AND (dia_semana <= 7))) |  |
| `nombre_dia` | varchar(10) | no | — |  |
| `es_fin_de_semana` | boolean | no | — |  |
| `es_feriado` | boolean | no | DEFAULT false |  |
| `nombre_feriado` | varchar(80) | sí | — |  |

### `categorias_sensor`

Categorías de sensores (Calidad del aire, Clima, Movilidad, Energía).  
**Fuente:** Semilla en `001_tablas.sql`

| Atributo | Tipo | Nulo | Restricciones | Descripción |
|---|---|---|---|---|
| `id_categoria` | integer | no | PK<br>DEFAULT nextval('categorias_sensor_id_categoria_seq'::regclass) |  |
| `nombre_categoria` | varchar(100) | no | UNIQUE |  |

### `medicion_horaria`

Promedio horario por sensor y variable; particionada por mes; la carga Spark (B3.4).  
**Fuente:** Spark `carga_pg.py` (B3.4) desde el Parquet curado

| Atributo | Tipo | Nulo | Restricciones | Descripción |
|---|---|---|---|---|
| `id_sensor` | varchar(20) | no | FK → sensores(id_sensor) ON DELETE RESTRICT<br>PK (id_sensor, timestamp, variable) |  |
| `timestamp` | timestamptz | no | PK (id_sensor, timestamp, variable) |  |
| `variable` | varchar(40) | no | CHECK ((((variable)::text <> 'humedad'::text) OR ((valor >= (0)::double precision) AND (valor <= (100)::double precision))))<br>CHECK ((((variable)::text = 'temperatura'::text) OR (valor >= (0)::double precision)))<br>FK → variables_medicion(codigo) ON DELETE RESTRICT<br>PK (id_sensor, timestamp, variable) |  |
| `valor` | double precision | no | CHECK ((((variable)::text <> 'humedad'::text) OR ((valor >= (0)::double precision) AND (valor <= (100)::double precision))))<br>CHECK ((((variable)::text = 'temperatura'::text) OR (valor >= (0)::double precision)))<br>CHECK ((valor <> 'NaN'::double precision)) |  |
| `origen` | varchar(30) | no | CHECK (((origen)::text = ANY ((ARRAY['openaq'::character varying, 'open-meteo'::character varying, 'sintetico'::character varying, 'prueba_pg'::character varying])::text[])))<br>DEFAULT 'sintetico'::character varying | openaq / open-meteo / sintetico / prueba_pg (datos de prueba de scripts/pg_generar_mediciones.py) |

### `organismos`

Organismos responsables de los sensores.  
**Fuente:** Catálogo ficticio plausible — `pg_cargar_maestros.py`

| Atributo | Tipo | Nulo | Restricciones | Descripción |
|---|---|---|---|---|
| `id_organismo` | integer | no | PK<br>DEFAULT nextval('organismos_id_organismo_seq'::regclass) |  |
| `nombre` | varchar(150) | no | UNIQUE |  |
| `responsable` | varchar(150) | sí | — |  |

### `sensores`

Catálogo maestro de sensores; id_sensor es el mismo en MongoDB, HDFS y Spark.  
**Fuente:** `data/sensores.csv` (estaciones OpenAQ + virtuales + T/E) — `pg_cargar_maestros.py`

| Atributo | Tipo | Nulo | Restricciones | Descripción |
|---|---|---|---|---|
| `id_sensor` | varchar(20) | no | PK |  |
| `nombre` | varchar(150) | no | — |  |
| `tipo_sensor` | varchar(50) | no | FK → tipos_sensor(tipo_sensor) ON DELETE RESTRICT |  |
| `fecha_instalacion` | date | sí | CHECK (((fecha_baja IS NULL) OR (fecha_instalacion IS NULL) OR (fecha_baja >= fecha_instalacion)))<br>CHECK ((fecha_instalacion <= CURRENT_DATE)) |  |
| `estado` | varchar(20) | no | CHECK (((estado)::text = ANY ((ARRAY['activo'::character varying, 'inactivo'::character varying])::text[])))<br>CHECK (((fecha_baja IS NULL) OR ((estado)::text = 'inactivo'::text)))<br>DEFAULT 'activo'::character varying | activo / inactivo (contrato.ESTADOS_SENSOR) |
| `id_ubicacion` | integer | no | FK → ubicaciones(id_ubicacion) ON DELETE RESTRICT |  |
| `id_organismo` | integer | sí | FK → organismos(id_organismo) ON DELETE RESTRICT |  |
| `origen` | varchar(30) | sí | CHECK (((origen)::text = ANY ((ARRAY['openaq'::character varying, 'open-meteo'::character varying, 'sintetico'::character varying])::text[])))<br>DEFAULT 'sintetico'::character varying | openaq / open-meteo / sintetico |
| `envia_datos` | boolean | sí | DEFAULT true | false = sensor registrado que nunca envió datos (PG-Q07) |
| `fecha_baja` | date | sí | CHECK (((fecha_baja IS NULL) OR (fecha_instalacion IS NULL) OR (fecha_baja >= fecha_instalacion)))<br>CHECK (((fecha_baja IS NULL) OR ((estado)::text = 'inactivo'::text))) | Fecha desde la que dejó de enviar datos (solo sensores inactivos) |
| `id_estacion_openaq` | varchar(30) | sí | — |  |
| `frecuencia_min` | integer | sí | CHECK ((frecuencia_min > 0))<br>DEFAULT 60 |  |
| `latitud` | numeric(9,6) | sí | CHECK ((((latitud IS NULL) OR ((latitud >= ('-90'::integer)::numeric) AND (latitud <= (90)::numeric))) AND ((longitud IS NULL) OR ((longitud >= ('-180'::integer)::numeric) AND (longitud <= (180)::numeric))))) |  |
| `longitud` | numeric(9,6) | sí | CHECK ((((latitud IS NULL) OR ((latitud >= ('-90'::integer)::numeric) AND (latitud <= (90)::numeric))) AND ((longitud IS NULL) OR ((longitud >= ('-180'::integer)::numeric) AND (longitud <= (180)::numeric))))) |  |

### `spark_anomalia`

Lecturas anómalas detectadas por Spark (marca de calidad, z-score, IQR).  
**Fuente:** Spark `analitica.py --analisis anomalias` vía `carga_pg.py`

| Atributo | Tipo | Nulo | Restricciones | Descripción |
|---|---|---|---|---|
| `id_sensor` | varchar(20) | no | FK → sensores(id_sensor) ON DELETE RESTRICT<br>PK (id_sensor, timestamp, variable) |  |
| `timestamp` | timestamptz | no | PK (id_sensor, timestamp, variable) |  |
| `variable` | varchar(40) | no | PK (id_sensor, timestamp, variable) |  |
| `valor` | double precision | no | — |  |
| `tipo_anomalia` | varchar(80) | no | — |  |

### `spark_kpi_zona`

KPI diario por zona y métrica, formato largo, calculado por Spark (B3.4).  
**Fuente:** Spark SP-Q10 vía `carga_pg.py`

| Atributo | Tipo | Nulo | Restricciones | Descripción |
|---|---|---|---|---|
| `zona` | varchar(100) | no | FK → ubicaciones(nombre_zona) ON DELETE RESTRICT<br>PK (zona, fecha, metrica) |  |
| `fecha` | date | no | PK (zona, fecha, metrica) |  |
| `metrica` | varchar(50) | no | PK (zona, fecha, metrica) |  |
| `valor` | double precision | no | — |  |

### `tipos_sensor`

Tipo de sensor; determina su categoría (tabla propia para cumplir 3FN).  
**Fuente:** Semilla en `001_tablas.sql`

| Atributo | Tipo | Nulo | Restricciones | Descripción |
|---|---|---|---|---|
| `tipo_sensor` | varchar(50) | no | PK |  |
| `descripcion` | varchar(150) | no | — |  |
| `id_categoria` | integer | no | FK → categorias_sensor(id_categoria) ON DELETE RESTRICT |  |

### `ubicaciones`

Zonas de la ciudad (Londres): una ubicación por zona del contrato, con distrito y centroide.  
**Fuente:** `config/zonas.yaml` (5 distritos de Londres) — `pg_cargar_maestros.py`

| Atributo | Tipo | Nulo | Restricciones | Descripción |
|---|---|---|---|---|
| `id_ubicacion` | integer | no | PK<br>DEFAULT nextval('ubicaciones_id_ubicacion_seq'::regclass) |  |
| `nombre_zona` | varchar(100) | no | UNIQUE | Clave candidata; valores exactos de contrato.ZONAS |
| `distrito` | varchar(100) | no | — |  |
| `latitud` | numeric(9,6) | no | CHECK (((latitud >= ('-90'::integer)::numeric) AND (latitud <= (90)::numeric))) |  |
| `longitud` | numeric(9,6) | no | CHECK (((longitud >= ('-180'::integer)::numeric) AND (longitud <= (180)::numeric))) |  |

### `usuarios`

Usuarios del sistema (ficticios), asociados a un organismo.  
**Fuente:** 40 usuarios ficticios (`@smartcity.example`, semilla 2026) — `pg_cargar_maestros.py`

| Atributo | Tipo | Nulo | Restricciones | Descripción |
|---|---|---|---|---|
| `id_usuario` | integer | no | PK<br>DEFAULT nextval('usuarios_id_usuario_seq'::regclass) |  |
| `nombre` | varchar(150) | no | — |  |
| `correo` | varchar(150) | no | CHECK (((correo)::text ~ '^[^@[:space:]]+@[^@[:space:]]+\.[^@[:space:]]+$'::text))<br>UNIQUE |  |
| `id_organismo` | integer | sí | FK → organismos(id_organismo) ON DELETE RESTRICT |  |

### `variables_medicion`

Variables medidas y su unidad.  
**Fuente:** Semilla en `002_extensiones.sql` (umbrales de `config/umbrales.yaml`)

| Atributo | Tipo | Nulo | Restricciones | Descripción |
|---|---|---|---|---|
| `codigo` | varchar(40) | no | PK |  |
| `unidad` | varchar(30) | no | — |  |

## 2. MongoDB (`smartcity_db`)

Validadores `$jsonSchema` con `validationLevel: moderate` (definidos en `scripts/mongo_admin.py` y `mongo/01_colecciones.js`): exigen solo los campos comunes; el resto del documento es libre, que es justamente lo que permite guardar lecturas de tipos y firmwares distintos sin migraciones.

### `lectura`

| Atributo | Tipo | Obligatorio | Restricciones / notas |
|---|---|---|---|
| `sensor_id` | string | sí | — |
| `tipo` | enum: calidad_aire, clima, trafico, energia, ruido, ocupacion_parqueo, transporte_publico | sí | — |
| `timestamp` | date | sí | — |
| `ubicacion` | object | sí | subdocumento {id_zona: int, zona: string}; obligatorios: id_zona, zona |
| `mediciones` | object | no | sin propiedades fijas: cada tipo/firmware trae las suyas |
| `origen` | string | no | openaq · open-meteo · sintetico |
| `ubicacion.geo` | Point GeoJSON | no | [lon, lat]; índice 2dsphere |
| `ubicacion.tipo_zona` | string | no | mixta · residencial · comercial · industrial |
| `mediciones.<variable>` | double | no | pm25, pm10, no2, o3, co, co2 · temperatura, humedad, precipitacion, viento — solo las que mide cada tipo/firmware |
| `dispositivo.firmware` | string | no | 1.x / 2.x (2.x desde 2024-03-01 agrega o3, co2, bateria_pct) |
| `dispositivo.bateria_pct` | double | no | solo firmware 2.x |
| `calidad_dato.valido` | bool | no | false en anomalías inyectadas |
| `calidad_dato.flags` | array<string> | no | pico, negativo, fuera_rango, pegado |

### `evento`

| Atributo | Tipo | Obligatorio | Restricciones / notas |
|---|---|---|---|
| `tipo_evento` | enum: congestion, pico_ruido, contaminacion_alta, evento_urbano, falla_sensor, lluvia_intensa, ola_calor | sí | — |
| `severidad` | enum: baja, media, alta, critica | sí | — |
| `inicio` | date | sí | — |
| `zona` | object | sí | subdocumento {id_zona: int, nombre: string}; obligatorios: id_zona, nombre |
| `detalles` | object | no | — |
| `detalles` | object | no | distinto por tipo de evento (p. ej. mm_h en lluvia_intensa) |
| `fin` | date | no | cierre del evento |
| `id_sensor` | string | no | cuando lo origina un sensor |

### `alerta`

| Atributo | Tipo | Obligatorio | Restricciones / notas |
|---|---|---|---|
| `valor` | double \| int | sí | — |
| `umbral` | double \| int | sí | — |
| `estado` | enum: abierta, atendida, cerrada | sí | — |
| `emitida` | date | sí | — |
| `zona` | object | sí | subdocumento {id_zona: int, nombre: string}; obligatorios: id_zona, nombre |
| `acciones` | array | no | — |
| `regla` | — | sí | — |
| `variable` | — | sí | — |
| `regla` | string | no | pm25_umbral, temperatura_umbral, stream_pm25, stream_congestion |
| `nivel` | string | no | media · alta · critica (config/umbrales.yaml) |
| `origen` | string | no | batch · streaming |
| `acciones` | array<object> | no | historial de atención |

### `incidente`

| Atributo | Tipo | Obligatorio | Restricciones / notas |
|---|---|---|---|
| `categoria` | enum: accidente_transito, bache, semaforo_danado, inundacion, alumbrado, residuos, vandalismo, quema_humo | sí | — |
| `titulo` | string | sí | — |
| `descripcion` | string | sí | — |
| `reportado` | date | sí | — |
| `zona` | object | sí | subdocumento {id_zona: int, nombre: string}; obligatorios: id_zona, nombre |
| `historial` | array | no | — |
| `estado_actual` | — | sí | — |
| `reportes_ciudadanos` | array<object> | no | largo variable |
| `historial` | array<object> | no | cambios de estado |
| `heridos` | int | no | solo accidentes |
| `adjuntos` | array | no | ~20 % de los incidentes |

Colecciones auxiliares sin validador: `sensor` (catálogo para `$lookup`), `stream_ventana` (streaming), `demo_crud` (demostración CRUD) y `bench_*` (experimentos C1).

## 3. HDFS y Spark

### Crudos en `/smartcity/raw` (inmutables)

| Archivo | Atributos | Fuente |
|---|---|---|
| `openaq/<estación>/<año>.csv` | `timestamp_utc` (ISO 8601 UTC), `parameter` (pm25 · pm10 · no2 · o3 · co), `value` (double, ≥ 0 tras limpieza), `unit` | OpenAQ API v3 |
| `openmeteo/zona_<zona>_<año>.json` | `hourly.time`, `temperature_2m` (°C), `relative_humidity_2m` (%), `precipitation` (mm), `wind_speed_10m` (km/h) | Open-Meteo |
| `trafico/trafico_<año>.csv` | `timestamp_utc`, `zona`, `id_sensor` (T-00x), `vehiculos_hora` (int ≥ 0), `velocidad_promedio_kmh`, `congestion_pct` (0–100), `origen` | Sintético |
| `energia/energia_<año>.csv` | `timestamp_utc`, `zona`, `id_sensor` (E-00x), `consumo_kwh` (≥ 0), `origen` | Sintético |
| `mongo_exports/<colección>/fecha=…/*.jsonl` | Documento de MongoDB en JSON extendido (`$date`, `$oid`) | MongoDB |

### Parquet curado (formato largo del contrato)

| Atributo | Tipo Spark | Restricciones |
|---|---|---|
| `zona` | string | ∈ `contrato.ZONAS` |
| `timestamp` | timestamp (UTC) | no nulo |
| `id_sensor` | string | existe en `sensores` de PostgreSQL |
| `tipo_metrica` | string | variable (pm25, temperatura, …) o trafico / energia |
| `valor` | double | no nulo, no NaN, en rango físico de la variable |
| `fuente` / `origen` | string | openaq · open-meteo · sintetico |
| `anio`, `mes` | int | columnas de partición (`anio=AAAA/mes=MM`) |

### Datos de benchmark (C1)

`lectura_<n>` = `id_sensor` (string), `zona` (string), `tipo_metrica` (string), `timestamp` (UTC), `valor` (double, 2 decimales, ≥ 0). Mismo contenido en PostgreSQL (`bench.lectura_<n>`), MongoDB (`bench_lectura_<n>`) y HDFS (`/smartcity/sandbox/bench`).
