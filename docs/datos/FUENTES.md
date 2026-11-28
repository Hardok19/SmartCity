# Fuentes de datos (F2.3)

El enunciado pide al menos dos fuentes distintas y documentar de cada una la fuente, la licencia y el proceso de
descarga y limpieza. SmartCity usa **dos fuentes reales** (OpenAQ y Open-Meteo) para el dominio Ambiente y
**cuatro conjuntos sintéticos** que amplían el volumen (D-06) o cubren KPIs fuera del dominio (D-03).

Las cifras medidas (filas, nulos, mínimos, máximos, rango de fechas, frecuencia, tamaño) están en
[perfil_fuentes.md](perfil_fuentes.md), que genera `python scripts/perfilar_fuentes.py` sobre lo que haya en `data/raw/`.

Ciudad de referencia: **Londres** (D-02, `config/zonas.yaml`). Cada una de las 5 zonas del contrato es un distrito real
con una estación OpenAQ (`config/estaciones_openaq.yaml`). Período: 2023-01-01 a 2024-12-31, en UTC.

## Resumen

| Fuente | Tipo | Qué aporta | Formato crudo | Destino |
|---|---|---|---|---|
| OpenAQ | Real | Contaminantes horarios de 5 estaciones | CSV por estación y año | HDFS `raw/openaq` → Mongo `lectura` (S-001…S-005) → Spark |
| Open-Meteo | Real | Clima horario en el centroide de cada zona | JSON por zona y año | HDFS `raw/openmeteo` → Mongo `lectura` (S-006…S-010) → Spark |
| Sensores IoT virtuales | Sintético anclado a lo real | 30 sensores, lecturas cada 30 min, anomalías, huecos, cambio de firmware | JSONL | Mongo `lectura` → export HDFS → Spark |
| Alertas, eventos, incidentes | Sintético derivado | Lo que dispararían las lecturas; incidentes con texto libre | JSONL | Mongo `alerta`, `evento`, `incidente` |
| Tráfico | Sintético | 5 contadores, 1 lectura/h | CSV por año | HDFS `raw/trafico` → Spark → PostgreSQL `medicion_horaria` |
| Energía | Sintético | 5 medidores agregados por zona, kWh/h | CSV por año | HDFS `raw/energia` → Spark → PostgreSQL `medicion_horaria` |
| Catálogo de sensores | Maestro | 50 sensores con zona, organismo, coordenadas, estado | CSV (`data/sensores.csv`) | PostgreSQL (maestros) y Mongo `sensor` |

## 1. OpenAQ — calidad del aire (real)

- **Qué es:** plataforma abierta que agrega mediciones de estaciones oficiales y de sensores de bajo costo de todo el
  mundo. Para Londres los datos vienen de las redes oficiales del Reino Unido (Defra / AURN y redes de los distritos).
- **URL:** <https://openaq.org> · API v3: <https://docs.openaq.org> · archivo público alternativo en S3
  (`openaq-data-archive`, sin clave).
- **Licencia:** OpenAQ no aplica una sola licencia: cada registro conserva la del proveedor original, que la API informa
  por estación ([política de datos](https://docs.openaq.org/docs/data-policy),
  [términos de uso](https://docs.openaq.org/about/terms)). Se exige citar al proveedor original y a OpenAQ. Uso
  académico, no comercial.
- **Descarga** (requiere `OPENAQ_API_KEY` gratuita en `.env`; no se sube al repositorio):
  ```bash
  python scripts/descargar_openaq.py --estacion 154 --anios 2024     # prueba con una estación
  python scripts/descargar_openaq.py                                  # las 5 estaciones, 2023 y 2024
  ```
  Recorre `/v3/locations/{id}` → sensores de la estación (pm25, pm10, no2, o3, co) → `/v3/sensors/{id}/hours` por año,
  respeta el límite de ~60 peticiones/min (espera y reintenta ante 429) y es idempotente (no repite archivos existentes).
  `descargar_openaq_corregido.py` es la versión ajustada tras probar contra la API real.

  **Sin clave** (la que se usó para los datos de este repositorio, 2026-11-27):
  ```bash
  python scripts/descargar_openaq_archivo.py                         # 5 estaciones × 2023, 2024 (~1 min)
  ```
  Lee el archivo público de OpenAQ en S3 (`openaq-data-archive/records/csv.gz/locationid=<id>/year=<a>/month=<mm>/`,
  un CSV.gz por estación y día local). El archivo marca cada promedio horario con su hora de **fin** en hora local;
  el script la pasa a UTC y resta 1 h, de modo que `timestamp_utc` es el **inicio** de la hora (la misma convención
  que `period.datetimeFrom` de la API). Si una estación tiene dos sensores del mismo parámetro, se promedian.
  Mismo formato de salida y mismo `MANIFEST.json` que la versión con API.
- **Salida:** `data/raw/openaq/<id_estacion>/<anio>.csv` con `timestamp_utc, parameter, value, unit`; cada archivo se
  anota en `data/raw/MANIFEST.json` (URL, fecha de descarga, bytes).
- **Frecuencia:** horaria. **Variables:** PM2.5, PM10, NO₂, O₃ y CO, según lo que mida cada estación.
- **Cobertura observada:** PM2.5 de OpenAQ solo existe en Centro (154) y Este (225796) en 2023–2024; Norte tiene
  PM10 y NO₂, Sur solo PM10 (4.203 horas) y Oeste PM10, NO₂ y O₃ (detalle en [perfil_fuentes.md](perfil_fuentes.md)).
  Las otras tres zonas usan la serie modelada para PM2.5. Es una limitación de la fuente, no del procesamiento. Los
  crudos traen valores negativos y picos aislados (p. ej. CO −1.000 o PM2.5 628,9 µg/m³) que el ETL descarta por rango.
- **Limpieza** (en `etl_ambiente*.py` y en el generador): se descartan nulos, NaN, valores negativos y fuera de rango;
  duplicados por `(id_sensor, timestamp, tipo_metrica)`; los tiempos se normalizan a UTC.

## 2. Open-Meteo — clima histórico (real, reanálisis)

- **Qué es:** API de clima. El endpoint histórico sirve el reanálisis ERA5 / ERA5-Land de Copernicus (ECMWF),
  interpolado al punto pedido. No es una estación física: es la mejor estimación horaria en el centroide de cada zona.
- **URL:** <https://open-meteo.com/en/docs/historical-weather-api> (endpoint `archive-api.open-meteo.com/v1/archive`).
- **Licencia:** datos bajo **CC BY 4.0** ([licencia](https://open-meteo.com/en/license)); hay que citar
  «Weather data by Open-Meteo.com». El uso gratuito de la API es para fines no comerciales
  ([términos](https://open-meteo.com/en/terms)).
- **Descarga** (sin clave):
  ```bash
  python scripts/descargar_openmeteo.py            # 5 zonas × 2023, 2024
  ```
  Una petición por zona y año con `hourly=temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m` y
  `timezone=UTC`; guarda los nulos por variable en `MANIFEST.json`.
- **Salida:** `data/raw/openmeteo/zona_<zona>_<anio>.json` (8.760 u 8.784 horas por archivo).
- **Limpieza:** rangos físicos (temperatura −60…60 °C, humedad 0…100 %, precipitación y viento ≥ 0) y deduplicación
  por zona-hora.

## 3. Datos sintéticos

El mecanismo completo (fórmulas, parámetros, semillas) está en [GENERACION_SINTETICA.md](GENERACION_SINTETICA.md),
como exige el enunciado. Resumen:

| Conjunto | Script | Semilla | Volumen | Por qué existe |
|---|---|---|---|---|
| Sensores IoT virtuales + alertas, eventos, incidentes | `generar_sinteticos.py` | 2026 | 1.068.584 lecturas, 1.190 alertas, 1.428 eventos, 3.000 incidentes (con los datos reales del 2026-11-27; la carga anterior del equipo, con la API, dio 1.068.777 / 1.182 / 1.420) | Las 5 estaciones reales no alcanzan los 500.000 documentos de Mongo ni el millón de Spark; los virtuales se anclan a las series reales de su zona |
| Tráfico | `generar_trafico_sintetico.py` | 2026 + año | 87.720 filas | KPIs de Movilidad y preguntas de PostgreSQL (zona con más vehículos, hora de mayor congestión) (D-03) |
| Energía | `generar_energia_sintetica.py` | 2026 + 100 + año | 87.720 filas | KPIs de Energía (D-03) |
| Streaming | `streaming_productor.py` | — (tiempo real) | Un lote cada 5 s | Reto adicional |
| Benchmarks (C1) | `benchmark_comparacion.py` | 2026 | 100k / 1 M / 5 M filas | Mismo dato lógico en PostgreSQL, MongoDB y Spark para comparar motores |

Todos los registros sintéticos llevan `origen = "sintetico"` (o `streaming_simulado`), de modo que nunca se mezclan sin
aviso con los reales.

## 4. En qué tecnología termina cada dato y por qué

| Dato | Tecnología | Por qué |
|---|---|---|
| Archivos crudos tal como se descargaron (CSV, JSON) | **HDFS** `/smartcity/raw` | Inmutables y masivos: se guardan una vez, replicados ×2, para poder reprocesarlos si cambia la limpieza |
| Lecturas IoT con campos distintos por tipo y firmware, alertas, eventos, incidentes con texto libre | **MongoDB** | Esquema flexible: un documento por lectura con solo los campos que trae; arreglos e historial embebidos; índices geoespaciales y de texto |
| Export de Mongo para análisis histórico | **HDFS** `/smartcity/raw/mongo_exports` | Separa la carga analítica de la base operativa; MapReduce y Spark leen bloques en paralelo |
| Limpieza, formato largo, agregados, análisis, ML | **Spark** → Parquet en HDFS | Procesamiento distribuido sobre millones de filas; Parquet columnar y particionado por año/mes |
| Catálogo (zonas, sensores, organismos, usuarios, calendario) y mediciones horarias limpias | **PostgreSQL** | Integridad referencial (FK, CHECK), 3FN, consultas puntuales e índices; los resultados de Spark vuelven aquí (B3.4) |

## 5. Atribución

- Datos de calidad del aire: OpenAQ (<https://openaq.org>) y sus proveedores originales (Defra / UK-AIR y redes
  locales de Londres), según la licencia que la API indica para cada estación.
- Datos de clima: Open-Meteo.com (<https://open-meteo.com>), CC BY 4.0, a partir del reanálisis ERA5 de Copernicus /
  ECMWF.
