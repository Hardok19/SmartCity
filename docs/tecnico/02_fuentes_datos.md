# 2. Fuentes de datos

El detalle de cada fuente (URL, licencia, comando de descarga, columnas y limpieza) está en
[datos/FUENTES.md](../datos/FUENTES.md); las cifras medidas, en [datos/perfil_fuentes.md](../datos/perfil_fuentes.md);
el mecanismo de generación sintética, en [datos/GENERACION_SINTETICA.md](../datos/GENERACION_SINTETICA.md); y los
volúmenes por tecnología, en [datos/VOLUMENES.md](../datos/VOLUMENES.md).

## 2.1 Fuentes reales (el enunciado pide al menos dos)

| | OpenAQ | Open-Meteo |
|---|---|---|
| Qué es | Agregador mundial de mediciones de calidad del aire | API de clima; el histórico es el reanálisis ERA5 de Copernicus |
| Variables | PM2.5, PM10, NO₂, O₃, CO (horario) | Temperatura, humedad relativa, precipitación, viento a 10 m (horario) |
| Cobertura | 5 estaciones de Londres, una por zona | Centroide de cada una de las 5 zonas |
| Acceso | API v3 con clave gratuita (`descargar_openaq.py`) | API sin clave (`descargar_openmeteo.py`) |
| Licencia | La del proveedor original de cada estación (Defra / UK-AIR y redes locales), más cita a OpenAQ | CC BY 4.0, cita a Open-Meteo.com |
| Formato crudo | CSV por estación y año | JSON por zona y año |
| Limitación | PM2.5 con cobertura útil solo en Centro y Este en 2023–2024 | Es un modelo, no una estación: suaviza extremos locales |

## 2.2 Por qué estas dos fuentes

1. Cubren la cadena del ejemplo «Ambiente» del enunciado: *sensores ambientales + datos meteorológicos + ubicación
   geográfica → MongoDB + PostgreSQL → Hadoop → Spark → identificación de zonas críticas*.
2. Se complementan: el clima explica parte de la contaminación (el viento la dispersa y la lluvia la lava), lo que da
   material real para el análisis correlacional (SP-Q04) y el predictivo (MLlib).
3. Son abiertas, con licencia clara y descarga reproducible por script (idempotente y registrada en
   `data/raw/MANIFEST.json`).
4. Tienen granularidad horaria y años de historia: alcanzan para series temporales, comparación interanual y
   detección de anomalías.

## 2.3 Ampliación sintética

El enunciado permite ampliar los datos con generación sintética si se documenta el mecanismo. Se usó para cuatro
cosas, siempre con `origen = sintetico` para no mezclar datos sin aviso:

| Conjunto | Para qué | Volumen |
|---|---|---|
| 30 sensores IoT virtuales (4 de aire y 2 de clima por zona) | Alcanzar los mínimos de MongoDB y Spark con datos que conservan el comportamiento real: cada lectura = serie real de la zona × sesgo del sensor + ruido; ~1 % de anomalías, huecos de 1–6 h, cambio de firmware el 2024-03-01 que agrega campos | ~1,07 M lecturas |
| Alertas, eventos e incidentes | Derivados de las lecturas (una alerta por cada tramo sobre el umbral), para que sean coherentes con los datos; incidentes con texto libre en español | 1.190 / 1.428 / 3.000 |
| Tráfico y energía (D-03) | KPIs de Movilidad y Energía y las preguntas «zona con más vehículos» y «hora de mayor congestión» | 87.720 + 87.720 filas |
| Datos de benchmark (C1) | El mismo dato lógico cargado en los tres motores a 100k, 1 M y 5 M filas | 6,1 M por motor |

Semilla fija **2026** en todos los generadores: la misma ejecución produce los mismos datos en cualquier computadora.

## 2.4 Calidad de los datos y limpieza

| Problema | Dónde aparece | Tratamiento |
|---|---|---|
| Nulos y NaN | Crudos de OpenAQ y Open-Meteo; huecos de sensores | Se descartan en el ETL; los huecos ≥ 4 h generan un evento `falla_sensor` |
| Valores negativos o fuera de rango físico | Anomalías inyectadas (~0,2 %) y errores de estación | Rango por variable (temperatura −60…60 °C, humedad 0…100 %, resto ≥ 0) + `calidad_dato.valido` |
| Picos y sensor pegado | Anomalías inyectadas | Se conservan en Mongo marcados y se detectan en Spark con z-score e IQR (análisis 7) |
| Duplicados | Reintentos de descarga, réplica de Mongo en HDFS | `dropDuplicates(id_sensor, timestamp, tipo_metrica)`; en el ETL integral `row_number()` prioriza el crudo externo sobre la réplica |
| Zonas horarias | OpenAQ trae hora local en algunos campos | Todo se normaliza a UTC; el perfil horario (hora pico) se calcula en hora de Londres |
| Esquema variable | Firmware 2.x agrega `o3`, `co2`, `bateria_pct` | MongoDB lo guarda tal cual; Spark lee con esquema explícito y los campos ausentes quedan nulos |
