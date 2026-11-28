# 1. Descripción del problema

## 1.1 Contexto

La municipalidad de una ciudad inteligente despliega sensores en distintas zonas urbanas. Estos sensores producen
datos de naturaleza muy distinta:

- **Estructurados y estables:** qué sensores existen, de qué tipo, dónde están, qué organismo responde por ellos y
  qué usuarios operan el sistema.
- **Semiestructurados y cambiantes:** cada lectura trae campos distintos según el tipo de sensor y la versión de
  firmware; los eventos, alertas e incidentes tienen detalles propios y texto libre.
- **Masivos e históricos:** años de lecturas cada 15–60 minutos, que se guardan para reprocesarlos.
- **Continuos:** lecturas que llegan en tiempo real y deben generar alertas en segundos.

La pregunta central del proyecto es **dónde almacenar cada tipo de información y cómo procesarla para obtener
conocimiento útil**, y demostrar con experimentos que ninguna tecnología sirve igual de bien para todo.

## 1.2 Alcance elegido: Ambiente, con Movilidad y Energía simuladas

| Decisión | Detalle | Justificación |
|---|---|---|
| Dominio principal: **Ambiente** | Calidad del aire (PM2.5, PM10, NO₂, O₃, CO) y clima (temperatura, humedad, precipitación, viento) | Hay dos fuentes abiertas reales, con historia de años y licencia clara: OpenAQ y Open-Meteo |
| Ciudad de referencia: **Londres** (D-02) | 5 distritos reales (Islington, Merton, Westminster, Greenwich, Richmond) mapeados a las 5 zonas del contrato | Tiene estaciones oficiales con PM2.5 en distintos tipos de zona: comercial, residencial, industrial |
| Período: **2023–2024**, en UTC | 17.544 horas | Dos años completos permiten la comparación interanual (SP-Q09) y entrenar con un año y probar con el siguiente (MLlib) |
| Movilidad y Energía **simuladas** (D-03) | 5 contadores de tráfico y 5 medidores de energía, una lectura por hora | El dashboard exige KPIs de los 4 grupos; se marcan `origen = sintetico` y se documentan como supuesto, no como hallazgo |
| Volumen ampliado con **sensores virtuales** (D-06) | 30 sensores IoT anclados a la serie real de su zona, con ruido, anomalías, huecos y cambio de firmware | Las 5 estaciones reales no alcanzan los mínimos (Mongo ≥ 500k, Spark ≥ 1 M); los virtuales conservan el comportamiento de los datos reales |

## 1.3 Preguntas que el sistema responde

| Grupo | Preguntas | Dónde se responden |
|---|---|---|
| Inventario | Sensores por tipo, ubicación, organismo y año; activos frente a inactivos; sensores sin actividad | PostgreSQL PG-Q01…Q10 |
| Ambiente | Temperatura promedio por zona; zona más contaminada; hora de mayor contaminación; rachas sobre el umbral de la OMS | PostgreSQL PG-P01…P04, P07 · MongoDB MG-P01…P05 |
| Lecturas | Lecturas de un sensor o un período; máximos y mínimos; valores sobre el límite; evolución de una variable | MongoDB MG-Q01…Q10 |
| Volumen histórico | Mediciones por sensor; sensor con más datos; temperatura promedio histórica; eventos por zona; comportamiento en un período | Hadoop MapReduce mr01…mr05 |
| Analítica | Sensores más activos; evolución temporal; correlaciones; predicción; zonas críticas; horas pico; anomalías; comparación entre zonas y años | Spark SP-Q01…Q10 y 8 análisis |
| Movilidad y energía | Zona con más vehículos; hora de mayor congestión; consumo por zona y hora; picos; variación respecto al período anterior | PostgreSQL PG-P05, P06 · Spark |
| Tiempo real | ¿Qué zona supera ahora el umbral de PM2.5 o de vehículos? | Spark Structured Streaming |

## 1.4 Requisitos de volumen

| Requisito del enunciado | Mínimo | Alcanzado | Detalle |
|---|---|---|---|
| Registros en PostgreSQL | 100.000 | 175.440 (tráfico y energía); más las mediciones ambientales al cargar el ETL | [VOLUMENES.md](../datos/VOLUMENES.md) |
| Documentos en MongoDB | 500.000 | ~1,07 M lecturas + alertas, eventos e incidentes | |
| Registros procesados por Spark | 1.000.000 | 4,38 M filas en formato largo | |
| Total recomendado | > 5 M | > 5,6 M (sin contar los 6,1 M por motor de los benchmarks) | |

## 1.5 Restricciones

- **Hardware de estudiante:** todo corre en una sola máquina con Docker (~8 GB para los contenedores). Por eso se
  limitó la memoria de cada servicio y el clúster tiene 3 DataNodes y 1 worker de Spark: alcanza para demostrar
  bloques, réplicas, tolerancia a fallos y paralelismo, no para medir rendimiento de producción.
- **Datos reales incompletos:** OpenAQ solo tiene PM2.5 útil para 2 de las 5 zonas en el período; las demás usan
  la serie modelada, y siempre se identifica con el campo `origen`.
- **Lo que no se mide:** ninguna fuente tiene tiempos de recorrido, así que el KPI «tiempo promedio de recorrido» se
  muestra como **sin dato** en lugar de inventarlo.
