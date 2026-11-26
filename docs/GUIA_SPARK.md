# SmartCity — Guía de instalación, datos y Apache Spark

**Proyecto CE3101 · Pareja B (MongoDB y Apache Spark)**

**Rama de referencia:** `trabajo-b`

**Período analizado:** 2023–2024

**Documento:** `docs/GUIA_SPARK.md`

> **Objetivo.** Permitir que un integrante del equipo prepare las fuentes de datos, cargue MongoDB, ejecute el ETL MongoDB → Spark → HDFS, reproduzca los **8 análisis obligatorios de B4.2** y ejecute las **10 consultas obligatorias SP-Q01 a SP-Q10 de B4.1**.
>
> Esta guía describe el código publicado en la rama `trabajo-b`. La ejecución completa en una instalación *desde cero* todavía debe validarse en un segundo equipo. Los datos y volúmenes usados en las pruebas originales **no se incluyen en GitHub**.

## 1. Qué contiene el repositorio

| Ruta | Función |
|---|---|
| `docker/docker-compose.yml` | PostgreSQL, MongoDB, HDFS y Spark; monta el repositorio en `/opt/smartcity:ro` en los dos servicios Spark |
| `docker/docker-compose.spark-ml.yml` | Configuración complementaria para construir la imagen Spark con bibliotecas de MLlib |
| `docker/spark/Dockerfile` | Imagen `smartcity-spark-ml:3.5`, con NumPy 1.26.4 y SciPy 1.13.1 |
| `config/zonas.yaml`, `config/estaciones_openaq.yaml`, `config/umbrales.yaml` | Parámetros de generación y adquisición de datos |
| `data/maestros/sensores.csv` | Catálogo versionado de 40 sensores |
| `scripts/descargar_openmeteo.py` | Descarga datos meteorológicos históricos |
| `scripts/descargar_openaq_corregido.py` | Descarga mediciones OpenAQ API v3 por estación y año |
| `scripts/generar_sinteticos.py` | Genera las colecciones de lecturas, alertas, eventos e incidentes, integrando fuentes externas disponibles |
| `scripts/mongo_admin.py`, `scripts/mongo_cargar.py` | Crea colecciones/índices y carga documentos en MongoDB |
| `spark/jobs/etl_ambiente.py` | Transforma lecturas MongoDB en Parquet por año y mes en HDFS |
| `spark/jobs/analitica.py` | **8 análisis B4.2**, implementados con PySpark DataFrames/MLlib |
| `spark/jobs/ejecutar_consultas.py` | Ejecutor de las **10 consultas B4.1**, escritas en Spark SQL |
| `spark/sql/SP-Q01.sql` … `SP-Q10.sql` | Consultas obligatorias individuales |
| `docs/resultados/hallazgos_spark.md` | Hallazgos y cifras obtenidas en la ejecución de referencia |

### Datos que NO se descargan al clonar

Git no almacena los datos descargados en `data/raw/`, los JSONL generados en `data/interim/`, el contenido de MongoDB, los volúmenes de Docker ni los archivos Parquet de HDFS. Se regeneran siguiendo las secciones 4–6, o se restauran desde un respaldo proporcionado por el equipo.

## 2. Requisitos previos

- Docker Desktop con Docker Compose y memoria suficiente para HDFS y Spark (el proyecto usa un worker de 2 núcleos y 2 GiB).
- Python instalado en el equipo anfitrión y Git Bash si se trabaja en Windows.
- Acceso a Internet para obtener imágenes Docker, el conector MongoDB y las fuentes externas, si se van a descargar nuevamente.
- Una clave personal de **OpenAQ API v3** para descargar los datos de OpenAQ. **Open-Meteo no requiere clave** para el script del proyecto.
- Puertos **5432**, **27017**, **9870**, **8020**, **8080** y **7077** disponibles. En Windows, detener o remapear los servicios PostgreSQL/MongoDB locales que estén ocupando 5432/27017.

**Todos los comandos siguientes se ejecutan desde la raíz del repositorio** (`SmartCity/`), usando **Git Bash**, salvo indicación contraria. Los comandos con rutas internas `/opt/...` o `/smartcity/...` incluyen `MSYS_NO_PATHCONV=1` para evitar la conversión de rutas de Git Bash.

```bash
# Clonar si todavía no se tiene el repositorio:
git clone https://github.com/Hardok19/SmartCity.git
cd SmartCity

git fetch origin
git switch --track origin/trabajo-b

# Si ya está clonado, comprobar la rama:
git branch --show-current
```

> Al instalarse la rama en `main` más adelante, habrá que adaptar el nombre de la rama en estas instrucciones.

### Preparar Python local

```bash
# Opcional pero recomendado: entorno virtual de Python en la raíz
python -m venv .venv
source .venv/Scripts/activate  # Windows + Git Bash
# En Linux/macOS usar: source .venv/bin/activate

python -m pip install --upgrade pip
python -m pip install numpy PyYAML requests pymongo python-dotenv
```

El entorno virtual local **no** instala dependencias dentro de los contenedores Spark; eso lo resuelve el Dockerfile.

## 3. Levantar Docker, Hadoop y Spark ML

Utilizar **siempre ambos archivos Compose** cuando se inicia o recrea el entorno. El segundo añade las dependencias usadas por Spark MLlib; ejecutar Compose solo con el archivo base podría volver a usar la imagen sin ellas.

```bash
# Validar la configuración combinada
docker compose -f docker/docker-compose.yml -f docker/docker-compose.spark-ml.yml config -q

# Construir Spark ML e iniciar TODOS los servicios por primera vez
docker compose -f docker/docker-compose.yml -f docker/docker-compose.spark-ml.yml up -d --build

# Comprobar servicios
docker compose -f docker/docker-compose.yml -f docker/docker-compose.spark-ml.yml ps
```

Se esperan los contenedores `smartcity_postgres`, `smartcity_mongo`, `smartcity_namenode`, `smartcity_datanode`, `smartcity_spark_master` y `smartcity_spark_worker`.

Verificar que el repositorio y las dependencias sean visibles dentro de Spark:

```bash
MSYS_NO_PATHCONV=1 docker exec smartcity_spark_master ls /opt/smartcity/spark/jobs

for c in smartcity_spark_master smartcity_spark_worker; do
  MSYS_NO_PATHCONV=1 docker exec "$c" /opt/bitnami/python/bin/python -c \
    "import numpy, scipy; from pyspark.ml.feature import VectorAssembler; print('SPARK_ML_OK', numpy.__version__, scipy.__version__)"
done
```

Comprobar el almacenamiento HDFS y crear las carpetas base:

```bash
MSYS_NO_PATHCONV=1 docker exec smartcity_namenode \
  hadoop fs -mkdir -p /smartcity/raw /smartcity/sandbox

MSYS_NO_PATHCONV=1 docker exec smartcity_namenode \
  hadoop fs -ls /smartcity
```

Paneles locales: Hadoop NameNode <http://localhost:9870> · Spark Master <http://localhost:8080>.

**No usar `docker compose down -v`**: `-v` eliminaría los volúmenes que contienen las bases de datos y los archivos de Hadoop. Para detener sin eliminar datos, usar:

```bash
docker compose -f docker/docker-compose.yml -f docker/docker-compose.spark-ml.yml stop
```

## 4. Descargar los datos históricos de 2023 y 2024

Las fuentes son **OpenAQ** (calidad del aire) y **Open-Meteo** (meteorología). El generador de SmartCity las incorpora cuando están disponibles y simula las mediciones faltantes. No equivale a que todos los datos del resultado sean observaciones reales.

### 4.1. Open-Meteo (sin API key)

```bash
python scripts/descargar_openmeteo.py --anios 2023 2024
```

Se guardan archivos como `data/raw/openmeteo/zona_centro_2023.json` y `zona_norte_2024.json`; se esperan 5 zonas × 2 años = **10 archivos**, si todas las solicitudes terminan correctamente. Son datos meteorológicos horarios y deben atribuirse a Open-Meteo.

### 4.2. OpenAQ (requiere API key)

Obtener una clave desde el servicio OpenAQ. Introducirla en Git Bash sin escribirla en el código ni subirla a Git:

```bash
read -rsp "OpenAQ API key: " OPENAQ_API_KEY
echo
export OPENAQ_API_KEY

# Prueba con una estación configurada
python scripts/descargar_openaq_corregido.py --estacion 154 --anios 2023 2024

# Descargar las estaciones del archivo config/estaciones_openaq.yaml
python scripts/descargar_openaq_corregido.py --anios 2023 2024

unset OPENAQ_API_KEY
```

El script crea `data/raw/openaq/<id_estacion>/<anio>.csv` y actualiza `data/raw/MANIFEST.json`. Si un archivo anual ya existe, el descargador normalmente lo omite; revisar los mensajes. Las estaciones configuradas son 154, 225791, 225773, 225796 y 225771.

**Limitación comprobada en la ejecución de referencia:** el PM2.5 histórico de OpenAQ solo estuvo disponible en **Zona Centro y Zona Este** durante el período estudiado. Las demás zonas pueden tener otros contaminantes de OpenAQ, pero no PM2.5 histórico disponible en la extracción utilizada. Los resultados actuales **no deben presentarse como datos reales completos de las cinco zonas**. Las APIs pueden modificar disponibilidad, límites y respuestas; para reproducir *exactamente* las cifras del informe se necesitan snapshots de las fuentes originales.

### 4.3. Comprobar que los datos se descargaron

```bash
find data/raw/openmeteo -type f -name '*.json' | sort
find data/raw/openaq -type f -name '*.csv' | sort
```

Si no se descarga OpenAQ o Open-Meteo, **el generador puede producir datos sintéticos**, pero los conteos, los orígenes y los análisis diferirán de los resultados publicados. No afirmar que se utilizaron esas fuentes si no se descargaron.

## 5. Generar los documentos y cargarlos en MongoDB

### 5.1. Catálogo de sensores

El archivo `data/maestros/sensores.csv` ya está versionado. Confirmar que existe:

```bash
ls data/maestros/sensores.csv
```

Solo si no existe o se desea regenerarlo explícitamente:

```bash
python scripts/generar_maestros.py
```

### 5.2. Generar JSONL para las cuatro colecciones

```bash
python scripts/generar_sinteticos.py --salida data/interim/mongo/final
```

Genera `lectura.jsonl`, `alerta.jsonl`, `evento.jsonl`, `incidente.jsonl` y `resumen.json` en `data/interim/mongo/final/`.

> **Atención:** volver a ejecutar el generador sobre la misma carpeta **sobrescribe** los archivos JSONL. Mantener la versión corregida de `generar_sinteticos.py`, que formatea correctamente las fechas de finalización de eventos.

La semilla del generador es fija; sin embargo, cambiar los datos externos, el catálogo o el código puede cambiar las cifras.

### 5.3. Preparar MongoDB y cargar documentos

Estos scripts se ejecutan en **Python local**, donde MongoDB usa `localhost:27017`, **no** el hostname `mongo` de la red Docker. Si se había configurado `MONGO_URI` para Spark en esta terminal, primero hacer `unset MONGO_URI`, o suministrar una URI local al script.

```bash
# Para el entorno de desarrollo definido en docker-compose.yml:
unset MONGO_URI

# Crear colecciones, validadores e índices
python scripts/mongo_admin.py todo

# SOLO para una base nueva o una recarga intencional que puede eliminar datos
python scripts/mongo_cargar.py --dir data/interim/mongo/final --reiniciar --lote 10000
```

`--reiniciar` **borra los documentos actuales de las colecciones afectadas** antes de recargar. **No ejecutar en una base compartida o con información que se desee conservar.** Si las credenciales Docker no son las predeterminadas del Compose, configurar `MONGO_URI` con `localhost:27017` y `authSource=admin` antes de ejecutar los scripts.

La carga imprime los conteos. En la ejecución de referencia se obtuvieron:

| Colección | Documentos de referencia |
|---|---:|
| `lectura` | 1.068.777 |
| `alerta` | 1.182 |
| `evento` | 1.420 |
| `incidente` | 3.000 |

Los totales dependen de las fuentes y del generador. **No son conteos garantizados para una instalación nueva.**

## 6. ETL ambiental: MongoDB → Spark → Parquet HDFS (B3.2)

El ETL lee `smartcity_db.lectura`, aplica esquema explícito, pasa las mediciones a formato largo, descarta valores no válidos, evita duplicados de `sensor_id + timestamp + tipo_metrica` y escribe Parquet particionado por `anio` y `mes`.

**Configurar la conexión desde el contenedor**, donde MongoDB se identifica como `mongo:27017`:

```bash
read -rsp "Contraseña de MongoDB Docker: " MONGO_PASS
echo
export MONGO_URI="mongodb://smartcity:${MONGO_PASS}@mongo:27017/smartcity_db?authSource=admin"
unset MONGO_PASS
```

La contraseña debe ser la de **MongoDB dentro de Docker**, no la del MongoDB instalado en Windows. Si la contraseña contiene caracteres especiales de URL, codificarlos antes de construir la URI. No guardar la URI en el repositorio.

### 6.1. Prueba con 5.000 documentos (opcional)

```bash
MSYS_NO_PATHCONV=1 docker exec --user 0:0 -e HOME=/tmp -e HADOOP_USER_NAME=root -e MONGO_URI \
  smartcity_spark_master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 --total-executor-cores 2 \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.mongodb.spark:mongo-spark-connector_2.12:10.4.1 \
  /opt/smartcity/spark/jobs/etl_ambiente.py --limite 5000
```

Escribe en `/smartcity/sandbox/etl_ambiente_prueba`. No modifica MongoDB.

### 6.2. Procesamiento completo (REQUERIDO para los análisis y las consultas)

```bash
MSYS_NO_PATHCONV=1 docker exec --user 0:0 -e HOME=/tmp -e HADOOP_USER_NAME=root -e MONGO_URI \
  smartcity_spark_master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 --total-executor-cores 2 \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.mongodb.spark:mongo-spark-connector_2.12:10.4.1 \
  /opt/smartcity/spark/jobs/etl_ambiente.py \
  --salida hdfs://namenode:8020/smartcity/sandbox/etl_ambiente_completa
```

Verificar:

```bash
MSYS_NO_PATHCONV=1 docker exec smartcity_namenode \
  hadoop fs -ls /smartcity/sandbox/etl_ambiente_completa
```

En la ejecución de referencia: `SMARTCITY_ETL_DOCUMENTOS_ENTRADA=1068777` y `SMARTCITY_ETL_FILAS_PARQUET=4389705`. Se espera `_SUCCESS` y particiones `anio=2023/`, `anio=2024/`. El ETL **sobrescribe exclusivamente la ruta de salida indicada**: no apuntarlo a una carpeta con datos que se quieran conservar.

> **Alcance:** esta versión del ETL consume la colección MongoDB y no lee directamente los CSV/JSON crudos desde HDFS ni carga todavía los resultados en PostgreSQL. Es una implementación funcional de la primera etapa, no toda la integración B3.2/B3.4 planeada.

---

## 7. OCHO análisis obligatorios con Apache Spark (B4.2)

**Programa:** `spark/jobs/analitica.py`

**Entrada:** `hdfs://namenode:8020/smartcity/sandbox/etl_ambiente_completa`

**Salidas:** `hdfs://namenode:8020/smartcity/sandbox/analitica/`

**Tecnología:** API DataFrame de PySpark y MLlib para el predictivo.

Estos **8 análisis** son trabajos analíticos que guardan resultados en Parquet. **No son las mismas cosas que las 10 consultas SQL de la sección 8.**

| N.º | Identificador `--analisis` | Propósito | Salidas en `/smartcity/sandbox/analitica/` |
|---:|---|---|---|
| 1 | `descriptivo` | Estadísticos, mediciones y sensores por zona/variable/origen | `descriptivo` |
| 2 | `temporal` | PM2.5 por mes y temperatura según hora | `temporal_pm25_mensual`, `temporal_temperatura_hora` |
| 3 | `correlacional` | Matriz Pearson entre PM2.5, PM10, temperatura, humedad, precipitación y viento | `correlacional_matriz` |
| 4 | `predictivo` | Regresión de PM2.5 diario con MLlib: entrenamiento 2023, evaluación 2024, RMSE/MAE/R² y comparación con baseline | `predictivo_metricas`, `predictivo_estimaciones_2024` |
| 5 | `espacial` | Agregados y ranking de PM2.5 por zona y sensor con coordenadas | `espacial_zonas_pm25`, `espacial_sensores_pm25` |
| 6 | `movilidad` | **Proxy ambiental**: perfil horario, comparación entre franjas pico/valle y top 3 | `movilidad_proxy_perfil_horario`, `movilidad_proxy_pico_valle`, `movilidad_proxy_top3_pico_valle` |
| 7 | `anomalias` | Casos marcados en MongoDB + detección estadística Z-score en Parquet limpio | `anomalias_detalle`, `anomalias_resumen`, `anomalias_sensores` |
| 8 | `comparativo` | Estadísticos interanuales 2023–2024 y top 3 de incrementos de PM2.5 | `comparativo_perfil_anual`, `comparativo_pm25_2023_2024`, `comparativo_top3_incremento_interanual` |

### 7.1. Ejecutar los siete análisis que solo requieren HDFS

Para facilitar las pruebas en máquinas pequeñas, este bloque ejecuta **uno por uno**, conservando un resultado y un resumen por cada ejecución:

```bash
for tipo in descriptivo temporal correlacional predictivo espacial movilidad comparativo; do
  echo "===== B4.2: $tipo ====="
  MSYS_NO_PATHCONV=1 docker exec --user 0:0 -e HOME=/tmp -e HADOOP_USER_NAME=root \
    smartcity_spark_master /opt/bitnami/spark/bin/spark-submit \
    --master spark://spark-master:7077 --total-executor-cores 2 \
    --conf spark.jars.ivy=/tmp/.ivy2 \
    /opt/smartcity/spark/jobs/analitica.py --analisis "$tipo" || break
done
```

Cada proceso correcto termina con `SMARTCITY_B42_RESUMEN=1/1` y produce uno o varios mensajes `SMARTCITY_B42_..._RUTA=...`.

Para probar solo un análisis, por ejemplo el predictivo, usar el mismo comando `spark-submit` con `--analisis predictivo`, sin el bucle.

### 7.2. Ejecutar el análisis n.º 7: anomalías (requiere MongoDB)

El séptimo análisis necesita el conector MongoDB y la variable `MONGO_URI` exportada en el paso 6:

```bash
MSYS_NO_PATHCONV=1 docker exec --user 0:0 -e HOME=/tmp -e HADOOP_USER_NAME=root -e MONGO_URI \
  smartcity_spark_master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 --total-executor-cores 2 \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.mongodb.spark:mongo-spark-connector_2.12:10.4.1 \
  /opt/smartcity/spark/jobs/analitica.py --analisis anomalias
```

Este análisis no está limitado a las 50 filas de demostración de SP-Q07. En la ejecución de referencia generó **10.762 casos marcados** y **38.493 detecciones estadísticas**. No deben interpretarse como 49.255 incidentes independientes ni confundirse con las anomalías originalmente inyectadas por el generador.

### 7.3. Comprobar que se guardaron los ocho análisis

```bash
MSYS_NO_PATHCONV=1 docker exec smartcity_namenode \
  hadoop fs -ls /smartcity/sandbox/analitica
```

Se esperan las carpetas señaladas en la tabla. No se requiere que cada consulta produzca la misma cantidad de registros en otra instalación.

**Limitaciones metodológicas:**

- OpenAQ/Open-Meteo y sintéticos se identifican por su `origen` y no deben confundirse ni mezclarse silenciosamente.
- El análisis `movilidad` estudia contaminación en horarios supuestos como pico: **no demuestra tráfico vehicular real**.
- El predictivo usa el clima **observado** de 2024 para evaluar estimaciones; no es un pronóstico futuro. En las pruebas, los modelos externos no superaron el baseline.
- El análisis de anomalías implementa marcas de calidad y Z-score; el IQR mencionado en el TODO sigue pendiente.

---

## 8. DIEZ consultas obligatorias de Apache Spark SQL (B4.1)

**Archivos:** `spark/sql/SP-Q01.sql` a `spark/sql/SP-Q10.sql`

**Ejecutor:** `spark/jobs/ejecutar_consultas.py`

**Vista base:** `ambiente`, construida leyendo el Parquet del ETL.

**Vistas adicionales:** `eventos` para SP-Q02, `lecturas_crudas` para SP-Q07 y `catalogo_sensores` para SP-Q08.

Las **10 consultas son diferentes de los 8 análisis de B4.2**. Cada SQL se conserva como archivo independiente para su consulta, documentación e integración al dashboard.

| ID | Consulta obligatoria | Datos o técnica principal |
|---|---|---|
| **SP-Q01** | Sensores que generan más información | `GROUP BY`, `COUNT`, ordenamiento |
| **SP-Q02** | Distribución geográfica de eventos | MongoDB `evento`, zonas, JOIN y coordenadas |
| **SP-Q03** | Evolución temporal de indicadores urbanos | Mes, zona, variable y origen; promedio, mínimo, máximo |
| **SP-Q04** | Correlación entre variables | PM2.5 frente al clima; pares horarios y fuentes separadas |
| **SP-Q05** | Ranking de zonas con mayor actividad | `RANK()` y número de sensores/mediciones |
| **SP-Q06** | Tendencias por período | Promedio mensual, `LAG()` y variación porcentual |
| **SP-Q07** | Detección de anomalías | Marcas de calidad MongoDB y Z-score; vista de detalle |
| **SP-Q08** | Agrupamiento por categorías | JOIN con `data/maestros/sensores.csv` y `ROLLUP` |
| **SP-Q09** | Comparación histórica entre años | PM2.5 2023 frente a 2024, con fuentes separadas y cobertura |
| **SP-Q10** | Reporte consolidado | KPIs por zona; exportación de Parquet a HDFS |

### 8.1. Ejecutar las diez consultas de una sola vez

Si se cerró Git Bash después de realizar el ETL, repetir la configuración de `MONGO_URI` del paso 6. **SP-Q02 y SP-Q07 requieren MongoDB**. Las demás consultan principalmente el Parquet; SP-Q08 también necesita el catálogo CSV.

```bash
MSYS_NO_PATHCONV=1 docker exec --user 0:0 -e HOME=/tmp -e HADOOP_USER_NAME=root -e MONGO_URI \
  smartcity_spark_master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 --total-executor-cores 2 \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.mongodb.spark:mongo-spark-connector_2.12:10.4.1 \
  /opt/smartcity/spark/jobs/ejecutar_consultas.py \
  --consultas SP-Q01 SP-Q02 SP-Q03 SP-Q04 SP-Q05 SP-Q06 SP-Q07 SP-Q08 SP-Q09 SP-Q10 \
  --exportar-reporte
```

Si todas tienen éxito, deberán aparecer los diez mensajes `SMARTCITY_SPARK_OK=SP-Qxx` y:

```text
SMARTCITY_SPARK_RESUMEN=10/10
SMARTCITY_SPARK_REPORTE=hdfs://namenode:8020/smartcity/sandbox/reporte_spark_q10
```

El ejecutor muestra normalmente las primeras **20 filas** de cada resultado; **eso no significa que cada consulta tenga solo 20 filas**. SP-Q07, además, puede devolver una muestra limitada por su SQL, diferente del análisis completo B4.2 de anomalías.

### 8.2. Ejecutar consultas individuales o un grupo

Ejemplo de SP-Q01, SP-Q03 y SP-Q05 (estas no requieren conexión a MongoDB durante su ejecución):

```bash
MSYS_NO_PATHCONV=1 docker exec --user 0:0 -e HOME=/tmp \
  smartcity_spark_master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 --total-executor-cores 2 \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  /opt/smartcity/spark/jobs/ejecutar_consultas.py \
  --consultas SP-Q01 SP-Q03 SP-Q05
```

Para probar **SP-Q02 o SP-Q07**, añadir `-e MONGO_URI` a `docker exec` y `--packages org.mongodb.spark:mongo-spark-connector_2.12:10.4.1` a `spark-submit`, tal como en el comando completo del apartado 8.1.

Para exportar únicamente el reporte SP-Q10:

```bash
MSYS_NO_PATHCONV=1 docker exec --user 0:0 -e HOME=/tmp -e HADOOP_USER_NAME=root \
  smartcity_spark_master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 --total-executor-cores 2 \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  /opt/smartcity/spark/jobs/ejecutar_consultas.py \
  --consultas SP-Q10 --exportar-reporte
```

Comprobar el reporte guardado:

```bash
MSYS_NO_PATHCONV=1 docker exec smartcity_namenode \
  hadoop fs -ls -h /smartcity/sandbox/reporte_spark_q10
```

Se espera `_SUCCESS` y al menos un `part-*.snappy.parquet`.

**Limitaciones pendientes de B4.1:** SP-Q09 compara años mediante agregaciones condicionales; el TODO solicita evidenciar `PIVOT` explícitamente. SP-Q06 aún puede combinar fuentes en algunas tendencias, por lo que debe documentarse como análisis del conjunto combinado o ampliarse para separar orígenes. SP-Q10 exporta un reporte HDFS, **todavía no llena las tablas finales de PostgreSQL**.

## 9. Controles de calidad y resultados de referencia

Estos totales corresponden a la ejecución probada en el equipo de la pareja B y **no sustituyen pruebas en cada instalación**:

| Control | Resultado de referencia |
|---|---:|
| Documentos MongoDB `lectura` | 1.068.777 |
| Filas Parquet del ETL | 4.389.705 |
| Sensores con lecturas | 38 |
| Eventos MongoDB | 1.420 |
| Filas de la matriz correlacional | 210 |
| Modelos MLlib entrenados | 7 |
| Filas del espacial por sensores PM2.5 | 20 |
| Detecciones del análisis B4.2 de anomalías (dos métodos) | 49.255 |
| Estado de los análisis | Ocho programas ejecutados; movilidad es un proxy |
| Consultas Spark SQL ejecutadas | 10 de 10 |

Ver las interpretaciones y cifras específicas en `docs/resultados/hallazgos_spark.md`.

## 10. Problemas habituales

| Síntoma | Qué revisar |
|---|---|
| `ModuleNotFoundError: No module named 'numpy'` | Asegurarse de usar los **dos** archivos Compose y reconstruir `smartcity-spark-ml:3.5`; verificar ambos contenedores |
| `Exception authenticating MongoCredential` | Contraseña del **MongoDB Docker** y `authSource=admin`; no usar la contraseña del MongoDB de Windows |
| `Partitioning failed ... collStats` | En las lecturas MongoDB para SP-Q02 y SP-Q07 se utiliza `SinglePartitionPartitioner`; comprobar que se ejecuta el código actualizado |
| `FileNotFoundException` para `etl_ambiente_completa` | No se ha ejecutado el ETL completo o no se usó la ruta indicada en el apartado 6 |
| Error al leer `catalogo_sensores` | Comprobar `data/maestros/sensores.csv` y la carga de la vista en SP-Q08 |
| `WARN WindowExec: No Partition Defined` | Advertencia por una ventana global; no necesariamente es error de ejecución |
| `WARN InstanceBuilder ... JNIBLAS` | MLlib puede usar implementaciones alternativas; confirmar las métricas finales y el código de salida |
| `WARN SparkStringUtils: Truncated ...` | Spark abrevió la descripción del plan, no implica que la consulta falló |
| Puerto 27017 o 5432 ocupado | Cerrar/remapear MongoDB o PostgreSQL de Windows antes de iniciar los servicios Docker |
| Resultados distintos del informe | Comparar snapshots OpenAQ/Open-Meteo, fechas, catálogos, código del generador y carga MongoDB |

## 11. Pendientes para completar el proyecto integral

Esta guía **reproduce la parte de MongoDB y Spark que se implementó y probó**. No implica que estén completos los siguientes elementos del enunciado y del TODO:

1. **Integración B3.4 con PostgreSQL:** carga de agregados y ejecución de procedimientos una vez disponibles las tablas de la pareja A.
2. **Dashboard/Streamlit:** ejecutar las consultas y mostrar resultados, gráficos, filtros y mapa desde la interfaz grupal.
3. **Movilidad real:** actualmente solo hay un proxy ambiental, no sensores de tráfico.
4. **Anomalías con IQR:** falta añadirlo si se busca cumplir literalmente la técnica adicional prevista en el TODO.
5. **Optimización Spark (B4.3):** mediciones comparativas, particionado, `explain`, `cache`, AQE y evidencia en la documentación.
6. **B3.2 integral:** adquisición cruda en HDFS e integración adicional con maestros PostgreSQL, cuando corresponda.
7. **Prueba de instalación limpia:** verificar todos los comandos en otra computadora y corregir diferencias de entorno.

---

**Recordatorio de seguridad y colaboración:** no subir archivos `.env`, claves OpenAQ, credenciales personales, JSONL voluminosos ni volúmenes Docker. Antes de integrar cambios con otras ramas, revisar conflictos en `docker/docker-compose.yml` y acordar una rama de destino con el equipo.
