# SmartCity — Movilidad con tráfico simulado y ampliación de B3.2

**Rama de trabajo:** `trabajo-b` · **Periodo:** 2023–2024 · **Tecnologías:** Python, HDFS y PySpark 3.5.

## Estado y límites

Este paquete no modifica datos de MongoDB ni las rutas HDFS originales `etl_ambiente_completa` o `analitica/movilidad_proxy_*`.

- **Movilidad:** genera cinco contadores de tráfico **simulados** (T-001 a T-005), uno por zona. Los datos no provienen de Transport for London ni de sensores físicos. El patrón de hora pico es un **supuesto del generador**, no un hallazgo sobre el tráfico real. El análisis Spark calcula las horas pico a partir de los valores generados y las cruza con PM2.5 y precipitación por zona/hora.
- **B3.2 raw:** copia los originales OpenAQ, Open-Meteo y una exportación de `lectura` de MongoDB a `/smartcity/raw`. Un nuevo ETL Spark consume esos archivos, aplica validación, deduplica y crea Parquet particionado por `anio/mes` en `/smartcity/curated/ambiente_hdfs`. Usa temporalmente el catálogo CSV de sensores.
- **Pendiente:** la sustitución del catálogo CSV por un JOIN sobre el esquema definitivo de **PostgreSQL**. Las rutas productivas (`/smartcity/curated/`, `/smartcity/analytics/`) ya están en uso desde el 2026-11-27 (`spark/jobs/rutas.py`). No afirmar que B3.2 completo (incluido PostgreSQL) está validado antes de ello.

## 1. Preparación desde la raíz del repositorio

Usar Git Bash en Windows, desde `~/Documents/BD/SmartCity`. Tener Docker Desktop y los seis contenedores activos. Levantarlos con los **dos** Compose (imagen Spark ML):

```bash
docker compose -f docker/docker-compose.yml -f docker/docker-compose.spark-ml.yml up -d
```

Comprobar que `data/raw/openaq`, `data/raw/openmeteo` y `data/maestros/sensores.csv` contienen los datos adquiridos para 2023/2024. Las fuentes originales se obtienen siguiendo `docs/GUIA_SPARK.md`.

El paquete contiene los archivos nuevos:

```
scripts/generar_trafico_sintetico.py
scripts/subir_crudos_hdfs.py
spark/jobs/movilidad_trafico.py
spark/jobs/etl_ambiente_hdfs.py
docs/GUIA_MOVILIDAD_B32.md
integrar_movilidad.patch
```

Para integrar el octavo análisis sin sustituir ni reescribir `analitica.py` manualmente:

```bash
git apply --check integrar_movilidad.patch
git apply integrar_movilidad.patch
python -m py_compile spark/jobs/analitica.py spark/jobs/movilidad_trafico.py spark/jobs/etl_ambiente_hdfs.py
```

**Si falla `git apply --check`, no fuerce el parche**: puede ejecutar `movilidad_trafico.py` independientemente sin alterar `analitica.py`. El parche se preparó sobre la versión probada de la rama de trabajo de pareja B.

## 2. Generar los 87.720 registros de tráfico SIMULADO

```bash
python scripts/generar_trafico_sintetico.py --anios 2023 2024
wc -l data/raw/trafico/trafico_2023.csv data/raw/trafico/trafico_2024.csv
```

Salidas esperadas: 43.800 filas de datos para 2023 y 43.920 para 2024, más una cabecera por CSV. El generador usa semilla fija 2026, calendario y hora local de Londres (`Europe/London`). `origen=sintetico` en cada registro. `data/raw` debe permanecer fuera del repositorio Git.

## 3. Subir las fuentes crudas a HDFS

El script utiliza `docker cp` para el NameNode y después `hadoop fs -put`; no necesita configurar WebHDFS en Windows. Copia una fuente a la vez, comprueba bytes y **no pisa archivos diferentes** salvo que se especifique `--sobrescribir`.

```bash
python scripts/subir_crudos_hdfs.py --fuentes openaq openmeteo trafico

MSYS_NO_PATHCONV=1 docker exec smartcity_namenode \
  hadoop fs -ls -R /smartcity/raw | head -n 40
```

Árbol esperado:

```
/smartcity/raw/openaq/<id_estacion>/<anio>.csv
/smartcity/raw/openmeteo/zona_<zona>_<anio>.json
/smartcity/raw/trafico/trafico_<anio>.csv
```

Para exportar `lectura` **directamente desde MongoDB** hacia un snapshot HDFS (sin incluir contraseñas en el código o historial):

```bash
# Esta URI debe conectar desde el Python LOCAL (localhost:27017).
read -rsp 'MongoDB URI local: ' MONGO_URI
echo
export MONGO_URI

FECHA_MONGO="$(date -u +%F)"
python scripts/subir_crudos_hdfs.py --fuentes mongo --colecciones lectura --fecha "$FECHA_MONGO"
unset MONGO_URI
```

**Nota:** la exportación de `lectura` crea temporalmente un JSONL grande en el disco local, lo transmite a HDFS y elimina el temporal. Requiere espacio libre suficiente y puede tardar dependiendo del tamaño de la colección. Comprueba que el total de documentos exportados coincida con Mongo al finalizar. Si se vuelven a cargar datos el mismo día, se requiere `--sobrescribir`; no usarlo sin verificar la intención.

Quedará en:

```
/smartcity/raw/mongo_exports/lectura/fecha=AAAA-MM-DD/lectura.jsonl
```

La pareja A puede utilizar posteriormente otras colecciones exportándolas con `--colecciones evento alerta incidente` (comprobar el contrato de sus MapReduce antes de cambiar la ruta).

## 4. Ejecutar ETL B3.2 adicional con archivos RAW de HDFS

Ejecutar en Git Bash. `FECHA_MONGO` debe coincidir con la fecha del snapshot anterior; si se abrió otra terminal, volver a definirla manualmente. El trabajo **NO** requiere conector Mongo Spark porque procesa el JSONL ya almacenado en HDFS.

```bash
set -o pipefail

MSYS_NO_PATHCONV=1 docker exec \
  --user 0:0 -e HOME=/tmp -e HADOOP_USER_NAME=root \
  smartcity_spark_master \
  /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --total-executor-cores 2 \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  /opt/smartcity/spark/jobs/etl_ambiente_hdfs.py \
  --fecha-mongo "$FECHA_MONGO" \
  2>&1 | tee b32_hdfs.log

grep -E 'SMARTCITY_B32_' b32_hdfs.log
```

Entradas: raw CSV/JSON/JSONL. Esquemas `StructType` definidos en código; filtros por rangos y calidad; unión con maestros CSV; prioridad al dato externo directo cuando coincide con la réplica de Mongo; deduplicación por `id_sensor + timestamp + tipo_metrica`. Escribe y relee:

```
hdfs://namenode:8020/smartcity/curated/ambiente_hdfs
```

Etiquetas `fuente_raw` identifican dónde se obtuvo cada fila, evitando contar dos veces mediciones externas presentes también en Mongo. El script imprime entrada, descartes, porcentaje y conteo Parquet. El umbral de aceptación es **al menos 1 millón de mediciones**. Los conteos pueden diferir de la versión Mongo-only porque el conjunto y las prioridades de deduplicación son distintos.

## 5. Ejecutar análisis de tráfico B4.2

La integración con `analitica.py` ejecuta el proxy ambiental previo y además el nuevo análisis **directo del tráfico sintético**:

```bash
set -o pipefail
MSYS_NO_PATHCONV=1 docker exec \
  --user 0:0 -e HOME=/tmp -e HADOOP_USER_NAME=root \
  smartcity_spark_master \
  /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --total-executor-cores 2 \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  /opt/smartcity/spark/jobs/analitica.py --analisis movilidad \
  2>&1 | tee movilidad_trafico.log

grep -E 'SMARTCITY_(MOV_TRAFICO_|B42_MOVILIDAD_)' movilidad_trafico.log
```

**Alternativa si todavía no se aplicó el parche**: ejecutar como programa independiente, sustituyendo `analitica.py --analisis movilidad` por `movilidad_trafico.py` en el comando.

Rutas HDFS nuevas:

```
/smartcity/analytics/movilidad_trafico_perfil_horario
/smartcity/analytics/movilidad_trafico_horas_pico
/smartcity/analytics/movilidad_trafico_correlacion_pm25
/smartcity/analytics/movilidad_trafico_cruce_meteo
```

Esperar `SMARTCITY_MOV_TRAFICO_ENTRADA=87720` con el período completo y **10 filas de horas pico** (5 zonas × laborables/fin de semana). Correlaciones PM2.5 o lluvia requieren que haya horas coincidentes; nunca interpretar una correlación como causalidad ni tráfico simulado como una medición real.

## 6. Verificar sin modificar resultados existentes

```bash
MSYS_NO_PATHCONV=1 docker exec smartcity_namenode \
  hadoop fs -ls /smartcity/curated/ambiente

MSYS_NO_PATHCONV=1 docker exec smartcity_namenode \
  hadoop fs -ls /smartcity/curated/ambiente_hdfs

MSYS_NO_PATHCONV=1 docker exec smartcity_namenode \
  hadoop fs -ls /smartcity/analytics/movilidad_trafico_horas_pico
```

No ejecutar `docker compose down -v`. Los resultados de este paquete quedan en `/smartcity/curated/` y `/smartcity/analytics/` (`spark/jobs/rutas.py`).

## Validación pendiente

Este paquete se ha verificado con compilación sintáctica de Python y generación determinista de CSV; **no se ejecutó Spark/Docker real en el entorno donde se preparó**. Por ello, el funcionamiento completo se confirma con las salidas que devuelva tu clúster.

Para cerrar B3.2 conforme al TODO original falta el JOIN con zonas/sensores de PostgreSQL. Las rutas productivas `/smartcity/curated/` y `/smartcity/analytics/` ya están en uso (2026-11-27). No modificar el esquema de PostgreSQL de la pareja A unilateralmente.
