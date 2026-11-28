# 7. Estrategia Big Data (Hadoop: HDFS + MapReduce sobre YARN)

## 7.1 Clúster

`docker/docker-compose.yml` (D-05): **1 NameNode + 3 DataNodes** con `dfs.replication = 2`, más **YARN** (1 ResourceManager + 1 NodeManager con Python 3 para Hadoop Streaming). Imágenes `bde2020/hadoop-*:2.0.0-hadoop3.2.1-java8`; el NodeManager se construye desde [docker/hadoop/Dockerfile.nodemanager](../../docker/hadoop/Dockerfile.nodemanager).

| Componente | Rol | UI |
|---|---|---|
| NameNode | Guarda solo la **metadata**: árbol de directorios y la lista de bloques de cada archivo con los DataNodes que tienen cada réplica | http://localhost:9870 |
| DataNode ×3 | Guardan los **bloques** (128 MB por defecto) y envían latidos cada 3 s | :9864–9866 |
| ResourceManager | Reparte CPU y memoria entre los jobs de YARN | http://localhost:8088 |
| NodeManager | Ejecuta las tareas map/reduce en contenedores YARN | :8042 |

## 7.2 Estructura de almacenamiento

Creada con [hadoop/hdfs/01_estructura.sh](../../hadoop/hdfs/01_estructura.sh):

```
/smartcity
├── raw/                         datos tal como llegan (inmutables, «fuente de verdad» para reprocesar)
│   ├── openaq/<estacion>/<anio>.csv
│   ├── openmeteo/zona_<zona>_<anio>.json
│   ├── trafico/  energia/       series simuladas (D-03)
│   └── mongo_exports/<coleccion>/fecha=AAAA-MM-DD/<coleccion>.jsonl   (export de MongoDB, B2.6)
├── curated/                     Parquet limpio particionado anio=/mes= (ETL Spark)
│   ├── ambiente/                desde MongoDB (etl_ambiente.py): lo leen análisis, consultas y carga_pg
│   ├── ambiente_hdfs/           desde los crudos de raw/ (etl_ambiente_hdfs.py)
│   └── trafico_energia/         tráfico y energía simulados (etl_trafico_energia.py)
├── analytics/                   resultados de Spark para el dashboard
│   ├── descriptivo/ … comparativo_*/   una carpeta por salida de los 8 análisis (analitica.py)
│   ├── consultas_spark/SP-Qxx/  resultado de cada consulta Spark SQL
│   └── reporte_q10/             reporte consolidado SP-Q10
├── mapreduce/mr01 … mr05/       salidas de los jobs MapReduce
├── streaming/entrada|alertas|checkpoints/   reto adicional (Structured Streaming)
└── sandbox/                     pruebas y operaciones de la UI (único lugar donde la UI escribe o borra)
```

Decisiones:

- **raw inmutable + capas.** Si una regla de limpieza cambia, se reprocesa desde `raw` sin volver a descargar. Según [spark/jobs/rutas.py](../../spark/jobs/rutas.py), los ETL escriben en `/smartcity/curated/` (`ambiente`, `ambiente_hdfs`, `trafico_energia`) y los análisis y consultas en `/smartcity/analytics/`; `sandbox/` queda para pruebas, benchmarks y el experimento B4.3. Spark escribe con la misma réplica ×2 que el resto del clúster (`get_spark()` fija `dfs.replication = 2`).
- **Particiones estilo Hive** (`fecha=…/`, `anio=/mes=`): Spark y MapReduce leen solo las carpetas que piden (*partition pruning*).
- **Pocos archivos grandes (problema de archivos pequeños).** OpenAQ y Open-Meteo se guardan por estación o zona y **año**, no por día. El NameNode mantiene en memoria unos 150 bytes por cada archivo y bloque, y MapReduce lanza al menos una tarea por archivo: 10 años × 365 días × 40 sensores = 146.000 archivos diminutos saturarían el NameNode y crearían 146.000 tareas de map.
- **CSV/JSON sin gzip.** gzip no es divisible: un `.gz` de 1 GB lo procesa una sola tarea. Para los datos curados se usa Parquet + Snappy, que es divisible y columnar.

## 7.3 Bloques y réplicas (evidencia)

[docs/resultados/hdfs_demo.txt](../resultados/hdfs_demo.txt) (`hadoop/hdfs/03_demo.sh`) muestra `-mkdir, -put, -ls, -cat, -get, -rm, -du, -stat, -setrep` y `fsck`:

- El mismo archivo de 2,66 MB ocupa **1 bloque** con el tamaño por defecto (128 MB) y **3 bloques** con `-D dfs.blocksize=1MB`.
- Cada bloque tiene **2 réplicas en DataNodes distintos**; HDFS elige dónde ponerlas y `fsck -files -blocks -locations` las muestra.
- `-setrep 3` lleva cada bloque a los 3 nodos; `-setrep 2` vuelve a 2 y borra la réplica sobrante.
- El archivo que devuelve `-get` es idéntico al original (mismo md5): la división en bloques es transparente para el cliente.

**¿Qué pasa cuando un archivo se distribuye entre nodos?** El cliente pide al NameNode dónde escribir. El archivo se corta en bloques y cada bloque se envía en *pipeline* al primer DataNode, que lo reenvía al segundo (réplica). Al leer, el NameNode devuelve la ubicación de cada bloque y el cliente lee cada uno del DataNode más cercano que esté vivo. Los bloques de un archivo pueden estar en nodos distintos y leerse en paralelo.

## 7.4 Tolerancia a fallos (evidencia)

[docs/resultados/hdfs_tolerancia.md](../resultados/hdfs_tolerancia.md) (`hadoop/hdfs/04_tolerancia.ps1`), con `datanode2` apagado:

| Hecho | Medición |
|---|---|
| Lectura completa del archivo con el nodo apagado | **OK**: md5 idéntico, 1,8 s |
| El NameNode declara muerto al DataNode | **97 s** (teórico: 2 × 30 s de *recheck* + 10 × 3 s de latido = 90 s) |
| Los bloques que quedaron con 1 réplica vuelven a tener 2 (en otro nodo) | **152 s**, sin intervención manual |
| El nodo vuelve al encenderlo | 7 s; las réplicas sobrantes se eliminan |

El único punto único de fallo es el **NameNode**: si cae, los datos siguen en los DataNodes, pero nadie sabe dónde está cada bloque. En producción se usa NameNode en alta disponibilidad (activo + *standby* con JournalNodes y ZooKeeper). Aquí no se configura por los recursos del equipo.

## 7.5 Procesamiento cercano a los datos: MapReduce

Cinco jobs de Hadoop Streaming en Python ([hadoop/mapreduce/](../../hadoop/mapreduce/)) para las preguntas del enunciado: mediciones por sensor (mr01), sensor con más datos (mr02), temperatura promedio histórica (mr03), eventos por zona (mr04) y comportamiento de los sensores en un período (mr05). YARN intenta ejecutar cada tarea map en el nodo que guarda su bloque (**localidad de datos**): se mueve el código, que pesa unos KB, en lugar de los datos, que pesan GB.

- Los mappers emiten **sumas y conteos**, no promedios, porque el promedio de promedios parciales es incorrecto. El **combiner** suma en el nodo del mapper y reduce el tráfico de red del *shuffle*.
- Sobre la muestra de prueba, cada job tarda unos **17–19 s**, casi todo el costo fijo de YARN (pedir contenedores y arrancar las JVM). Ese costo se amortiza solo con volúmenes grandes. Es el mismo argumento de la §8.6: para consultas puntuales o datos pequeños conviene PostgreSQL o MongoDB.
- Tiempos con los datos completos: `docs/resultados/mapreduce_tiempos.csv` (lo escribe `hadoop/mapreduce/ejecutar.sh`).

## 7.6 ¿Por qué este problema justifica un sistema distribuido?

- **Volumen y crecimiento:** cada sensor virtual produce una lectura cada 30 min (≈ 17.500 por año); 38 sensores en 2 años dan ≈ 1,07 M documentos (≈ 380 MB en JSON) y 4,4 M mediciones en formato largo. Una ciudad real tiene **miles** de sensores con frecuencia de segundos: ×100 sensores × 30 de frecuencia = ×3.000 → del orden de **1 TB por año** solo de lecturas crudas.
- **Escritura continua y lectura secuencial masiva:** el histórico crudo no se actualiza, se agrega. HDFS está hecho para «escribir una vez, leer muchas veces» en recorridos secuenciales grandes.
- **Escalabilidad horizontal:** agregar un DataNode suma disco y ancho de banda sin detener el clúster, y el *balancer* redistribuye los bloques. Un servidor único escala solo comprando una máquina más grande.
- **Tolerancia a fallos con hardware común:** con miles de discos, que alguno falle es algo cotidiano. La replicación lo absorbe (§7.4).
- **Costo:** se almacena barato en discos comunes y se procesa con Spark o MapReduce junto a los datos.

## 7.7 ¿Qué pasa si el volumen crece ×100?

| Componente | Hoy | ×100 | Qué cambia |
|---|---|---|---|
| HDFS raw | 448 MB | ≈ 45 GB (≈ 90 GB con 2 réplicas) | Agregar DataNodes. El NameNode sigue holgado si se mantienen archivos grandes (≈ 360 bloques de 128 MB) |
| MapReduce / Spark | segundos–minutos | El tiempo crece casi lineal **por nodo** | Más NodeManagers/workers: se reparten los bloques. Parquet particionado lee solo lo necesario |
| MongoDB | 1 M documentos | 100 M | *Sharding* por `sensor_id` o zona; TTL o archivado del histórico en HDFS |
| PostgreSQL | 2,6 M filas horarias (427 MB) | 263 M (≈ 43 GB) | Particiones mensuales (ya están) + BRIN; se guardan agregados, no lecturas crudas |

Cada tecnología se queda con lo que mejor sabe hacer: el histórico masivo crudo vive en HDFS y se procesa con Spark, y PostgreSQL y MongoDB guardan solo lo que se consulta en línea.
