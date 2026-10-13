# SmartCity Analytics — Entorno Docker

Levanta PostgreSQL, MongoDB, Hadoop (HDFS) y Spark con un solo comando.

## Requisitos

- Docker Desktop instalado y corriendo (incluye Docker Compose).
- ~6-8 GB de RAM libres (Hadoop + Spark consumen bastante).

## 1. Levantar el entorno

```bash
cd smartcity-docker
docker compose up -d
```

La primera vez tarda varios minutos porque descarga las imágenes. Las siguientes veces arranca en segundos.

## 2. Verificar que todo esté corriendo

```bash
docker compose ps
```

Deben verse 6 contenedores en estado `Up`/`healthy`: `smartcity_postgres`, `smartcity_mongo`, `smartcity_namenode`, `smartcity_datanode`, `smartcity_spark_master`, `smartcity_spark_worker`.

Interfaces web para confirmar visualmente:
- HDFS (namenode): http://localhost:9870
- Spark (master): http://localhost:8080

## 3. Datos de conexión

| Tecnología | Host/Puerto | Usuario | Contraseña | Base/DB |
|---|---|---|---|---|
| PostgreSQL | `localhost:5432` | `smartcity` | `smartcity123` | `smartcity_db` |
| MongoDB | `localhost:27017` | `smartcity` | `smartcity123` | `smartcity_db` |
| HDFS (RPC) | `localhost:8020` (desde tu host) / `namenode:8020` (entre contenedores) | — | — | — |
| Spark master | `spark://localhost:7077` | — | — | — |

Ejemplo de conexión desde Python (fuera de Docker, en tu máquina):

```python
# PostgreSQL
import psycopg2
conn = psycopg2.connect(
    host="localhost", port=5432,
    dbname="smartcity_db", user="smartcity", password="smartcity123"
)

# MongoDB
from pymongo import MongoClient
client = MongoClient("mongodb://smartcity:smartcity123@localhost:27017/")
db = client["smartcity_db"]

# HDFS (usando el cliente hdfs vía WebHDFS, puerto 9870)
from hdfs import InsecureClient
hdfs_client = InsecureClient("http://localhost:9870", user="root")

# Spark (PySpark corriendo local, apuntando al master del clúster)
from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .appName("SmartCityAnalytics") \
    .master("spark://localhost:7077") \
    .getOrCreate()
```

Instala las librerías necesarias en tu entorno local (no dentro de Docker):

```bash
pip install psycopg2-binary pymongo hdfs pyspark
```

## 4. Comandos útiles de HDFS

Para ejecutar comandos `hdfs dfs` hay que entrar al contenedor del namenode:

```bash
docker exec -it smartcity_namenode hadoop fs -mkdir -p /smartcity/raw
docker exec -it smartcity_namenode hadoop fs -put /ruta/local/archivo.csv /smartcity/raw/
docker exec -it smartcity_namenode hadoop fs -ls /smartcity/raw
docker exec -it smartcity_namenode hadoop fs -cat /smartcity/raw/archivo.csv
```

Para copiar un archivo desde tu máquina al contenedor primero (y luego a HDFS):

```bash
docker cp archivo.csv smartcity_namenode:/tmp/archivo.csv
docker exec -it smartcity_namenode hadoop fs -put /tmp/archivo.csv /smartcity/raw/
```

## 5. Apagar el entorno

```bash
docker compose down          # apaga los contenedores, conserva los datos (volúmenes)
docker compose down -v       # apaga y BORRA todos los datos — usar con cuidado
```

## 6. Estructura de carpetas

```
smartcity-docker/
├── docker-compose.yml     # definición de los 6 servicios
├── hadoop.env              # variables de configuración de HDFS
├── postgres-init/
│   └── 01_schema.sql       # se ejecuta solo la 1ra vez que se crea el volumen de Postgres
├── mongo-init/
│   └── 01_init.js          # se ejecuta solo la 1ra vez que se crea el volumen de Mongo
└── README.md
```

`postgres-init/` y `mongo-init/` solo corren automáticamente la **primera vez** que se crea
el volumen de datos. Si editan esos scripts después de la primera ejecución, deben borrar
el volumen correspondiente (`docker volume rm smartcity-docker_postgres_data`, por ejemplo)
para que se vuelvan a aplicar — o simplemente corran los cambios manualmente contra la base
ya existente.

## Notas para el equipo

- Las 4 personas deben usar exactamente este mismo `docker-compose.yml` para evitar
  diferencias de entorno entre laptops.
- Suban esta carpeta al repositorio compartido (sin los volúmenes, que Docker gestiona aparte).
- El `depends_on` solo espera a que el contenedor *arranque*, no a que el servicio esté
  listo dentro de él — por eso Postgres y Mongo tienen `healthcheck`; si ven errores de
  conexión justo después de `docker compose up`, esperen unos segundos más.
