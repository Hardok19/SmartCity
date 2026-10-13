# Guía de Instalación y Prueba — Entorno Docker SmartCity Analytics

Esta guía cubre todo el proceso en Windows: instalar WSL2, instalar Docker Desktop,
clonar el repositorio, levantar los 4 servicios (PostgreSQL, MongoDB, Hadoop, Spark)
y comprobar que cada uno funciona correctamente. Sigan los pasos en orden.

---

## 1. Requisitos previos

- Windows 10 (versión 2004+) o Windows 11.
- ~8 GB de RAM libres (Hadoop + Spark consumen bastante).
- ~10 GB de espacio en disco libre.
- Cuenta de administrador en el equipo (algunos pasos lo requieren).

---

## 2. Instalar WSL2 (Windows Subsystem for Linux)

Docker Desktop en Windows necesita WSL2 como motor por debajo.

1. Abrir **PowerShell como administrador** (click derecho sobre el ícono → *Ejecutar como administrador*).
2. Ejecutar:
   ```powershell
   wsl --install
   ```
   Instala WSL2 junto con una distribución Linux por defecto. Si ya estaba instalado,
   este comando solo lo actualiza — no hay problema en correrlo igual.
3. **Reiniciar la computadora** cuando el instalador lo pida.
4. Después de reiniciar, verificar la instalación:
   ```powershell
   wsl --status
   ```
   Debe mostrar la versión por defecto como **2**. Si muestra 1:
   ```powershell
   wsl --set-default-version 2
   ```
5. Si `wsl --install` da un error de virtualización, hay que habilitarla en el
   BIOS/UEFI (opción tipo *Intel VT-x*, *AMD-V* o *Virtualization Technology* — varía
   según el fabricante de la placa madre).

---

## 3. Instalar Docker Desktop

1. Descargar desde: https://www.docker.com/products/docker-desktop/
2. Ejecutar el instalador, dejando marcada la opción **"Use WSL 2 instead of Hyper-V"**
   (viene marcada por defecto).
3. Reiniciar si lo solicita.
4. Abrir **Docker Desktop** desde el menú de inicio.
   - Puede pedir aceptar términos de servicio — aceptar.
   - Puede ofrecer crear cuenta — **no es necesario**, se puede omitir ("Skip").
5. Esperar a que el ícono de la ballena (bandeja del sistema, junto al reloj) quede
   estable, sin animación de carga.
6. Verificar en **Settings → General** que **"Use the WSL 2 based engine"** esté marcado.

### Confirmar que Docker funciona

```powershell
docker --version
docker compose version
docker run hello-world
```

El último comando descarga una imagen mínima y debe imprimir un mensaje de bienvenida.

---

## 4. Clonar el repositorio del proyecto

1. Si no tienen Git: descargar de https://git-scm.com/downloads e instalar con las
   opciones por defecto.
2. Abrir una terminal en la carpeta donde quieran el proyecto y ejecutar:
   ```powershell
   git clone <URL_DEL_REPOSITORIO>
   cd SmartCity
   ```
3. Confirmar que la carpeta `docker/` tenga esta estructura (importante — los
   scripts de inicialización deben estar en sus subcarpetas, no sueltos):
   ```
   docker/
   ├── docker-compose.yml
   ├── hadoop.env
   ├── postgres-init/
   │   └── 01_schema.sql
   └── mongo-init/
       └── 01_init.js
   ```

---

## 5. Levantar el entorno

```powershell
cd docker
docker compose up -d
```

La primera vez tarda varios minutos (descarga ~4 GB de imágenes entre Postgres, Mongo,
Hadoop y Spark). Las siguientes veces arranca en segundos.

```powershell
docker compose ps
```

Deben aparecer 6 contenedores: `smartcity_postgres`, `smartcity_mongo`,
`smartcity_namenode`, `smartcity_datanode`, `smartcity_spark_master`,
`smartcity_spark_worker`, todos en estado `Up` (Postgres/Mongo idealmente `healthy`).

---

## 6. Probar que las 4 tecnologías funcionan

### 6.1 PostgreSQL

```powershell
docker exec -it smartcity_postgres psql -U smartcity -d smartcity_db -c "\dt"
```

Debe listar las tablas creadas por `01_schema.sql`: `sensores`, `ubicaciones`,
`categorias_sensor`, `organismos`, `usuarios`. Si aparece vacío, revisar que ese
script esté dentro de `docker/postgres-init/` (ver sección 4).

### 6.2 MongoDB

```powershell
docker exec -it smartcity_mongo mongosh -u smartcity -p smartcity123 --authenticationDatabase admin smartcity_db --eval "db.getCollectionNames()"
```

Debe mostrar: `mediciones`, `eventos`, `alertas`.

### 6.3 HDFS (Hadoop)

Interfaz web:
```
http://localhost:9870
```
Ir a la pestaña **"Datanodes"** y confirmar que aparece 1 datanode en estado
**"In Service"** — eso confirma que namenode y datanode se conectaron.

Por comandos:
```powershell
docker exec -it smartcity_namenode hadoop fs -mkdir -p /smartcity/raw
docker exec -it smartcity_namenode hadoop fs -ls /smartcity
```
Debe listar la carpeta `raw` recién creada.

### 6.4 Spark

Interfaz web:
```
http://localhost:8080
```
Debe mostrar el **Spark Master UI** con 1 **Worker** registrado en estado `ALIVE`
(con los cores y memoria configurados).

### 6.5 Conexión desde Python (opcional, para confirmar que todo se integra)

```powershell
pip install psycopg2-binary pymongo hdfs pyspark
```

```python
# PostgreSQL
import psycopg2
conn = psycopg2.connect(host="localhost", port=5432,
    dbname="smartcity_db", user="smartcity", password="smartcity123")
print("Postgres OK")

# MongoDB
from pymongo import MongoClient
client = MongoClient("mongodb://smartcity:smartcity123@localhost:27017/")
print("Mongo OK:", client["smartcity_db"].list_collection_names())

# HDFS
from hdfs import InsecureClient
hdfs_client = InsecureClient("http://localhost:9870", user="root")
print("HDFS OK:", hdfs_client.list("/smartcity"))

# Spark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Test").master("spark://localhost:7077").getOrCreate()
print("Spark OK:", spark.version)
```

Si las 4 secciones imprimen "OK" sin lanzar excepción, el entorno está completo y
listo para que empiecen a cargar datos reales.

---

## 7. Apagar el entorno al terminar de trabajar

```powershell
docker compose down
```
Apaga los contenedores y **conserva los datos** (volúmenes intactos). Esto es lo
que deben usar en el día a día.

⚠️ `docker compose down -v` además **borra todos los datos** — úsenlo solo si
quieren reiniciar todo desde cero (por ejemplo, si editan los scripts de
`postgres-init/` o `mongo-init/` y necesitan que se vuelvan a ejecutar).

---

## 8. Problemas comunes

| Error | Causa | Solución |
|---|---|---|
| `failed to connect to the docker API at npipe:...` | Docker Desktop no está abierto | Abrir Docker Desktop y esperar a que el ícono de la ballena quede estable |
| `request returned 500 Internal Server Error ... dockerDesktopLinuxEngine` | El motor WSL2 no responde bien | Correr `wsl --update` como administrador, reiniciar Docker Desktop, confirmar "Use the WSL 2 based engine" en Settings |
| `wsl --install` da error de virtualización | Virtualización deshabilitada en BIOS | Habilitar Intel VT-x / AMD-V en el BIOS |
| `bitnami/spark:3.5: not found` | Bitnami movió sus imágenes versionadas a un repositorio legacy en 2025 | Usar `bitnamilegacy/spark:3.5` en vez de `bitnami/spark:3.5` en el `docker-compose.yml` |
| Un contenedor no llega a `Up`/`healthy` | Tardó más en iniciar o hay conflicto de puertos | Ver el log: `docker compose logs <nombre_servicio>` (ej. `docker compose logs namenode`) |
| Puerto ya en uso (5432, 27017, etc.) | Ya tienen Postgres/Mongo nativo corriendo en la máquina | Apagar el servicio nativo, o cambiar el puerto expuesto en `docker-compose.yml` (ej. `"5433:5432"`) |
| Tablas/colecciones vacías tras probar Postgres/Mongo | Los scripts de init no están dentro de sus subcarpetas (`postgres-init/`, `mongo-init/`) | Moverlos a la subcarpeta correcta, luego `docker compose down -v` y `docker compose up -d` para que se ejecuten de nuevo |

Si algún error no aparece en esta tabla, copien el mensaje completo de la terminal
para revisarlo — el texto exacto casi siempre indica la causa real.

---

## 9. Resumen rápido (para quien ya pasó por todo esto una vez)

```powershell
cd SmartCity/docker
docker compose up -d
docker compose ps                 # confirmar que todo esté Up
# ... trabajar, cargar datos, correr consultas ...
docker compose down               # al terminar
```
