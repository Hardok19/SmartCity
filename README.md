# SmartCity Analytics

Proyecto II del curso CE-3191 Bases de Datos (II semestre 2026). Es una arquitectura de datos para una ciudad
inteligente que integra **PostgreSQL, MongoDB, Apache Hadoop (HDFS + MapReduce) y Apache Spark**, con un dashboard en
Streamlit. Muestra con experimentos **qué tecnología conviene para cada tipo de dato y cada necesidad de
procesamiento**.

- **Dominio:** Ambiente (calidad del aire y clima de Londres, 2023–2024), con Movilidad y Energía simuladas.
- **Fuentes reales:** OpenAQ y Open-Meteo, ampliadas con sensores IoT virtuales anclados a los datos reales.
- **Volumen:** ~1,07 M documentos en MongoDB, 4,38 M filas procesadas por Spark y 2,6 M filas en PostgreSQL.

Diagrama de arquitectura: [docs/diagramas/arquitectura.md](docs/diagramas/arquitectura.md).

| Motor | Rol |
|---|---|
| PostgreSQL 16 | Maestros en 3FN (sensores, zonas, organismos, usuarios, calendario), mediciones horarias limpias particionadas por mes, resultados de Spark |
| MongoDB 7 | Lecturas IoT con esquema variable, eventos, alertas e incidentes con texto libre |
| Hadoop 3.2.1 | HDFS con 3 DataNodes y réplica ×2 para crudos, exports y Parquet; MapReduce en YARN |
| Spark 3.5 | ETL, 10 consultas Spark SQL, 8 análisis (con MLlib), carga a PostgreSQL y streaming con alertas |
| Streamlit | Vista única con KPIs de 4 grupos, filtros, gráficos, mapa y tabla; consultas por tecnología; comparación tecnológica |

## Documentación

| Documento | Contenido |
|---|---|
| [docs/tecnico/](docs/tecnico/) | Documento técnico: 01 problema · 02 fuentes · 03 arquitectura · 04 justificación tecnológica (matriz) · 05 modelo relacional · 06 modelo documental · 07 estrategia Big Data · 08 procesamiento · 09 resultados · 10 conclusiones |
| [docs/atributos/ATRIBUTOS.md](docs/atributos/ATRIBUTOS.md) | Documento de atributos (AF, AC) y [diccionario de datos](docs/atributos/DICCIONARIO_DATOS.md) |
| [docs/datos/](docs/datos/) | Fuentes y licencias, perfil de los datos, generación sintética, volúmenes |
| [docs/diagramas/](docs/diagramas/) | Arquitectura, flujo de datos, E-R, modelo documental, HDFS, despliegue (Mermaid + PNG) |
| [docs/resultados/](docs/resultados/) | Evidencias: benchmarks E0–E7, optimización PG / Mongo / Spark, HDFS, MapReduce, hallazgos de Spark |
| [GUIA_POSTGRES_HADOOP.md](docs/GUIA_POSTGRES_HADOOP.md) · [GUIA_SPARK.md](docs/GUIA_SPARK.md) · [DOCKER_SETUP.md](DOCKER_SETUP.md) | Guías detalladas por tecnología |
| [TODO.md](TODO.md) | Plan, decisiones (D-01…D-06) y estado de cada tarea |

## Requisitos

- Windows 10/11 con WSL2, o Linux / macOS.
- Docker Desktop con **≥ 8 GB de RAM** asignados y ~15 GB libres en disco.
- Python 3.11 o superior.
- Git Bash para los scripts `.sh` en Windows.
- Para regenerar los datos reales: una clave gratuita de OpenAQ (`OPENAQ_API_KEY`). Open-Meteo no la necesita.
- Opcional, para exportar diagramas, gráficos y PDF: Microsoft Edge o Google Chrome.

## Instalación desde cero

Todos los comandos se ejecutan desde la raíz del repositorio. Los `.ps1` van en PowerShell y los `.sh` en Git Bash.

### 1. Entorno

```powershell
cd docker
docker compose -f docker-compose.yml -f docker-compose.spark-ml.yml up -d --build
cd ..
copy .env.example .env                 # credenciales por defecto del compose; agregar OPENAQ_API_KEY si se descarga
python -m venv .venv; .\.venv\Scripts\activate
pip install -r requirements.txt
```

Verificación: `docker ps` muestra 10 contenedores, `docker exec smartcity_namenode hdfs dfsadmin -report` muestra
3 DataNodes vivos y http://localhost:8080 muestra el worker de Spark.

### 2. Datos

```powershell
python scripts/descargar_openmeteo.py                  # clima (sin clave)
python scripts/descargar_openaq.py                     # calidad del aire (OPENAQ_API_KEY en .env), o bien:
python scripts/descargar_openaq_archivo.py             # la misma salida sin clave (archivo público de OpenAQ en S3)
python scripts/generar_sinteticos.py --salida data/interim/mongo/final    # lecturas IoT, alertas, eventos, incidentes
python scripts/generar_trafico_sintetico.py
python scripts/generar_energia_sintetica.py
python scripts/perfilar_fuentes.py                     # -> docs/datos/perfil_fuentes.md
```

El catálogo `data/maestros/sensores.csv` está versionado (`scripts/generar_maestros.py` lo regenera). Los generadores
usan la semilla 2026: la misma ejecución da los mismos datos.

### 3. Carga

```powershell
python scripts/pg_init.py                              # DDL sql/ddl/001…007 (re-ejecutable)
python scripts/pg_cargar_maestros.py                   # zonas, organismos, 50 sensores, 40 usuarios
python scripts/mongo_admin.py todo                     # colecciones, validadores e índices
python scripts/mongo_cargar.py --dir data/interim/mongo/final --reiniciar --lote 10000
```

```bash
bash hadoop/hdfs/01_estructura.sh                      # /smartcity/{raw,curated,analytics,mapreduce,streaming,sandbox}
bash hadoop/hdfs/02_cargar_raw.sh                      # crudos + export de MongoDB, con conteos
```

### 4. Procesamiento

```powershell
.\scripts\spark_submit.ps1 etl_ambiente --salida hdfs://namenode:8020/smartcity/curated/ambiente
.\scripts\spark_submit.ps1 etl_trafico_energia
.\scripts\spark_submit.ps1 ejecutar_consultas --exportar
.\scripts\spark_submit.ps1 analitica --analisis descriptivo temporal correlacional predictivo espacial movilidad anomalias comparativo
.\scripts\spark_submit.ps1 carga_pg                    # resultados de Spark -> PostgreSQL (idempotente)
```

```bash
for j in mr01 mr02 mr03 mr04; do bash hadoop/mapreduce/ejecutar.sh $j; done
DESDE=2024-03-01 HASTA=2024-03-31 bash hadoop/mapreduce/ejecutar.sh mr05
```

### 5. Evidencias y experimentos

```powershell
python scripts/pg_optimizacion.py --repeticiones 5                     # A2.5
python scripts/mongo_experimento_indices.py --repeticiones 3           # B2.5
.\scripts\spark_submit.ps1 optimizar_spark --pruebas todas             # B4.3
python scripts/benchmark_comparacion.py --preparar --medir             # C1: E0–E7 a 100k, 1 M y 5 M filas
python scripts/graficos_benchmarks.py                                  # C3: gráficos PNG
.\hadoop\hdfs\04_tolerancia.ps1                                        # A3.4 / E6: DataNode caído
```

```bash
bash hadoop/hdfs/03_demo.sh                                            # comandos HDFS, bloques y réplicas
```

### 6. Dashboard y pruebas

```powershell
streamlit run dashboard.py                 # http://localhost:8501
python -m pytest -q                        # las pruebas de integración se omiten si un servicio no responde
```

### 7. Documentación generada

```powershell
python scripts/generar_diccionario.py      # docs/atributos/DICCIONARIO_DATOS.md (lee PostgreSQL en vivo)
python scripts/exportar_diagramas.py       # docs/diagramas/img/*.png
python scripts/ensamblar_documento.py      # docs/entrega/SmartCity_documento_tecnico.pdf
python scripts/empaquetar_entrega.py       # zip del código sin datos, .venv ni .env
```

## Servicios

| Servicio | Dirección | Credenciales (desarrollo) |
|---|---|---|
| Dashboard | http://localhost:8501 | — |
| PostgreSQL | `localhost:5433`, base `smartcity_db` | `smartcity` / ver `.env` |
| MongoDB | `localhost:27017`, base `smartcity_db` | `smartcity` / ver `.env` (`authSource=admin`) |
| HDFS NameNode | http://localhost:9870 | — |
| YARN | http://localhost:8088 | — |
| Spark | http://localhost:8080 | — |

PostgreSQL se publica en el **5433** porque el 5432 suele estar ocupado por un PostgreSQL local.

## Estructura del repositorio

```
contrato.py · comun.py                 contrato de datos, Filtros, Consulta, configuración (.env)
conectores_postgres_hadoop.py          pareja A: PostgreSQL + HDFS + MapReduce
conectores_mongo_spark.py              pareja B: MongoDB + Spark (+ streaming)
dashboard.py · dashboard_app/          interfaz Streamlit
config/                                zonas, estaciones OpenAQ, umbrales
data/                                  maestros y muestras versionados; raw/ e interim/ se generan (ignorados por git)
docker/                                docker-compose, imagen del NodeManager, init scripts
sql/ddl · sql/queries · sql/demo       DDL, 17 consultas PG-*, demo de la defensa
mongo/                                 colecciones, índices, CRUD, consultas MG-*, export
hadoop/hdfs · hadoop/mapreduce         operaciones HDFS, tolerancia a fallos, jobs mr01–mr05
spark/jobs · spark/sql                 jobs PySpark y consultas SP-*
scripts/                               descarga, generación, carga, experimentos, documentación
tests/                                 pruebas unitarias y de integración
docs/                                  documentación (ver tabla de arriba)
```

## Fuentes de datos y licencias

| Fuente | Licencia | Atribución |
|---|---|---|
| [OpenAQ](https://openaq.org) | La del proveedor original de cada estación (Defra / UK-AIR y redes locales de Londres), según indica la API ([política de datos](https://docs.openaq.org/docs/data-policy)) | «Datos de calidad del aire: OpenAQ y sus proveedores originales» |
| [Open-Meteo](https://open-meteo.com) | [CC BY 4.0](https://open-meteo.com/en/license); reanálisis ERA5 de Copernicus / ECMWF | «Weather data by Open-Meteo.com» |
| Datos sintéticos | Generados por el proyecto (`scripts/generar_*.py`) | Marcados con `origen = sintetico` |

Detalle en [docs/datos/FUENTES.md](docs/datos/FUENTES.md). Software usado: PostgreSQL (PostgreSQL License), MongoDB
Community Server (SSPL), Apache Hadoop y Apache Spark (Apache 2.0), Streamlit, pandas, Plotly y pydeck.
