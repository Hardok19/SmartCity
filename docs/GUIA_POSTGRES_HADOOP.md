# SmartCity — Guía de PostgreSQL y Hadoop (HDFS + MapReduce)

Complementa [GUIA_SPARK.md](GUIA_SPARK.md), que cubre MongoDB y Spark. Los comandos se ejecutan en la raíz del repositorio: los `.sh` con Git Bash (o Linux) y los `.ps1` con PowerShell.

## 1. Levantar el entorno

```powershell
cd docker
docker compose -f docker-compose.yml -f docker-compose.spark-ml.yml up -d --build
docker ps        # postgres, mongo, namenode, datanode1..3, resourcemanager, nodemanager, spark-master, spark-worker
cd ..
copy .env.example .env      # credenciales por defecto del compose
pip install -r requirements.txt
```

| Servicio | Dirección |
|---|---|
| PostgreSQL | `localhost:5433` (5432 dentro de Docker; el 5432 de Windows suele estar ocupado por un PostgreSQL local) |
| HDFS NameNode | http://localhost:9870 |
| YARN | http://localhost:8088 |
| Spark | http://localhost:8080 |

Verificación del clúster (F0.3):

```bash
docker exec smartcity_namenode hdfs dfsadmin -report | grep "Live datanodes"     # 3
docker exec smartcity_resourcemanager yarn node -list                           # 1 nodo RUNNING
docker exec smartcity_namenode hadoop jar /opt/hadoop-3.2.1/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.2.1.jar pi 2 10
```

## 2. PostgreSQL

```powershell
python scripts/pg_init.py              # sql/ddl/001…007 en orden; re-ejecutable (también migra bases anteriores)
python scripts/pg_cargar_maestros.py   # zonas, organismos, 50 sensores, 40 usuarios (idempotente)
```

Las mediciones (`medicion_horaria`) y las tablas `spark_*` las carga **Spark** (B3.4):

```powershell
.\scripts\spark_submit.ps1 etl_trafico_energia     # B3.3 (requiere trafico/energia en HDFS, §3)
.\scripts\spark_submit.ps1 carga_pg                # B3.4: medicion_horaria, agg, spark_kpi_zona, spark_anomalia
```

`scripts/pg_generar_mediciones.py` solo sirve para probar SQL sin Spark: marca sus filas con `origen = 'prueba_pg'`, y `carga_pg` las borra antes de cargar los datos reales.

| Archivo | Contenido |
|---|---|
| `sql/ddl/001_tablas.sql` | Entidades mínimas en 3FN (`tipos_sensor` separa la categoría del sensor) |
| `sql/ddl/002_extensiones.sql` | Variables, `medicion_horaria` particionada por mes (`fn_crear_particiones`), tablas destino de Spark |
| `sql/ddl/003_analitica.sql` | Vista, 2 vistas materializadas, funciones y `sp_refrescar_mv()` |
| `sql/ddl/004_restricciones.sql` | CHECK, NOT NULL, `ON DELETE RESTRICT`, `COMMENT ON` y migración a 3FN |
| `sql/ddl/005_indices.sql` | Índices con su justificación (B-tree compuesto, BRIN, parcial) |
| `sql/ddl/006_calendario.sql` | Dimensión de fechas con feriados de Inglaterra 2023–2024 |
| `sql/ddl/007_benchmark.sql` | `benchmark_resultado` |
| `sql/queries/PG-*.sql` | PG-Q01…Q10 y PG-P01…P07 (cabecera: id, título, técnicas, parámetros) |
| `sql/demo/demo_defensa.sql` | Plan B de la defensa en `psql` |

Optimización (A2.5): `python scripts/pg_optimizacion.py --repeticiones 5` → `docs/resultados/postgres_optimizacion.md`.

Demo en `psql`:

```bash
docker exec -i smartcity_postgres psql -U smartcity -d smartcity_db < sql/demo/demo_defensa.sql
```

## 3. HDFS

```bash
bash hadoop/hdfs/01_estructura.sh                 # /smartcity/{raw,curated,analytics,mapreduce,streaming,sandbox}
python scripts/generar_trafico_sintetico.py && python scripts/generar_energia_sintetica.py
bash hadoop/hdfs/02_cargar_raw.sh                 # raw OpenAQ/Open-Meteo/tráfico/energía + export de Mongo (con conteos)
bash hadoop/hdfs/03_demo.sh                       # comandos, bloques y réplicas -> docs/resultados/hdfs_demo.txt
```

```powershell
.\hadoop\hdfs\04_tolerancia.ps1                   # apaga un DataNode y mide -> docs/resultados/hdfs_tolerancia.md
```

## 4. MapReduce

Ver [hadoop/mapreduce/README.md](../hadoop/mapreduce/README.md). Resumen:

```bash
bash hadoop/mapreduce/ejecutar.sh mr01 && bash hadoop/mapreduce/ejecutar.sh mr02
bash hadoop/mapreduce/ejecutar.sh mr03 && bash hadoop/mapreduce/ejecutar.sh mr04
DESDE=2024-03-01 HASTA=2024-03-31 bash hadoop/mapreduce/ejecutar.sh mr05
```

## 5. Desde Python o el dashboard

```python
import conectores_postgres_hadoop as a
a.estado(); a.kpis_iot(); a.sensores()
a.ejecutar("PG-P05")                       # o cualquier PG-Qxx / PG-Pxx / HD-*
print(a.explain("PG-Q07"))
a.hdfs_ls("/smartcity/raw"); a.hdfs_fsck("/smartcity/raw/trafico")
a.mr_ejecutar("mr01"); a.mr_resultado("mr01")
```

Pruebas: `pytest -q tests/test_conectores_pg_hadoop.py tests/test_mapreduce.py`.

Documentación técnica: [05_modelo_relacional.md](tecnico/05_modelo_relacional.md), [07_estrategia_big_data.md](tecnico/07_estrategia_big_data.md) y el [diagrama E-R](diagramas/er_postgres.md).
