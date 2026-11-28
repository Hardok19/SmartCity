# Plan de volúmenes y datos sintéticos (F1.5, D-03, D-06)

Qué es real y qué es sintético, cuántos registros hay en cada tecnología y cómo se alcanzan los mínimos del enunciado.
**Medido** = contado en este entorno tras la ejecución completa del 2026-11-27 (OpenAQ del archivo público S3,
Open-Meteo, generadores con semilla 2026).

## 1. Mínimos del enunciado

| Requisito | Mínimo | Valor | Estado | Cómo se alcanza |
|---|---|---|---|---|
| Registros PostgreSQL | 100.000 | 2.632.112 en `medicion_horaria` (medido) | ✅ | Spark carga las mediciones ambientales horarias (2.456.672) y las de tráfico + energía (175.440) con `carga_pg` (B3.4); además 87.720 agregados, 35.104 KPI y 104.137 anomalías |
| Documentos MongoDB | 500.000 | 1.068.584 lecturas (medido) | ✅ | 5 estaciones reales + 30 sensores IoT virtuales cada 30 min durante 2 años (D-06) |
| Registros procesados por Spark | 1.000.000 | 4.380.538 (medido; 4.397.164 desde los crudos de HDFS) | ✅ | `explode` de las mediciones de cada documento a formato largo |
| Total recomendado | > 5 M | ≈ 8,1 M de registros del proyecto (PG + Mongo + Parquet) | ✅ | Suma de lo anterior, sin contar copias ni benchmarks |
| Tamaño del dataset | 100 MB (aceptable) · 500 MB–1 GB (bueno) | 448 MB de crudos en HDFS (896 MB con réplica), 427 MB en PostgreSQL y 387 MB en MongoDB, sin contar benchmarks | ✅ bueno | Ver §3 |

## 2. Real frente a sintético

| Conjunto | Origen | Registros | Formato | Tecnología |
|---|---|---|---|---|
| OpenAQ, 5 estaciones, 2023–2024 | **Real** | 168.824 filas estación-hora-parámetro (cobertura real: PM2.5 solo en Centro y Este) | CSV | HDFS → Mongo → Spark |
| Open-Meteo, 5 zonas, 2023–2024 | **Real** (reanálisis) | 5 × 17.544 h × 4 variables | JSON | HDFS → Mongo → Spark |
| Sensores IoT virtuales (30) | Sintético anclado a lo real | ≈ 1,0 M lecturas (1.068.584 con las 10 reales) | JSONL | Mongo → HDFS → Spark |
| Alertas / eventos / incidentes | Sintético derivado | 1.190 / 1.428 / 3.000 | JSONL | Mongo |
| Tráfico (5 contadores) | Sintético (D-03) | 87.720 | CSV | HDFS → Spark → PostgreSQL |
| Energía (5 medidores) | Sintético (D-03) | 87.720 | CSV | HDFS → Spark → PostgreSQL |
| Catálogo (50 sensores, 10 organismos, 40 usuarios, 5 zonas, 731 días) | Maestro (sensores reales + virtuales) | < 1.000 | CSV / SQL | PostgreSQL |
| Benchmarks C1 | Sintético | 100k + 1 M + 5 M por motor | CSV | PostgreSQL, Mongo, HDFS/Spark |

Los mecanismos de generación están en [GENERACION_SINTETICA.md](GENERACION_SINTETICA.md). Todo lo sintético lleva
`origen = sintetico` y semilla 2026.

## 3. Tamaño por tecnología

| Ubicación | Contenido | Tamaño |
|---|---|---|
| `data/interim/mongo/final/*.jsonl` | Documentos para Mongo | 365 MB (medido) |
| MongoDB `lectura` | 1.068.584 documentos | 387 MB sin comprimir · 60 MB almacenados + 66 MB de índices (medido) |
| PostgreSQL `medicion_horaria` | 2.632.112 filas en 24 particiones | 427 MB con índices (medido) |
| `data/raw/trafico`, `data/raw/energia` | CSV de tráfico y energía | 5,3 MB + 4,7 MB (medido) |
| `data/raw/openaq`, `data/raw/openmeteo` | Crudos reales | 6,2 MB + 3,5 MB (medido; detalle en [perfil_fuentes.md](perfil_fuentes.md)) |
| HDFS `/smartcity/raw` | Crudos + export de Mongo | 448 MB lógicos, 896 MB con réplica ×2 (medido) |
| HDFS Parquet ambiental | 4,38 M filas | 15,5 MB (columnar + Snappy; medido) |
| `data/interim/bench` | CSV de benchmark 100k + 1 M + 5 M | 5,1 + 51 + 255 MB (medido) |
| PostgreSQL `bench.lectura_5000000` | 5 M filas + índice | 501 MB (medido) |
| MongoDB `bench_lectura_5000000` | 5 M documentos | 132 MB almacenados + 112 MB de índices (598 MB sin comprimir, medido) |

## 4. Cómo reproducir los conteos

```bash
docker exec smartcity_postgres psql -U smartcity -d smartcity_db -c "SELECT count(*) FROM medicion_horaria"
docker exec smartcity_mongo mongosh -u smartcity -p --authenticationDatabase admin smartcity_db \
  --eval "db.lectura.countDocuments()"
docker exec smartcity_namenode hdfs dfs -du -s -h /smartcity/raw /smartcity/sandbox
python scripts/perfilar_fuentes.py
```

`spark/jobs/etl_ambiente.py` imprime `SMARTCITY_ETL_DOCUMENTOS_ENTRADA` y `SMARTCITY_ETL_FILAS_PARQUET`, y
`carga_pg.py` compara el conteo de Spark con el de PostgreSQL al terminar.
