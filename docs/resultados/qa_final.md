# QA final (E7) — 2026-11-27

## 1. Instalación desde cero

| Paso | Resultado |
|---|---|
| `git clone` del repositorio en otra carpeta | OK |
| `python -m venv` + `pip install -r requirements.txt` en un entorno **limpio** (Python 3.13.14, Windows 11) | **OK** (F1.6). Versiones resueltas: pandas 3.1.1, numpy 2.5.5, pyarrow 26.0.1, pyspark 3.5.10, streamlit 1.67.0, plotly 7.2.0, pydeck 0.9.3, pymongo 4.20.2, psycopg2-binary 2.9.13, SQLAlchemy 2.1.9, pytest 9.2.0, Markdown 3.11.4 |
| `python -m pytest -q` con ese entorno | Sin datos: 43 pasan y 28 se omiten (integración de MongoDB). **Con los datos completos cargados: 90 pasan, 0 se omiten** (49 s; incluye `test_rutas.py`) |
| Importar los conectores, el dashboard y los scripts | OK |

## 2. Dashboard contra los servicios reales

Recorrido automático con `streamlit.testing` (sin datos simulados):

| Página | Resultado |
|---|---|
| Vista principal | Sin errores. Con los datos completos: **con caché 0,5 s** (objetivo < 5 s: ✅); la primera carga, con la caché vacía, tarda 30 s (seis agregaciones de MongoDB sobre 1,07 M documentos de 2–4 s cada una y la lectura de tráfico desde HDFS) |
| Consultas por tecnología | PostgreSQL 17 consultas, MongoDB 15, HDFS 8 (operaciones + 5 MapReduce), Spark 10; sin errores. Las 17 de PostgreSQL devuelven filas (también las ambientales P01, P03, P04 y P07) |
| Streaming en vivo | Sin errores |
| Comparación tecnológica | 7 gráficos de experimentos, matriz, diagramas y respuestas; sin errores |

**Errores encontrados y corregidos en este QA:**

1. La tabla de detalle de la vista principal se caía (`No objects to concatenate`) cuando todas las fuentes venían vacías
   para el filtro elegido. Ahora muestra el estado vacío.
2. `sensores()` y `kpis_iot()` ignoraban el filtro de tipo de medición, y los KPIs de movilidad y energía ignoraban el
   filtro de sensor. Corregido; lo detectó la prueba de filtros (§3).

## 3. Filtros (D3)

`python scripts/prueba_filtros.py` → [prueba_filtros.md](prueba_filtros.md): cada filtro (fechas, perfil horario, zona,
tipo de medición, sensor, rango de valores) cambia **todos** los KPIs y conjuntos de datos de PostgreSQL, MongoDB y
Spark: **42 combinaciones cambian, 0 no cambian, 0 sin datos**.

## 4. Volúmenes

| Requisito | En este entorno (2026-11-27) |
|---|---|
| PostgreSQL ≥ 100k | ✅ 2.632.112 (`medicion_horaria`) |
| MongoDB ≥ 500k | ✅ 1.068.584 (`lectura`) |
| Spark ≥ 1 M | ✅ 4.380.538 (ETL desde Mongo) · 4.397.164 (ETL desde los crudos de HDFS) |

Detalle en [VOLUMENES.md](../datos/VOLUMENES.md).

## 5. Instalación completa con datos (2026-11-27)

Se siguió el README de principio a fin sobre los contenedores ya levantados:

| Paso | Resultado |
|---|---|
| `descargar_openaq_archivo.py` (sin clave) | 10 archivos, 168.824 filas, ~1 min |
| `descargar_openmeteo.py` | 10 archivos, 0 nulos |
| `generar_sinteticos.py --salida data/interim/mongo/final` | 1.068.584 lecturas, 1.190 alertas, 1.428 eventos, 3.000 incidentes en 23 s |
| `mongo_admin.py colecciones` + `mongo_cargar.py` + `mongo_admin.py indices` | 0 rechazados, 61 s |
| `02_cargar_raw.sh` | crudos + export de Mongo; conteo Mongo = HDFS en las 4 colecciones |
| `etl_ambiente`, `etl_ambiente_hdfs`, `ejecutar_consultas --exportar`, `analitica` (8), `carga_pg` | 4.380.538 filas · 10/10 · 8/8 · PostgreSQL con conteo Spark = PG |
| MapReduce mr01…mr05 sobre el export real | 5/5 en 17–26 s cada uno |
| `mongo_experimento_indices.py`, `pg_optimizacion.py`, `optimizar_spark` | cifras en `mongo_indices.md`, `postgres_optimizacion.md`, `spark_optimizacion.md` |
| `exportar_diagramas.py` | 7 diagramas PNG en `docs/diagramas/img/` (los gráficos y el documento final los arma el equipo) |

Errores corregidos en esta corrida: Edge devolvía el control antes de escribir la captura (ahora se espera el archivo y
se reintenta), el diagrama de arquitectura salía recortado (ventana más ancha) y las etiquetas del gráfico E1 se
ubicaban mal en el eje logarítmico.

## 6. Pendiente (a cargo del equipo)

- [ ] Documento técnico final y material de la defensa (guion y preguntas).
- [ ] Capturas de la interfaz, para el documento y como plan B de la defensa si falla la demo en vivo.
- [ ] Capturas de la Spark UI de B4.3 (los planes en texto ya están en `spark_optimizacion_planes.txt`).
- [ ] Ensayo cronometrado de la defensa (objetivo: 25 min).
- [ ] Antes de la defensa, abrir la vista principal una vez para llenar la caché: la primera carga tarda ~30 s y la
      caché dura 12 h («Actualizar datos» en la barra lateral la vacía).
