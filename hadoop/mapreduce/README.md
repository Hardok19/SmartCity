# MapReduce (A4) — Hadoop Streaming con Python

| Job | Pregunta del enunciado | Entrada | Salida (`/smartcity/mapreduce/<job>`, separada por tabuladores) |
|---|---|---|---|
| `mr01` | ¿Cuántas mediciones existen por sensor? | `raw/mongo_exports/lectura/*/lectura.jsonl` | `id_sensor  mediciones` |
| `mr02` | ¿Cuál sensor produjo mayor cantidad de datos? (top N) | salida de `mr01` | `posicion  id_sensor  mediciones` |
| `mr03` | ¿Cuál es la temperatura promedio histórica? | `lectura` | `ambito  zona  anio  temperatura_promedio  lecturas` (global, por zona, por zona-año) |
| `mr04` | ¿Cuál es la cantidad de eventos por zona? | `raw/mongo_exports/evento/*/evento.jsonl` | `ambito  zona  tipo_evento  eventos` |
| `mr05` | ¿Cuál es el comportamiento de los sensores durante un período? | `lectura` + `DESDE`/`HASTA` | `id_sensor  fecha  variable  promedio  minimo  maximo  lecturas` |

Cada carpeta tiene `mapper.py` y `reducer.py`. El reducer también hace de **combiner** con `--combinar`: suma en el nodo del mapper, salvo en mr02, que necesita un único reducer para el top global. El código es compatible con **Python 3.5** (el del NodeManager, Debian 9): sin f-strings.

Diseño:
- Los mappers emiten **suma y conteo**, nunca promedios: el promedio de promedios parciales es incorrecto.
- Las lecturas con `calidad_dato.valido = false` se descartan en mr03 y mr05.
- Las líneas JSON inválidas se cuentan en el contador de Hadoop `SmartCity:lineas_invalidas`, que aparece en la UI de YARN.

## Ejecutar

Desde la raíz del repo, con Git Bash o Linux:

```bash
bash hadoop/mapreduce/ejecutar.sh mr01
bash hadoop/mapreduce/ejecutar.sh mr02                      # después de mr01
bash hadoop/mapreduce/ejecutar.sh mr03
bash hadoop/mapreduce/ejecutar.sh mr04                      # requiere el export de `evento` (B2.6)
DESDE=2024-03-01 HASTA=2024-03-31 bash hadoop/mapreduce/ejecutar.sh mr05
```

`ejecutar.sh` copia el job al NameNode, ejecuta `run_job.sh` (borra la salida anterior y lanza `hadoop jar hadoop-streaming…`) y anota el tiempo en `docs/resultados/mapreduce_tiempos.csv`. El progreso se ve en http://localhost:8088.

Con un segundo argumento se usa otra entrada y la salida va a `sandbox`. Así se prueba en YARN con las muestras:

```bash
docker exec -i smartcity_namenode hdfs dfs -put -f - /smartcity/sandbox/mr_muestra/lectura.jsonl < data/samples/lectura_muestra.jsonl
bash hadoop/mapreduce/ejecutar.sh mr01 /smartcity/sandbox/mr_muestra/lectura.jsonl
```

Desde Python o el dashboard: `conectores_postgres_hadoop.mr_ejecutar("mr01")` y `mr_resultado("mr01")`, o la sección «Consultas por tecnología → HDFS» (HD-MR01…HD-MR05).

## Probar sin Hadoop

```bash
cat data/samples/lectura_muestra.jsonl | python hadoop/mapreduce/mr01/mapper.py | sort | python hadoop/mapreduce/mr01/reducer.py
pytest -q tests/test_mapreduce.py       # compara contra un cálculo independiente en Python
```
