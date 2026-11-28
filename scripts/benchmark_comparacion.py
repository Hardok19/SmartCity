#!/usr/bin/env python3
"""C1 — Experimentos de comparación tecnológica E0…E7 con el MISMO dato lógico en cada motor.

Dato: ``lectura_<n>`` = (id_sensor, zona, tipo_metrica, timestamp, valor) para los 50 sensores del catálogo de
PostgreSQL, una lectura cada 15 min por sensor desde 2023-01-01 UTC. Valores con perfil horario + ruido gaussiano
(semilla 2026; la escala chica es prefijo de la grande). Se carga igual en:

  PostgreSQL  bench.lectura_<n>            (COPY + índice (id_sensor, timestamp DESC))
  MongoDB     bench_lectura_<n>            (insert_many + índice {id_sensor: 1, timestamp: -1})
  HDFS/Spark  /smartcity/sandbox/bench/csv/lectura_<n>.csv  ->  parquet/lectura_<n>

Experimentos (1 calentamiento + N repeticiones, se guarda la mediana):
  E0 carga (ingesta) en cada motor
  E1 agregación promedio por zona-hora                          PG · Mongo · Spark
  E2 búsqueda puntual: últimas 100 lecturas de S-011            PG (índice) · Mongo (índice) · Spark (recorre todo)
  E3 join sensor-tipo-categoría-ubicación-organismo             PG JOIN · Mongo $lookup (por documento y agrupando
                                                                antes) · Spark (broadcast)
  E4 nuevo tipo de sensor con campos nuevos                     PG (DDL en transacción revertida) · Mongo (0 DDL)
  E5 Spark con 1 frente a 2 núcleos (consulta E1)
  E6 HDFS con un DataNode caído (cifras de A3.4, docs/resultados/hdfs_tolerancia.md)
  E7 índices: con y sin índice en PG y Mongo (consulta E2) + plan / documentos examinados

Salida: tabla ``benchmark_resultado`` (PostgreSQL), docs/resultados/benchmarks.csv y benchmarks.md.
Las corridas parciales se combinan con el CSV existente (misma clave experimento/tecnología/variante/escala).

  python scripts/benchmark_comparacion.py --preparar --medir --escalas 100000 1000000 5000000
  python scripts/benchmark_comparacion.py --medir --experimentos E1 E2 --sin-spark      # solo PG y Mongo
  python scripts/benchmark_comparacion.py --informe                                     # rehace el .md
"""
from __future__ import annotations

import argparse
import csv
import io
import json
import math
import platform
import re
import statistics
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import psycopg2
from pymongo import ASCENDING, DESCENDING, MongoClient

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ))
from comun import CONFIG  # noqa: E402

DIR_DATOS = RAIZ / "data" / "interim" / "bench"
CSV_SALIDA = RAIZ / "docs" / "resultados" / "benchmarks.csv"
MD_SALIDA = RAIZ / "docs" / "resultados" / "benchmarks.md"
HDFS_BASE = "/smartcity/sandbox/bench"
INICIO = datetime(2023, 1, 1, tzinfo=timezone.utc)
SENSOR_PUNTUAL = "S-011"
SEMILLA = 2026
COLUMNAS = ["experimento", "tecnologia", "variante", "escala_filas", "segundos", "detalle", "medido_en"]


# ------------------------------------------------------------------------------------------- utilidades
def pg():
    con = psycopg2.connect(host=CONFIG.pg_host, port=CONFIG.pg_port, dbname=CONFIG.pg_db, user=CONFIG.pg_user,
                           password=CONFIG.pg_password)
    con.autocommit = True
    with con.cursor() as cur:
        cur.execute("SET timezone = 'UTC'")
    return con


def mongo():
    return MongoClient(CONFIG.mongo_uri, serverSelectionTimeoutMS=5000)[CONFIG.mongo_db]


def docker(*args, entrada=None, timeout=900):
    r = subprocess.run(["docker", *args], input=entrada, capture_output=True, text=True, timeout=timeout,
                       encoding="utf-8", errors="replace")
    if r.returncode != 0:
        raise RuntimeError(f"docker {' '.join(args[:4])}…: {r.stderr[-600:]}")
    return r.stdout


def medir(funcion, repeticiones):
    funcion()                                  # calentamiento: caché del motor y del sistema de archivos
    tiempos, resultado = [], None
    for _ in range(repeticiones):
        t0 = time.perf_counter()
        resultado = funcion()
        tiempos.append(round(time.perf_counter() - t0, 4))
    return statistics.median(tiempos), tiempos, resultado


def fila(experimento, tecnologia, variante, escala, segundos, **detalle):
    print(f"  {experimento:<3} {tecnologia:<10} {variante:<34} {escala or '':>9} {segundos:10.4f} s")
    return {"experimento": experimento, "tecnologia": tecnologia, "variante": variante, "escala_filas": escala,
            "segundos": round(float(segundos), 4), "detalle": json.dumps(detalle, ensure_ascii=False, default=str),
            "medido_en": datetime.now(timezone.utc).isoformat(timespec="seconds")}


def catalogo():
    with pg() as con, con.cursor() as cur:
        cur.execute("""SELECT s.id_sensor, u.nombre_zona, s.tipo_sensor FROM sensores s
                       JOIN ubicaciones u USING (id_ubicacion) ORDER BY s.id_sensor""")
        return cur.fetchall()


# ------------------------------------------------------------------------------------------- datos
def generar(n: int, sensores) -> Path:
    """CSV determinista de n filas (mismo contenido para los tres motores)."""
    DIR_DATOS.mkdir(parents=True, exist_ok=True)
    ruta = DIR_DATOS / f"lectura_{n}.csv"
    if ruta.exists() and sum(1 for _ in ruta.open("rb")) == n + 1:
        return ruta
    rng = np.random.default_rng(SEMILLA)
    ids = np.array([s[0] for s in sensores]); zonas = np.array([s[1] for s in sensores])
    tipos = np.array([s[2] for s in sensores])
    i = np.arange(n)
    s, k = i % len(sensores), i // len(sensores)
    minutos = k * 15
    hora = (minutos // 60) % 24
    ruido = rng.normal(0, 1, n)
    tipo = tipos[s]
    perfil = {
        "calidad_aire": 12 + 6 * np.sin(2 * np.pi * (hora - 2) / 24) + 3 * ruido,        # PM2.5 µg/m3
        "clima": 20 + 5 * np.sin(2 * np.pi * (hora - 9) / 24) + 1.5 * ruido,             # °C
        "trafico": 300 + 700 * np.exp(-((hora - 7.5) ** 2) / 3) + 650 * np.exp(-((hora - 17.5) ** 2) / 3)
                   + 60 * ruido,                                                         # vehículos/h
        "energia": 900 + 350 * np.sin(2 * np.pi * (hora - 13) / 24) + 40 * ruido,        # kWh
    }
    valor = np.zeros(n)
    for t, v in perfil.items():
        m = tipo == t
        valor[m] = v[m]
    valor = np.round(np.clip(valor, 0, None), 2)
    ts = pd.to_datetime(INICIO) + pd.to_timedelta(minutos, unit="m")
    df = pd.DataFrame({"id_sensor": ids[s], "zona": zonas[s], "tipo_metrica": tipo,
                       "timestamp": ts.strftime("%Y-%m-%d %H:%M:%S"), "valor": valor})
    df.to_csv(ruta, index=False)
    return ruta


def cargar_pg(n, ruta):
    t = f"bench.lectura_{n}"
    with pg() as con, con.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS bench")
        cur.execute(f"DROP TABLE IF EXISTS {t}")
        cur.execute(f"""CREATE TABLE {t} (id_sensor VARCHAR(20) NOT NULL, zona VARCHAR(50) NOT NULL,
                        tipo_metrica VARCHAR(30) NOT NULL, "timestamp" TIMESTAMPTZ NOT NULL,
                        valor DOUBLE PRECISION NOT NULL)""")
        t0 = time.perf_counter()
        with ruta.open("r", encoding="utf-8") as f:
            cur.copy_expert(f"COPY {t} FROM STDIN WITH (FORMAT csv, HEADER true)", f)
        copia = time.perf_counter() - t0
        t0 = time.perf_counter()
        cur.execute(f'CREATE INDEX ix_bench_{n}_sensor_ts ON {t} (id_sensor, "timestamp" DESC)')
        indice = time.perf_counter() - t0
        cur.execute(f"ANALYZE {t}")
        cur.execute(f"SELECT count(*), pg_total_relation_size('{t}') FROM {t}")
        filas, tam = cur.fetchone()
    return [fila("E0", "PostgreSQL", "copy", n, copia, filas=filas, bytes_tabla_e_indice=tam),
            fila("E0", "PostgreSQL", "crear_indice", n, indice)]


def cargar_mongo(n, ruta):
    db = mongo()
    col = db[f"bench_lectura_{n}"]
    col.drop()
    t0 = time.perf_counter()
    for parte in pd.read_csv(ruta, chunksize=50_000):
        parte["timestamp"] = pd.to_datetime(parte["timestamp"], utc=True)
        col.insert_many(parte.to_dict("records"), ordered=False)
    insercion = time.perf_counter() - t0
    t0 = time.perf_counter()
    col.create_index([("id_sensor", ASCENDING), ("timestamp", DESCENDING)], name="ix_sensor_ts")
    indice = time.perf_counter() - t0
    db.client.admin.command("fsync")            # fuerza el checkpoint para que storageSize sea real
    st = db.command("collStats", col.name)
    return [fila("E0", "MongoDB", "insert_many", n, insercion, documentos=col.estimated_document_count(),
                 bytes_datos=st.get("size"), bytes_almacenados=st.get("storageSize"),
                 bytes_indices=st.get("totalIndexSize")),
            fila("E0", "MongoDB", "crear_indice", n, indice)]


def catalogo_mongo():
    """Copia el catálogo normalizado de PostgreSQL a colecciones bench_* (para E3 con $lookup)."""
    db = mongo()
    consultas = {
        "bench_sensor": "SELECT id_sensor AS _id, tipo_sensor, id_ubicacion, id_organismo FROM sensores",
        "bench_tipo_sensor": "SELECT tipo_sensor AS _id, id_categoria FROM tipos_sensor",
        "bench_categoria": "SELECT id_categoria AS _id, nombre_categoria AS nombre FROM categorias_sensor",
        "bench_ubicacion": "SELECT id_ubicacion AS _id, nombre_zona, distrito FROM ubicaciones",
        "bench_organismo": "SELECT id_organismo AS _id, nombre FROM organismos",
    }
    with pg() as con, con.cursor() as cur:
        for coleccion, sql in consultas.items():
            cur.execute(sql)
            nombres = [d[0] for d in cur.description]
            docs = [dict(zip(nombres, r)) for r in cur.fetchall()]
            db[coleccion].drop()
            db[coleccion].insert_many(docs)


def subir_hdfs(n, ruta):
    destino = f"{HDFS_BASE}/csv/lectura_{n}.csv"
    nn = CONFIG.contenedor_namenode
    docker("exec", nn, "hdfs", "dfs", "-mkdir", "-p", f"{HDFS_BASE}/csv")
    docker("cp", str(ruta), f"{nn}:/tmp/{ruta.name}")
    t0 = time.perf_counter()
    docker("exec", nn, "hdfs", "dfs", "-put", "-f", f"/tmp/{ruta.name}", destino)
    segundos = time.perf_counter() - t0
    docker("exec", nn, "rm", "-f", f"/tmp/{ruta.name}")
    salida = docker("exec", nn, "hdfs", "fsck", destino, "-blocks")
    bloques = re.search(r"Total blocks \(validated\):\s+(\d+)", salida)
    return [fila("E0", "HDFS", "put_csv", n, segundos, bytes=ruta.stat().st_size,
                 bloques=int(bloques.group(1)) if bloques else None, replicacion=2)]


# ------------------------------------------------------------------------------------------- consultas
SQL_E1 = """SELECT count(*) AS grupos, round(sum(p)::numeric, 4) AS suma FROM (
              SELECT zona, date_trunc('hour', "timestamp") AS hora, avg(valor) AS p
              FROM {t} GROUP BY 1, 2) x"""
SQL_E2 = f"""SELECT "timestamp", valor FROM {{t}} WHERE id_sensor = '{SENSOR_PUNTUAL}'
             ORDER BY "timestamp" DESC LIMIT 100"""
SQL_E3 = """SELECT count(*) AS grupos, sum(lecturas) AS total FROM (
              SELECT o.nombre, c.nombre_categoria, u.distrito, count(*) AS lecturas, avg(l.valor) AS prom
              FROM {t} l
              JOIN sensores s           ON s.id_sensor = l.id_sensor
              JOIN tipos_sensor ts      ON ts.tipo_sensor = s.tipo_sensor
              JOIN categorias_sensor c  ON c.id_categoria = ts.id_categoria
              JOIN ubicaciones u        ON u.id_ubicacion = s.id_ubicacion
              LEFT JOIN organismos o    ON o.id_organismo = s.id_organismo
              GROUP BY 1, 2, 3) x"""

PIPE_E1 = [{"$group": {"_id": {"z": "$zona", "h": {"$dateTrunc": {"date": "$timestamp", "unit": "hour"}}},
                       "p": {"$avg": "$valor"}}},
           {"$group": {"_id": None, "grupos": {"$sum": 1}, "suma": {"$sum": "$p"}}}]


def _lookups(campo_sensor):
    return [
        {"$lookup": {"from": "bench_sensor", "localField": campo_sensor, "foreignField": "_id", "as": "s"}},
        {"$unwind": "$s"},
        {"$lookup": {"from": "bench_tipo_sensor", "localField": "s.tipo_sensor", "foreignField": "_id", "as": "t"}},
        {"$unwind": "$t"},
        {"$lookup": {"from": "bench_categoria", "localField": "t.id_categoria", "foreignField": "_id", "as": "c"}},
        {"$unwind": "$c"},
        {"$lookup": {"from": "bench_ubicacion", "localField": "s.id_ubicacion", "foreignField": "_id", "as": "u"}},
        {"$unwind": "$u"},
        {"$lookup": {"from": "bench_organismo", "localField": "s.id_organismo", "foreignField": "_id", "as": "o"}},
        {"$unwind": {"path": "$o", "preserveNullAndEmptyArrays": True}},
    ]


CLAVE_E3 = {"o": "$o.nombre", "c": "$c.nombre", "d": "$u.distrito"}
FINAL_E3 = {"$group": {"_id": None, "grupos": {"$sum": 1}, "total": {"$sum": "$lecturas"}}}
PIPE_E3_DOC = _lookups("id_sensor") + [
    {"$group": {"_id": CLAVE_E3, "lecturas": {"$sum": 1}, "prom": {"$avg": "$valor"}}}, FINAL_E3]
PIPE_E3_AGR = [{"$group": {"_id": "$id_sensor", "n": {"$sum": 1}, "suma": {"$sum": "$valor"}}}] + _lookups("_id") + [
    {"$group": {"_id": CLAVE_E3, "lecturas": {"$sum": "$n"}, "suma": {"$sum": "$suma"}}}, FINAL_E3]


def plan_pg(cur, sql):
    cur.execute("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + sql)
    plan = cur.fetchone()[0][0]
    nodos = []

    def recorrer(nodo):
        nodos.append(nodo["Node Type"])
        for hijo in nodo.get("Plans", []):
            recorrer(hijo)
    recorrer(plan["Plan"])
    return {"nodos": nodos, "ms_servidor": plan.get("Execution Time"),
            "bloques_leidos": plan["Plan"].get("Shared Hit Blocks", 0) + plan["Plan"].get("Shared Read Blocks", 0)}


def medir_pg(n, rep, experimentos):
    t, filas = f"bench.lectura_{n}", []
    con = pg()
    cur = con.cursor()
    if "E1" in experimentos:
        med, ts, res = medir(lambda: (cur.execute(SQL_E1.format(t=t)), cur.fetchone())[1], rep)
        filas.append(fila("E1", "PostgreSQL", "group_by_sql", n, med, grupos=res[0], suma=float(res[1]), tiempos=ts))
    if "E2" in experimentos or "E7" in experimentos:
        sql = SQL_E2.format(t=t)
        med, ts, res = medir(lambda: (cur.execute(sql), cur.fetchall())[1], rep)
        plan = plan_pg(cur, sql)
        con_idx = dict(filas=len(res), tiempos=ts, plan=plan)
        cur.execute("SET enable_indexscan = off; SET enable_bitmapscan = off; SET enable_indexonlyscan = off")
        med_sin, ts_sin, res_sin = medir(lambda: (cur.execute(sql), cur.fetchall())[1], rep)
        plan_sin = plan_pg(cur, sql)
        cur.execute("RESET enable_indexscan; RESET enable_bitmapscan; RESET enable_indexonlyscan")
        if "E2" in experimentos:
            filas.append(fila("E2", "PostgreSQL", "con_indice", n, med, **con_idx))
        if "E7" in experimentos:
            filas.append(fila("E7", "PostgreSQL", "con_indice", n, med, **con_idx))
            filas.append(fila("E7", "PostgreSQL", "sin_indice", n, med_sin, filas=len(res_sin), tiempos=ts_sin,
                              plan=plan_sin))
    if "E3" in experimentos:
        med, ts, res = medir(lambda: (cur.execute(SQL_E3.format(t=t)), cur.fetchone())[1], rep)
        filas.append(fila("E3", "PostgreSQL", "join_sql", n, med, grupos=res[0], total=int(res[1]), tiempos=ts))
    con.close()
    return filas


def medir_mongo(n, rep, experimentos, max_lookup):
    db = mongo()
    col, filas = db[f"bench_lectura_{n}"], []
    if "E1" in experimentos:
        def e1():
            r = list(col.aggregate(PIPE_E1, allowDiskUse=True))[0]
            return r["grupos"], r["suma"]
        med, ts, res = medir(e1, rep)
        filas.append(fila("E1", "MongoDB", "aggregate_group", n, med, grupos=res[0], suma=round(res[1], 4), tiempos=ts))
    if "E2" in experimentos or "E7" in experimentos:
        filtro, proy = {"id_sensor": SENSOR_PUNTUAL}, {"_id": 0, "timestamp": 1, "valor": 1}

        def consulta(hint=None):
            cur = col.find(filtro, proy).sort("timestamp", DESCENDING).limit(100)
            return list(cur.hint(hint) if hint else cur)

        def explicar(hint=None):
            cmd = {"find": col.name, "filter": filtro, "projection": proy, "sort": {"timestamp": -1}, "limit": 100}
            if hint:
                cmd["hint"] = hint
            e = db.command("explain", cmd, verbosity="executionStats")["executionStats"]
            etapa = e["executionStages"]
            etapas = []
            while etapa:
                etapas.append(etapa["stage"])
                etapa = etapa.get("inputStage")
            return {"etapas": etapas, "docs_examinados": e["totalDocsExamined"],
                    "claves_examinadas": e["totalKeysExamined"], "ms_servidor": e["executionTimeMillis"]}
        med, ts, res = medir(consulta, rep)
        con_idx = dict(filas=len(res), tiempos=ts, plan=explicar())
        med_sin, ts_sin, res_sin = medir(lambda: consulta([("$natural", 1)]), rep)
        if "E2" in experimentos:
            filas.append(fila("E2", "MongoDB", "con_indice", n, med, **con_idx))
        if "E7" in experimentos:
            filas.append(fila("E7", "MongoDB", "con_indice", n, med, **con_idx))
            filas.append(fila("E7", "MongoDB", "sin_indice", n, med_sin, filas=len(res_sin), tiempos=ts_sin,
                              plan=explicar({"$natural": 1})))
    if "E3" in experimentos:
        def e3(pipeline):
            r = list(col.aggregate(pipeline, allowDiskUse=True))[0]
            return r["grupos"], r["total"]
        med, ts, res = medir(lambda: e3(PIPE_E3_AGR), rep)
        filas.append(fila("E3", "MongoDB", "lookup_tras_agrupar", n, med, grupos=res[0], total=res[1], tiempos=ts))
        if n <= max_lookup:                      # ~50 µs por $lookup: sobre 100k ya tarda decenas de segundos
            if n <= 100_000:
                med, ts, res = medir(lambda: e3(PIPE_E3_DOC), rep)
            else:                                # una sola corrida, sin calentamiento (minutos)
                t0 = time.perf_counter()
                res = e3(PIPE_E3_DOC)
                med = time.perf_counter() - t0
                ts = [round(med, 4)]
            filas.append(fila("E3", "MongoDB", "lookup_por_documento", n, med, grupos=res[0], total=res[1],
                              tiempos=ts))
    return filas


def medir_e4(n, rep):
    """Nuevo tipo de sensor 'ruido' con campos nuevos (nivel_db, frecuencia_hz, bateria_pct)."""
    filas, nuevos = [], 10_000
    t = f"bench.lectura_{n}"
    pasos = [
        ("pg_insert_tipo_sensor", "INSERT INTO tipos_sensor (tipo_sensor, descripcion, id_categoria) "
                                  "SELECT 'ruido_bench', 'Sonómetro (experimento E4)', min(id_categoria) "
                                  "FROM categorias_sensor"),
        ("pg_add_column_nullable", f"ALTER TABLE {t} ADD COLUMN nivel_db REAL"),
        ("pg_add_column_default_constante", f"ALTER TABLE {t} ADD COLUMN frecuencia_hz REAL DEFAULT 0"),
        ("pg_add_column_default_volatil", f"ALTER TABLE {t} ADD COLUMN bateria_pct REAL DEFAULT (random() * 100)"),
        ("pg_add_column_jsonb", f"ALTER TABLE {t} ADD COLUMN extra JSONB"),
    ]
    tiempos = {nombre: [] for nombre, _ in pasos}
    tiempos["pg_insert_10k_nuevo_tipo"] = []
    filas_nuevas = [(f"R-{i % 5 + 1:03d}", "Zona Centro", "ruido", INICIO + timedelta(minutes=i), 55.0 + i % 30,
                     55.0 + i % 30, 1000.0, 80.0) for i in range(nuevos)]
    for _ in range(rep + 1):
        con = psycopg2.connect(host=CONFIG.pg_host, port=CONFIG.pg_port, dbname=CONFIG.pg_db,
                               user=CONFIG.pg_user, password=CONFIG.pg_password)
        try:
            with con.cursor() as cur:
                for nombre, sql in pasos:
                    t0 = time.perf_counter()
                    cur.execute(sql)
                    tiempos[nombre].append(time.perf_counter() - t0)
                buf = io.StringIO()
                for r in filas_nuevas:
                    buf.write("\t".join(str(x) for x in r) + "\n")
                buf.seek(0)
                t0 = time.perf_counter()
                cur.copy_expert(f"COPY {t} (id_sensor, zona, tipo_metrica, \"timestamp\", valor, nivel_db, "
                                "frecuencia_hz, bateria_pct) FROM STDIN", buf)
                tiempos["pg_insert_10k_nuevo_tipo"].append(time.perf_counter() - t0)
        finally:
            con.rollback()                       # el experimento no deja cambios
            con.close()
    for nombre, ts in tiempos.items():
        ts = ts[1:]                              # descarta el calentamiento
        reescribe = nombre == "pg_add_column_default_volatil"
        filas.append(fila("E4", "PostgreSQL", nombre, n, statistics.median(ts), tiempos=[round(x, 4) for x in ts],
                          reescribe_tabla=reescribe, sentencias_ddl=0 if "insert" in nombre else 1))

    col = mongo()[f"bench_lectura_{n}"]
    docs = [{"id_sensor": f"R-{i % 5 + 1:03d}", "zona": "Zona Centro", "tipo_metrica": "ruido",
             "timestamp": INICIO + timedelta(minutes=i), "valor": 55.0 + i % 30,
             "mediciones": {"nivel_db": 55.0 + i % 30, "frecuencia_hz": 1000.0},
             "dispositivo": {"firmware": "3.0", "bateria_pct": 80.0}, "e4": True} for i in range(nuevos)]
    ins, con_exists = [], []
    for _ in range(rep + 1):
        col.delete_many({"e4": True})
        t0 = time.perf_counter()
        col.insert_many([dict(d) for d in docs], ordered=False)
        ins.append(time.perf_counter() - t0)
        t0 = time.perf_counter()
        encontrados = col.count_documents({"tipo_metrica": "ruido", "mediciones.nivel_db": {"$exists": True}})
        con_exists.append(time.perf_counter() - t0)
    col.delete_many({"e4": True})
    filas.append(fila("E4", "MongoDB", "insert_10k_campos_nuevos", n, statistics.median(ins[1:]),
                      tiempos=[round(x, 4) for x in ins[1:]], sentencias_ddl=0))
    filas.append(fila("E4", "MongoDB", "consulta_exists_campo_nuevo", n, statistics.median(con_exists[1:]),
                      encontrados=encontrados, sin_indice=True, sentencias_ddl=0))
    return filas


def medir_e6():
    """E6 = A3.4: se toma de docs/resultados/hdfs_tolerancia.md (el script mide con un nodo apagado de verdad)."""
    md = RAIZ / "docs" / "resultados" / "hdfs_tolerancia.md"
    if not md.exists():
        return []
    texto = md.read_text(encoding="utf-8-sig")
    buscar = lambda patron: (float(m.group(1).replace(",", ".")) if (m := re.search(patron, texto)) else None)  # noqa: E731
    lectura = buscar(r"md5 idéntico al original, ([\d.,]+) s")
    muerto = buscar(r"declara muerto el DataNode \| (\d+) s")
    rerep = buscar(r"2 réplicas vivas \| (\d+) s")
    reingreso = buscar(r"reportarse al encenderlo \| (\d+) s")
    filas = []
    for variante, valor in [("lectura_con_nodo_caido", lectura), ("deteccion_nodo_muerto", muerto),
                            ("re_replicacion_completa", rerep), ("reingreso_nodo", reingreso)]:
        if valor is not None:
            filas.append(fila("E6", "HDFS", variante, None, valor, fuente="hdfs_tolerancia.md (A3.4)",
                              lectura_ok=True, replicacion=2, datanodes=3))
    return filas


def spark(argumentos, cores=2, timeout=3600):
    import conectores_mongo_spark as b
    t0 = time.perf_counter()
    codigo, salida = b.lanzar_job_spark("benchmark_spark", *argumentos, timeout=timeout, cores=cores)
    total = time.perf_counter() - t0
    if codigo != 0:
        raise RuntimeError(f"benchmark_spark terminó con código {codigo}:\n{salida[-2000:]}")
    filas = []
    for linea in salida.splitlines():
        if linea.startswith("SMARTCITY_BENCH "):
            d = json.loads(linea[len("SMARTCITY_BENCH "):])
            filas.append(fila(d["experimento"], d["tecnologia"], d["variante"], d["escala"], d["segundos"],
                              **d["detalle"]))
        elif linea.startswith("SMARTCITY_BENCH_"):
            print("  " + linea)
    return filas, total


# ------------------------------------------------------------------------------------------- salida
CLAVE = ["experimento", "tecnologia", "variante", "escala_filas"]


def combinar(nuevas):
    previas = pd.read_csv(CSV_SALIDA, dtype={"escala_filas": "Int64"}) if CSV_SALIDA.exists() else pd.DataFrame(columns=COLUMNAS)
    nuevo = pd.DataFrame(nuevas, columns=COLUMNAS)
    nuevo["escala_filas"] = nuevo["escala_filas"].astype("Int64")
    todo = pd.concat([previas, nuevo], ignore_index=True).drop_duplicates(CLAVE, keep="last")
    todo = todo.sort_values(CLAVE, na_position="first").reset_index(drop=True)
    CSV_SALIDA.parent.mkdir(parents=True, exist_ok=True)
    todo.to_csv(CSV_SALIDA, index=False, quoting=csv.QUOTE_MINIMAL)
    return todo


def guardar_pg(df):
    with pg() as con, con.cursor() as cur:
        cur.execute("DELETE FROM benchmark_resultado WHERE experimento ~ '^E[0-9]$'")
        for r in df.itertuples():
            cur.execute("""INSERT INTO benchmark_resultado (experimento, tecnologia, variante, escala_filas, segundos,
                           detalle, medido_en) VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                        (r.experimento, r.tecnologia, r.variante,
                         None if pd.isna(r.escala_filas) else int(r.escala_filas), float(r.segundos), r.detalle,
                         r.medido_en))


def fmt(x):
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return "—"
    if x >= 100:
        return f"{x:,.0f}".replace(",", ".")
    if x >= 1:
        return f"{x:.2f}".replace(".", ",")
    return f"{x * 1000:.1f} ms".replace(".", ",")


def escala_txt(n):
    if pd.isna(n):
        return "—"
    n = int(n)
    return f"{n // 1_000_000} M" if n % 1_000_000 == 0 else f"{n // 1000}k"


NOTAS = {
    "E0": "La conversión a Parquet de 100k incluye el primer job de la sesión de Spark (carga de clases y "
          "conexión con los ejecutores), por eso tarda más que la de 1 M.",
    "E2": "`job_completo_con_arranque` es un `spark-submit` completo (JVM, registro en el master, ejecutores y "
          "consulta), medido con la escala más chica.",
    "E3": "`lookup_por_documento` con más de 100k filas se corre una sola vez, sin calentamiento (tarda minutos); "
          "con 5 M no se ejecuta.",
    "E4": "Cada paso de PostgreSQL corre en una transacción que se revierte; el de MongoDB borra los documentos "
          "insertados al terminar.",
    "E6": "Cifras de `hadoop/hdfs/04_tolerancia.ps1` (A3.4), que apaga de verdad `smartcity_datanode2`.",
}


def informe(df=None):
    if df is None:
        if not CSV_SALIDA.exists():
            print("No hay resultados todavía.")
            return
        df = pd.read_csv(CSV_SALIDA, dtype={"escala_filas": "Int64"})
    info = json.loads(df.loc[df["experimento"] == "INFO", "detalle"].iloc[0]) if (df["experimento"] == "INFO").any() else {}
    df = df[df["experimento"] != "INFO"]
    titulos = {
        "E0": "E0 · Carga (ingesta) del mismo CSV",
        "E1": "E1 · Agregación promedio por zona-hora",
        "E2": f"E2 · Búsqueda puntual: últimas 100 lecturas de {SENSOR_PUNTUAL}",
        "E3": "E3 · Join sensor → tipo → categoría → ubicación → organismo",
        "E4": "E4 · Nuevo tipo de sensor con campos nuevos (10.000 lecturas)",
        "E5": "E5 · Spark con 1 frente a 2 núcleos (consulta E1)",
        "E6": "E6 · HDFS con un DataNode caído",
        "E7": f"E7 · Índices: con y sin índice (consulta E2)",
    }
    lineas = ["# Comparación tecnológica — resultados de los experimentos (C1)", "",
              "Generado por `scripts/benchmark_comparacion.py`. Mediana de las repeticiones tras 1 calentamiento; "
              "los tiempos de PostgreSQL y MongoDB se miden desde el cliente Python con la conexión abierta, y los de "
              "Spark dentro del job ya iniciado (el arranque del job se informa aparte en E2). "
              "Datos CSV: [benchmarks.csv](benchmarks.csv); también en la tabla `benchmark_resultado`.", ""]
    if info:
        lineas += ["**Entorno:** " + ", ".join(f"{k} = {v}" for k, v in info.items()), ""]
    for exp, titulo in titulos.items():
        d = df[df["experimento"] == exp]
        if d.empty:
            continue
        lineas += [f"## {titulo}", ""]
        escalas = sorted(d["escala_filas"].dropna().unique())
        if escalas:
            lineas.append("| Motor | Variante | " + " | ".join(escala_txt(e) for e in escalas) + " |")
            lineas.append("|---|---|" + "---|" * len(escalas))
            for (tec, var), g in d.groupby(["tecnologia", "variante"], sort=False):
                celdas = []
                for e in escalas:
                    v = g.loc[g["escala_filas"] == e, "segundos"]
                    celdas.append(fmt(float(v.iloc[0])) if len(v) else "—")
                lineas.append(f"| {tec} | `{var}` | " + " | ".join(celdas) + " |")
        else:
            lineas += ["| Motor | Medida | Segundos |", "|---|---|---|"]
            for r in d.itertuples():
                lineas.append(f"| {r.tecnologia} | `{r.variante}` | {fmt(float(r.segundos))} |")
        lineas += ["", "Tiempos en segundos (o ms cuando es < 1 s)." + (" " + NOTAS[exp] if exp in NOTAS else ""), ""]
    MD_SALIDA.write_text("\n".join(lineas), encoding="utf-8")
    print(f"-> {MD_SALIDA.relative_to(RAIZ)}")


def entorno():
    try:
        ncpu, mem = docker("info", "--format", "{{.NCPU}} {{.MemTotal}}").split()
        mem = f"{int(mem) / 2**30:.1f} GiB"
    except Exception:  # noqa: BLE001
        ncpu, mem = "?", "?"
    with pg() as con, con.cursor() as cur:
        cur.execute("SHOW server_version"); version_pg = cur.fetchone()[0]
        cur.execute("SHOW shared_buffers"); sb = cur.fetchone()[0]
    version_mongo = mongo().command("buildInfo")["version"]
    return fila("INFO", "PostgreSQL", "entorno", None, 0, cpus_docker=ncpu, memoria_docker=mem,
                postgresql=version_pg, shared_buffers=sb, mongodb=version_mongo, spark="3.5 (1 worker, 2 núcleos, 2 GB)",
                hdfs="3.2.1 (3 DataNodes, replicación 2)", anfitrion=platform.platform(terse=True))


# ------------------------------------------------------------------------------------------- main
def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--escalas", nargs="+", type=int, default=[100_000, 1_000_000, 5_000_000])
    ap.add_argument("--repeticiones", type=int, default=3)
    ap.add_argument("--preparar", action="store_true", help="genera los CSV y los carga en los motores (E0)")
    ap.add_argument("--medir", action="store_true")
    ap.add_argument("--informe", action="store_true", help="solo rehace benchmarks.md desde el CSV")
    ap.add_argument("--experimentos", nargs="+", default=["E1", "E2", "E3", "E4", "E5", "E6", "E7"])
    ap.add_argument("--sin-spark", action="store_true")
    ap.add_argument("--max-lookup", type=int, default=1_000_000,
                    help="escala máxima para E3 con $lookup por documento (es lento a propósito)")
    ap.add_argument("--escala-e4", type=int, default=1_000_000)
    a = ap.parse_args()
    if a.informe:
        informe()
        return
    resultados = [entorno()]
    sensores = catalogo()
    if a.preparar:
        print("== Preparación (E0)")
        catalogo_mongo()
        for n in a.escalas:
            ruta = generar(n, sensores)
            print(f" lectura_{n}: {ruta.stat().st_size / 2**20:.1f} MB")
            resultados += cargar_pg(n, ruta) + cargar_mongo(n, ruta)
            if not a.sin_spark:
                resultados += subir_hdfs(n, ruta)
        if not a.sin_spark:
            filas, _ = spark(["--preparar", "--escalas", *map(str, a.escalas)])
            resultados += filas
    if a.medir:
        print("== Medición")
        for n in a.escalas:
            resultados += medir_pg(n, a.repeticiones, a.experimentos)
            resultados += medir_mongo(n, a.repeticiones, a.experimentos, a.max_lookup)
        if "E4" in a.experimentos:
            resultados += medir_e4(min(a.escala_e4, max(a.escalas)), a.repeticiones)
        if "E6" in a.experimentos:
            resultados += medir_e6()
        if not a.sin_spark and {"E1", "E2", "E3", "E5"} & set(a.experimentos):
            exp = [e for e in ("E1", "E2", "E3") if e in a.experimentos] or ["E1"]
            filas, _ = spark(["--medir", "--escalas", *map(str, a.escalas), "--repeticiones", str(a.repeticiones),
                              "--etiqueta", "cores2", "--experimentos", *exp])
            resultados += filas
            if "E5" in a.experimentos:
                filas, _ = spark(["--medir", "--escalas", *map(str, a.escalas), "--repeticiones",
                                  str(a.repeticiones), "--etiqueta", "cores1", "--experimentos", "E1"], cores=1)
                resultados += filas
            if "E2" in a.experimentos:            # latencia real de una consulta puntual: incluye arrancar el job
                _, total = spark(["--medir", "--escalas", str(min(a.escalas)), "--repeticiones", "1",
                                  "--etiqueta", "arranque", "--experimentos", "E2"])
                resultados.append(fila("E2", "Spark", "job_completo_con_arranque", min(a.escalas), total,
                                       nota="spark-submit completo: JVM, registro en el master, ejecutores, consulta"))
    df = combinar(resultados)
    guardar_pg(df[df["experimento"] != "INFO"])
    informe(df)


if __name__ == "__main__":
    main()
