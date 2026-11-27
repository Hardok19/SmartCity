"""conectores_mongo_spark.py — Módulo de la pareja B (MongoDB + Spark) para el dashboard (B5).

La UI no contiene consultas: todo lo que muestra de MongoDB y Spark sale de este módulo.

Secciones:
  1. Conexiones y estado            estado(), db(), lanzar_job_spark(), leer_parquet_hdfs()
  2. Consultas MongoDB              CONSULTAS_MONGO (MG-Q01…Q10, MG-P01…P05) + crud()
  3. Consultas Spark SQL            CONSULTAS_SPARK (SP-Q01…Q10)
  4. Funciones para el dashboard    reciben comun.Filtros y devuelven DataFrames / dicts
                                    con la fuente indicada (contrato.FUENTE_MONGO / FUENTE_SPARK)

Spark (D-04): los jobs corren con spark-submit DENTRO de smartcity_spark_master y escriben Parquet
en HDFS; aquí se leen copiándolos desde el namenode con ``docker exec`` (no requiere tocar el archivo
hosts de Windows). Cuando la pareja A cargue las tablas spark_* en PostgreSQL, basta con cambiar las
funciones spark_* para leer de allí.
"""
from __future__ import annotations

import io
import json
import os
import subprocess
import sys
import tarfile
import tempfile
import time
import urllib.request
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any, Optional

import pandas as pd

from comun import CONFIG, Consulta, Filtros
from contrato import (
    FUENTE_MONGO, FUENTE_SPARK, UNIDADES, ZONAS, normalizar_timestamp, ordenar_columnas_estandar,
    validar_tipo_metrica, validar_zonas,
)

RAIZ = Path(__file__).resolve().parent
if str(RAIZ) not in sys.path:
    sys.path.insert(0, str(RAIZ))
sys.path.append(str(RAIZ / "spark" / "jobs"))
from rutas import RUTAS as RUTAS_HDFS  # noqa: E402  (rutas.py no depende de pyspark)
from scripts.mongo_contrato import a_contrato, pipeline_lecturas  # noqa: E402

DIR_MONGO_QUERIES = RAIZ / "mongo" / "queries"
DIR_SPARK_SQL = RAIZ / "spark" / "sql"
SEVERIDADES = ["baja", "media", "alta", "critica"]
VARIABLES_AIRE = ["pm25", "pm10", "co", "no2", "o3", "co2"]
VARIABLES_CLIMA = ["temperatura", "humedad", "precipitacion", "viento"]
UNIDADES_VARIABLE = {"pm25": "µg/m3", "pm10": "µg/m3", "co": "µg/m3", "no2": "µg/m3", "o3": "µg/m3",
                     "co2": "ppm", "temperatura": "°C", "humedad": "%", "precipitacion": "mm",
                     "viento": "km/h"}

# =====================================================================================
# 1. Conexiones y estado
# =====================================================================================


@lru_cache(maxsize=1)
def cliente_mongo():
    from pymongo import MongoClient
    return MongoClient(CONFIG.mongo_uri, serverSelectionTimeoutMS=3000, tz_aware=True)


def db():
    return cliente_mongo()[CONFIG.mongo_db]


def _estado_mongo() -> tuple[bool, float, str]:
    t0 = time.perf_counter()
    try:
        db().command("ping")
        return True, (time.perf_counter() - t0) * 1000, "ok"
    except Exception as e:  # noqa: BLE001 — el dashboard solo necesita saber si responde
        return False, (time.perf_counter() - t0) * 1000, f"{type(e).__name__}: {str(e)[:160]}"


def _estado_spark() -> tuple[bool, float, str]:
    t0 = time.perf_counter()
    try:
        with urllib.request.urlopen(CONFIG.spark_ui.rstrip("/") + "/json/", timeout=3) as r:
            datos = json.loads(r.read().decode("utf-8"))
        vivos = int(datos.get("aliveworkers", 0))
        ok = datos.get("status") == "ALIVE" and vivos > 0
        return ok, (time.perf_counter() - t0) * 1000, f"master {datos.get('status')}, workers vivos: {vivos}"
    except Exception as e:  # noqa: BLE001
        return False, (time.perf_counter() - t0) * 1000, f"{type(e).__name__}: {str(e)[:160]}"


def estado() -> dict:
    """``{"MongoDB": bool, "Spark": bool, "latencia_ms": {...}, "detalle": {...}}``."""
    m_ok, m_ms, m_det = _estado_mongo()
    s_ok, s_ms, s_det = _estado_spark()
    return {
        FUENTE_MONGO: m_ok, FUENTE_SPARK: s_ok,
        "latencia_ms": {FUENTE_MONGO: round(m_ms, 1), FUENTE_SPARK: round(s_ms, 1)},
        "detalle": {FUENTE_MONGO: m_det, FUENTE_SPARK: s_det},
    }


def _docker(*args: str, env: Optional[dict] = None, timeout: int = 120, binario: bool = False):
    entorno = {**os.environ, **(env or {})}
    return subprocess.run(["docker", *args], capture_output=True, text=not binario, timeout=timeout, env=entorno)


def lanzar_job_spark(job: str, *argumentos: str, timeout: int = 1800) -> tuple[int, str]:
    """Ejecuta ``spark/jobs/<job>.py`` con spark-submit dentro del master (D-04). Devuelve (código, salida)."""
    r = _docker(
        "exec", "--user", "0:0", "-e", "HOME=/tmp", "-e", "HADOOP_USER_NAME=root", "-e", "MONGO_URI",
        CONFIG.contenedor_spark, "/opt/bitnami/spark/bin/spark-submit",
        "--master", "spark://spark-master:7077", "--total-executor-cores", "2",
        "--conf", "spark.jars.ivy=/tmp/.ivy2",
        "--packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.4.1,org.postgresql:postgresql:42.7.4",
        "--py-files", "/opt/smartcity/spark/jobs/common.py,/opt/smartcity/spark/jobs/rutas.py",
        f"/opt/smartcity/spark/jobs/{job}.py", *argumentos,
        env={"MONGO_URI": CONFIG.mongo_uri_spark}, timeout=timeout,
    )
    salida = (r.stdout or "") + ("\n" + r.stderr if r.returncode else "")
    lineas = [ln for ln in salida.splitlines() if ln.startswith(("SMARTCITY_", "B43_", "=== ")) or "Error" in ln]
    return r.returncode, "\n".join(lineas[-200:])


def leer_parquet_hdfs(ruta: str) -> pd.DataFrame:
    """Lee un directorio Parquet de HDFS (ruta absoluta /smartcity/...) copiándolo desde el namenode.

    Usa ``hdfs dfs -get`` + tar por stdout, así no se necesita WebHDFS ni editar el archivo hosts.
    """
    comando = (f"set -e; d=$(mktemp -d); hdfs dfs -get '{ruta}' \"$d/out\" 1>&2; "
               f"tar -C \"$d/out\" -cf - .; rm -rf \"$d\"")
    r = _docker("exec", CONFIG.contenedor_namenode, "bash", "-c", comando, binario=True, timeout=300)
    if r.returncode != 0:
        raise FileNotFoundError(f"No se pudo leer {ruta} de HDFS: {r.stderr.decode('utf-8', 'replace')[-400:]}")
    with tempfile.TemporaryDirectory(prefix="smartcity_hdfs_") as tmp:
        with tarfile.open(fileobj=io.BytesIO(r.stdout)) as tar:
            tar.extractall(tmp, filter="data")
        df = pd.read_parquet(tmp)
    for col in ("anio", "mes"):  # columnas de partición Hive llegan como categorías
        if col in df.columns and isinstance(df[col].dtype, pd.CategoricalDtype):
            df[col] = df[col].astype(int)
    return df


def ruta_analitica(nombre: str) -> str:
    return f"{RUTAS_HDFS['analitica']}/{nombre}"


# =====================================================================================
# 2. Consultas MongoDB
# =====================================================================================


def _fecha(texto: Any, fin: bool = False) -> Optional[datetime]:
    """'2024-02-15' -> datetime UTC (00:00 o 23:59:59.999 si ``fin``)."""
    if texto in (None, ""):
        return None
    if isinstance(texto, datetime):
        return texto if texto.tzinfo else texto.replace(tzinfo=timezone.utc)
    d = pd.Timestamp(str(texto)).to_pydatetime().replace(tzinfo=timezone.utc)
    if fin and len(str(texto)) <= 10:
        d = d + timedelta(days=1) - timedelta(milliseconds=1)
    return d


def _rango(campo: str, desde: Any, hasta: Any) -> dict:
    cond = {}
    if desde:
        cond["$gte"] = _fecha(desde)
    if hasta:
        cond["$lte"] = _fecha(hasta, fin=True)
    return {campo: cond} if cond else {}


def _lista(valor: Any) -> list:
    if valor in (None, "", "(todas)", "(todos)"):
        return []
    if isinstance(valor, str):
        return [v.strip() for v in valor.split(",") if v.strip()]
    return list(valor)


def _df(docs) -> pd.DataFrame:
    docs = list(docs)
    if not docs:
        return pd.DataFrame()
    df = pd.json_normalize(docs, sep=".")
    for c in df.columns:  # listas de strings -> texto (para mostrar en tablas)
        if df[c].map(lambda v: isinstance(v, list)).any():
            df[c] = df[c].map(lambda v: ", ".join(map(str, v)) if isinstance(v, list) else v)
    return df


def _agg(coleccion: str, pipeline: list) -> pd.DataFrame:
    return _df(db()[coleccion].aggregate(pipeline, allowDiskUse=True))


def mg_q01(sensor="S-011", limite=50):
    return _df(db().lectura.find({"sensor_id": sensor},
                                 {"_id": 0, "timestamp": 1, "tipo": 1, "ubicacion.zona": 1, "mediciones": 1,
                                  "dispositivo.firmware": 1}).sort("timestamp", -1).limit(int(limite)))


def mg_q02(desde="2024-04-01", hasta="2024-04-01", zona="Zona Norte", tipo="(todas)", limite=500):
    filtro = {**_rango("timestamp", desde, hasta)}
    if zona and zona != "(todas)":
        filtro["ubicacion.zona"] = zona
    if _lista(tipo):
        filtro["tipo"] = {"$in": _lista(tipo)}
    return _df(db().lectura.find(filtro, {"_id": 0, "sensor_id": 1, "tipo": 1, "timestamp": 1,
                                          "ubicacion.zona": 1, "mediciones": 1})
               .sort("timestamp", 1).limit(int(limite)))


def mg_q03(desde="2024-04-01", hasta="2024-04-30"):
    return _agg("lectura", [
        {"$match": _rango("timestamp", desde, hasta)},
        {"$project": {"tipo": 1, "kv": {"$objectToArray": "$mediciones"}}},
        {"$unwind": "$kv"},
        {"$match": {"kv.v": {"$type": "number"}}},
        {"$group": {"_id": {"tipo": "$tipo", "variable": "$kv.k"}, "minimo": {"$min": "$kv.v"},
                    "maximo": {"$max": "$kv.v"}, "promedio": {"$avg": "$kv.v"}, "lecturas": {"$sum": 1}}},
        {"$project": {"_id": 0, "tipo": "$_id.tipo", "variable": "$_id.variable", "minimo": 1, "maximo": 1,
                      "promedio": {"$round": ["$promedio", 2]}, "lecturas": 1}},
        {"$sort": {"tipo": 1, "variable": 1}},
    ])


def mg_q04(severidad_min="alta", tipo_evento="(todas)", zona="(todas)", limite=200):
    sev = SEVERIDADES[SEVERIDADES.index(severidad_min):] if severidad_min in SEVERIDADES else SEVERIDADES
    filtro = {"severidad": {"$in": sev}}
    if _lista(tipo_evento):
        filtro["tipo_evento"] = {"$in": _lista(tipo_evento)}
    if zona and zona != "(todas)":
        filtro["zona.nombre"] = zona
    return _df(db().evento.find(filtro, {"_id": 0, "tipo_evento": 1, "severidad": 1, "inicio": 1, "fin": 1,
                                         "zona.nombre": 1, "sensores": 1, "detalles": 1})
               .sort("inicio", -1).limit(int(limite)))


def mg_q05(desde="2024-02-15", hasta="2024-06-15", radio_km=0.0, lat=51.5074, lon=-0.1278):
    rango = _rango("emitida", desde, hasta)
    grupo = {"_id": "$zona.nombre", "alertas": {"$sum": 1},
             "abiertas": {"$sum": {"$cond": [{"$eq": ["$estado", "abierta"]}, 1, 0]}},
             "criticas_o_altas": {"$sum": {"$cond": [{"$in": ["$nivel", ["alta", "critica"]]}, 1, 0]}},
             "variables": {"$addToSet": "$variable"}, "ultima": {"$max": "$emitida"}}
    proyeccion = {"_id": 0, "zona": "$_id", "alertas": 1, "abiertas": 1, "criticas_o_altas": 1,
                  "variables": 1, "ultima": 1}
    if float(radio_km) > 0:   # $geoNear: solo zonas dentro del radio desde (lat, lon)
        inicio = [{"$geoNear": {"near": {"type": "Point", "coordinates": [float(lon), float(lat)]},
                                "distanceField": "distancia_m", "maxDistance": float(radio_km) * 1000,
                                "query": rango, "key": "zona.geo", "spherical": True}}]
        grupo["distancia_km"] = {"$min": {"$divide": ["$distancia_m", 1000]}}
        proyeccion["distancia_km"] = {"$round": ["$distancia_km", 2]}
    else:
        inicio = [{"$match": rango}]
    return _agg("alerta", inicio + [{"$group": grupo}, {"$project": proyeccion}, {"$sort": {"alertas": -1}}])


def mg_q06(tipo="calidad_aire", desde="2024-02-15", hasta="2024-06-15"):
    variables = VARIABLES_AIRE if tipo == "calidad_aire" else VARIABLES_CLIMA
    return _agg("lectura", [
        {"$match": {"tipo": tipo, **_rango("timestamp", desde, hasta)}},
        {"$group": {"_id": "$ubicacion.zona", "lecturas": {"$sum": 1},
                    **{v: {"$avg": f"$mediciones.{v}"} for v in variables}}},
        {"$project": {"_id": 0, "zona": "$_id", "lecturas": 1, **{v: {"$round": [f"${v}", 2]} for v in variables}}},
        {"$sort": {variables[0]: -1}},
    ])


def mg_q07(desde="2024-05-01", hasta="2024-05-31"):
    return _agg("lectura", [
        {"$match": _rango("timestamp", desde, hasta)},
        {"$project": {"tipo": 1, "sensor_id": 1, "kv": {"$objectToArray": "$mediciones"}}},
        {"$unwind": "$kv"},
        {"$match": {"kv.v": {"$type": "number"}}},
        {"$group": {"_id": {"tipo": "$tipo", "variable": "$kv.k"}, "promedio": {"$avg": "$kv.v"},
                    "desviacion": {"$stdDevPop": "$kv.v"}, "sensores": {"$addToSet": "$sensor_id"}}},
        {"$project": {"_id": 0, "tipo": "$_id.tipo", "variable": "$_id.variable",
                      "promedio": {"$round": ["$promedio", 2]}, "desviacion": {"$round": ["$desviacion", 2]},
                      "sensores": {"$size": "$sensores"}}},
        {"$sort": {"tipo": 1, "variable": 1}},
    ])


def mg_q08(desde="2024-02-15", hasta="2024-06-15", texto=""):
    filtro = _rango("reportado", desde, hasta)
    if texto:
        filtro["$text"] = {"$search": texto, "$language": "spanish"}
    return _agg("incidente", [
        {"$match": filtro},
        {"$group": {"_id": "$categoria", "incidentes": {"$sum": 1},
                    "resueltos": {"$sum": {"$cond": [{"$eq": ["$estado_actual", "resuelto"]}, 1, 0]}},
                    "reportes_ciudadanos": {"$sum": {"$size": {"$ifNull": ["$reportes_ciudadanos", []]}}},
                    "heridos": {"$sum": {"$ifNull": ["$heridos", 0]}},
                    "con_adjuntos": {"$sum": {"$cond": [{"$isArray": "$adjuntos"}, 1, 0]}}}},
        {"$project": {"_id": 0, "categoria": "$_id", "incidentes": 1, "resueltos": 1, "reportes_ciudadanos": 1,
                      "heridos": 1, "con_adjuntos": 1,
                      "pct_resuelto": {"$round": [{"$multiply": [{"$divide": ["$resueltos", "$incidentes"]}, 100]}, 1]}}},
        {"$sort": {"incidentes": -1}},
    ])


def mg_q09(variable="pm25", zona="(todas)", desde="2024-02-15", hasta="2024-06-15"):
    tipo = "clima" if variable in VARIABLES_CLIMA else "calidad_aire"
    match = {"tipo": tipo, **_rango("timestamp", desde, hasta)}
    if zona and zona != "(todas)":
        match["ubicacion.zona"] = zona
    campo = f"$mediciones.{variable}"
    return _agg("lectura", [
        {"$match": match},
        {"$facet": {
            "serie_diaria": [
                {"$group": {"_id": {"$dateTrunc": {"date": "$timestamp", "unit": "day", "timezone": "UTC"}},
                            "valor": {"$avg": campo}}},
                {"$setWindowFields": {"sortBy": {"_id": 1},
                                      "output": {"media_movil_7d": {"$avg": "$valor", "window": {"documents": [-6, 0]}}}}},
                {"$project": {"_id": 0, "dia": "$_id", "valor": {"$round": ["$valor", 2]},
                              "media_movil_7d": {"$round": ["$media_movil_7d", 2]}}},
            ],
            "histograma": [
                {"$bucket": {"groupBy": campo, "boundaries": [-1000, 0, 12, 35.4, 55.4, 150.4, 100000],
                             "default": "sin_dato", "output": {"lecturas": {"$sum": 1}}}},
                {"$project": {"_id": 0, "desde": "$_id", "lecturas": 1}},
            ],
            "top_sensores": [
                {"$group": {"_id": "$sensor_id", "valor": {"$avg": campo}, "zona": {"$first": "$ubicacion.zona"}}},
                {"$sort": {"valor": -1}}, {"$limit": 5},
                {"$lookup": {"from": "alerta", "localField": "_id", "foreignField": "sensor_id", "as": "alertas"}},
                {"$project": {"_id": 0, "sensor_id": "$_id", "zona": 1, "valor": {"$round": ["$valor", 2]},
                              "alertas": {"$size": "$alertas"}}},
            ],
            "resumen": [
                {"$group": {"_id": None, "lecturas": {"$sum": 1}, "promedio": {"$avg": campo},
                            "maximo": {"$max": campo}, "sensores": {"$addToSet": "$sensor_id"}}},
                {"$project": {"_id": 0, "lecturas": 1, "promedio": {"$round": ["$promedio", 2]}, "maximo": 1,
                              "sensores": {"$size": "$sensores"}}},
            ],
        }},
        {"$project": {"filas": {"$concatArrays": [
            {"$map": {"input": f"${f}", "in": {"$mergeObjects": ["$$this", {"faceta": f}]}}}
            for f in ("resumen", "serie_diaria", "histograma", "top_sensores")]}}},
        {"$unwind": "$filas"},
        {"$replaceRoot": {"newRoot": "$filas"}},
    ])


def mg_q10(tipos_zona="comercial,industrial,mixta", desde="2024-03-01", hasta="2024-06-15",
           pm25_min=35.4, bateria_max=90, limite=200):
    filtro = {"$and": [
        {"tipo": "calidad_aire"},
        {"ubicacion.tipo_zona": {"$in": _lista(tipos_zona)}},
        _rango("timestamp", desde, hasta),
        {"mediciones.co2": {"$exists": True}},
        {"$or": [{"mediciones.pm25": {"$gte": float(pm25_min)}}, {"calidad_dato.valido": False}]},
        {"dispositivo.bateria_pct": {"$lt": float(bateria_max)}},
    ]}
    return _df(db().lectura.find(filtro, {"_id": 0, "sensor_id": 1, "timestamp": 1, "ubicacion.zona": 1,
                                          "ubicacion.tipo_zona": 1, "mediciones.pm25": 1, "mediciones.co2": 1,
                                          "dispositivo": 1, "calidad_dato": 1})
               .sort("mediciones.pm25", -1).limit(int(limite)))


def mg_p01(desde="2024-02-15", hasta="2024-06-15"):
    return _agg("lectura", [
        {"$match": {"tipo": "clima", **_rango("timestamp", desde, hasta)}},
        {"$group": {"_id": "$ubicacion.zona", "temperaturaPromedio": {"$avg": "$mediciones.temperatura"},
                    "minima": {"$min": "$mediciones.temperatura"}, "maxima": {"$max": "$mediciones.temperatura"}}},
        {"$project": {"_id": 0, "zona": "$_id", "temperaturaPromedio": {"$round": ["$temperaturaPromedio", 2]},
                      "minima": 1, "maxima": 1}},
        {"$sort": {"temperaturaPromedio": -1}},
    ])


def mg_p02(variable="pm25", umbral=35.4, desde="2024-02-15", hasta="2024-06-15"):
    tipo = "clima" if variable in VARIABLES_CLIMA else "calidad_aire"
    campo = f"mediciones.{variable}"
    return _agg("lectura", [
        {"$match": {"tipo": tipo, campo: {"$gt": float(umbral)}, **_rango("timestamp", desde, hasta)}},
        {"$group": {"_id": "$sensor_id", "zona": {"$first": "$ubicacion.zona"}, "lecturas_sobre_limite": {"$sum": 1},
                    "valor_maximo": {"$max": f"${campo}"}, "primera": {"$min": "$timestamp"},
                    "ultima": {"$max": "$timestamp"}}},
        {"$project": {"_id": 0, "sensor_id": "$_id", "zona": 1, "lecturas_sobre_limite": 1, "valor_maximo": 1,
                      "primera": 1, "ultima": 1, "umbral": {"$literal": float(umbral)}}},
        {"$sort": {"lecturas_sobre_limite": -1}},
    ])


def mg_p03(desde="2024-02-15", hasta="2024-06-15"):
    return _agg("lectura", [
        {"$match": _rango("timestamp", desde, hasta)},
        {"$group": {"_id": {"sensor": "$sensor_id", "tipo": "$tipo"}, "mediciones": {"$sum": 1},
                    "primera": {"$min": "$timestamp"}, "ultima": {"$max": "$timestamp"}}},
        {"$project": {"_id": 0, "sensor_id": "$_id.sensor", "tipo": "$_id.tipo", "mediciones": 1, "primera": 1,
                      "ultima": 1}},
        {"$sort": {"mediciones": -1}},
    ])


def mg_p04(desde="2024-02-15", hasta="2024-06-15"):
    return _agg("lectura", [
        {"$match": {"tipo": "calidad_aire", **_rango("timestamp", desde, hasta)}},
        {"$group": {"_id": "$ubicacion.zona", "pm25": {"$avg": "$mediciones.pm25"}, "pm10": {"$avg": "$mediciones.pm10"},
                    "no2": {"$avg": "$mediciones.no2"},
                    "sobre_umbral": {"$sum": {"$cond": [{"$gt": ["$mediciones.pm25", 35.4]}, 1, 0]}},
                    "lecturas": {"$sum": 1}}},
        {"$project": {"_id": 0, "zona": "$_id", "pm25": {"$round": ["$pm25", 2]}, "pm10": {"$round": ["$pm10", 2]},
                      "no2": {"$round": ["$no2", 2]},
                      "pct_sobre_umbral": {"$round": [{"$multiply": [{"$divide": ["$sobre_umbral", "$lecturas"]}, 100]}, 2]}}},
        {"$sort": {"pm25": -1}},
    ])


def mg_p05(variable="pm25", zona="Zona Norte", unidad="hour", desde="2024-04-01", hasta="2024-04-07"):
    tipo = "clima" if variable in VARIABLES_CLIMA else "calidad_aire"
    campo = f"$mediciones.{variable}"
    return _agg("lectura", [
        {"$match": {"tipo": tipo, "ubicacion.zona": zona, **_rango("timestamp", desde, hasta)}},
        {"$group": {"_id": {"$dateTrunc": {"date": "$timestamp", "unit": unidad, "timezone": "UTC"}},
                    "promedio": {"$avg": campo}, "maximo": {"$max": campo}, "lecturas": {"$sum": 1}}},
        {"$project": {"_id": 0, "periodo": "$_id", "promedio": {"$round": ["$promedio", 2]}, "maximo": 1,
                      "lecturas": 1}},
        {"$sort": {"periodo": 1}},
    ])


def _js(ident: str) -> str:
    ruta = DIR_MONGO_QUERIES / f"{ident}.js"
    return ruta.read_text(encoding="utf-8") if ruta.exists() else f"// {ident}: archivo no encontrado"


_DEF_MONGO = [
    ("MG-Q01", "Lecturas de un sensor", "find + proyección + sort por timestamp (usa ix_sensor_ts).",
     {"sensor": "S-011", "limite": 50}, mg_q01),
    ("MG-Q02", "Lecturas en un rango de fechas", "find con $gte/$lte sobre timestamp y zona (ix_tipo_zona_ts).",
     {"desde": "2024-04-01", "hasta": "2024-04-01", "zona": "Zona Norte", "tipo": "(todas)", "limite": 500}, mg_q02),
    ("MG-Q03", "Valores máximos y mínimos", "$objectToArray + $unwind + $group $min/$max por tipo y variable.",
     {"desde": "2024-04-01", "hasta": "2024-04-30"}, mg_q03),
    ("MG-Q04", "Eventos críticos", "find con $in de severidad, orden por inicio (ix_tipo_sev_inicio).",
     {"severidad_min": "alta", "tipo_evento": "(todas)", "zona": "(todas)", "limite": 200}, mg_q04),
    ("MG-Q05", "Alertas por zona geográfica", "$group por zona; con radio_km > 0 usa $geoNear (2dsphere).",
     {"desde": "2024-02-15", "hasta": "2024-06-15", "radio_km": 0.0, "lat": 51.5074, "lon": -0.1278}, mg_q05),
    ("MG-Q06", "Promedio por zona", "$group $avg de cada variable por zona.",
     {"tipo": "calidad_aire", "desde": "2024-02-15", "hasta": "2024-06-15"}, mg_q06),
    ("MG-Q07", "Promedio por tipo de sensor", "$objectToArray + $group por tipo y variable ($avg, $stdDevPop).",
     {"desde": "2024-05-01", "hasta": "2024-05-31"}, mg_q07),
    ("MG-Q08", "Incidentes por categoría", "$group por categoría; texto opcional con $text (índice de texto).",
     {"desde": "2024-02-15", "hasta": "2024-06-15", "texto": ""}, mg_q08),
    ("MG-Q09", "Agregación avanzada", "$facet + $bucket + $dateTrunc + $setWindowFields (media móvil 7 días) + $lookup a alerta.",
     {"variable": "pm25", "zona": "(todas)", "desde": "2024-02-15", "hasta": "2024-06-15"}, mg_q09),
    ("MG-Q10", "Filtros múltiples", "$and/$or/$in/$exists sobre campos de firmware 2.x (co2, batería).",
     {"tipos_zona": "comercial,industrial,mixta", "desde": "2024-03-01", "hasta": "2024-06-15",
      "pm25_min": 35.4, "bateria_max": 90, "limite": 200}, mg_q10),
    ("MG-P01", "Temperatura promedio por zona", "Pregunta del enunciado: $group $avg/$min/$max de temperatura.",
     {"desde": "2024-02-15", "hasta": "2024-06-15"}, mg_p01),
    ("MG-P02", "Sensores sobre el límite", "Pregunta del enunciado: lecturas sobre el umbral por sensor.",
     {"variable": "pm25", "umbral": 35.4, "desde": "2024-02-15", "hasta": "2024-06-15"}, mg_p02),
    ("MG-P03", "Mediciones por sensor", "Pregunta del enunciado: conteo de lecturas por sensor y tipo.",
     {"desde": "2024-02-15", "hasta": "2024-06-15"}, mg_p03),
    ("MG-P04", "Zonas más contaminadas", "Pregunta del enunciado: promedios y % sobre umbral por zona.",
     {"desde": "2024-02-15", "hasta": "2024-06-15"}, mg_p04),
    ("MG-P05", "Evolución de una variable", "Pregunta del enunciado: $dateTrunc por hora/día.",
     {"variable": "pm25", "zona": "Zona Norte", "unidad": "hour", "desde": "2024-04-01", "hasta": "2024-04-07"}, mg_p05),
]

CONSULTAS_MONGO: dict[str, Consulta] = {
    ident: Consulta(id=ident, tecnologia=FUENTE_MONGO, titulo=titulo, descripcion=desc, codigo=_js(ident),
                    lenguaje="javascript", parametros=params, funcion=fn)
    for ident, titulo, desc, params, fn in _DEF_MONGO
}


# ---------------------------------------------------------------------- CRUD (B2.3)
def _ubic(id_zona, zona, lon, lat):
    return {"id_zona": id_zona, "zona": zona, "geo": {"type": "Point", "coordinates": [lon, lat]}}


OPERACIONES_CRUD = ["reiniciar", "insertOne", "insertMany", "find", "findOne", "updateOne", "updateMany", "deleteOne"]


def crud(operacion: str) -> tuple[Any, str]:
    """Ejecuta una operación CRUD de demostración sobre ``demo_crud`` (nunca sobre ``lectura``).

    Devuelve (resultado, comando mongosh equivalente) — el comando es el de mongo/03_crud_demo.js.
    """
    c = db().demo_crud
    if operacion == "reiniciar":
        r = c.delete_many({})
        return {"deletedCount": r.deleted_count}, "db.demo_crud.deleteMany({})"
    if operacion == "insertOne":
        r = c.insert_one({"sensor_id": "S-DEMO-001", "tipo": "calidad_aire",
                          "timestamp": datetime(2024, 6, 1, 8, tzinfo=timezone.utc),
                          "ubicacion": _ubic(1, "Zona Norte", -0.1058, 51.5465),
                          "mediciones": {"pm25": 18.6, "pm10": 25.1, "co": 0.85, "no2": 12.3}})
        return {"insertedId": str(r.inserted_id)}, (
            'db.demo_crud.insertOne({ sensor_id: "S-DEMO-001", tipo: "calidad_aire", timestamp: ISODate("2024-06-01T08:00:00Z"),\n'
            '  ubicacion: ubic(1, "Zona Norte", -0.1058, 51.5465), mediciones: { pm25: 18.6, pm10: 25.1, co: 0.85, no2: 12.3 } })')
    if operacion == "insertMany":
        t = lambda h: datetime(2024, 6, 1, h, tzinfo=timezone.utc)  # noqa: E731
        r = c.insert_many([
            {"sensor_id": "S-DEMO-002", "tipo": "clima", "timestamp": t(9), "ubicacion": _ubic(5, "Zona Oeste", -0.3037, 51.4613),
             "mediciones": {"temperatura": 27.2, "humedad": 70}},
            {"sensor_id": "S-DEMO-003", "tipo": "ruido", "timestamp": t(9), "ubicacion": _ubic(3, "Zona Centro", -0.1357, 51.4975),
             "mediciones": {"db_promedio": 142.0, "db_max": 150.0}},
            {"sensor_id": "S-DEMO-004", "tipo": "calidad_aire", "timestamp": t(10), "ubicacion": _ubic(2, "Zona Sur", -0.1949, 51.4098),
             "mediciones": {"pm25": -4.0, "co2": 520}},
            {"sensor_id": "S-DEMO-005", "tipo": "flujo_peatonal", "timestamp": t(10), "ubicacion": _ubic(4, "Zona Este", 0.0648, 51.4892),
             "mediciones": {"entradas": 85, "salidas": 60, "direccion_predominante": "entrada"}},
            {"tipo_documento": "alerta", "regla": "pm25_nivel_1", "variable": "pm25", "valor": 41.8, "umbral": 35.4,
             "sensor_id": "S-DEMO-001", "estado": "abierta", "emitida": t(11), "acciones": []},
        ])
        return {"insertedIds": [str(i) for i in r.inserted_ids]}, (
            "db.demo_crud.insertMany([ {S-DEMO-002 clima}, {S-DEMO-003 ruido}, {S-DEMO-004 calidad_aire},\n"
            "  {S-DEMO-005 flujo_peatonal}, {tipo_documento: \"alerta\", ...} ])   // esquema distinto por documento")
    if operacion == "find":
        docs = list(c.find({"tipo": {"$in": ["calidad_aire", "clima"]}}, {"_id": 0, "sensor_id": 1, "tipo": 1, "mediciones": 1})
                    .sort("timestamp", -1).limit(5))
        return _df(docs), ('db.demo_crud.find({ tipo: { $in: ["calidad_aire", "clima"] } }, '
                           '{ _id: 0, sensor_id: 1, tipo: 1, mediciones: 1 }).sort({ timestamp: -1 }).limit(5)')
    if operacion == "findOne":
        doc = c.find_one({"sensor_id": "S-DEMO-001", "tipo": "calidad_aire"}, {"_id": 0})
        return doc or {}, 'db.demo_crud.findOne({ sensor_id: "S-DEMO-001", tipo: "calidad_aire" }, { _id: 0 })'
    if operacion == "updateOne":
        r = c.update_one({"tipo_documento": "alerta", "estado": "abierta"},
                         {"$set": {"estado": "atendida"},
                          "$push": {"acciones": {"fecha": datetime.now(timezone.utc), "usuario": "operador@smartcity.example",
                                                 "accion": "Inspección"}}})
        return {"matchedCount": r.matched_count, "modifiedCount": r.modified_count}, (
            'db.demo_crud.updateOne({ tipo_documento: "alerta", estado: "abierta" },\n'
            '  { $set: { estado: "atendida" }, $push: { acciones: { fecha: new Date(), usuario: "operador@smartcity.example", accion: "Inspección" } } })')
    if operacion == "updateMany":
        r = c.update_many({"$or": [{"mediciones.pm25": {"$lt": 0}}, {"mediciones.db_promedio": {"$gt": 140}}]},
                          {"$set": {"calidad_dato.valido": False}, "$addToSet": {"calidad_dato.flags": "fuera_de_rango"}})
        return {"matchedCount": r.matched_count, "modifiedCount": r.modified_count}, (
            'db.demo_crud.updateMany({ $or: [{ "mediciones.pm25": { $lt: 0 } }, { "mediciones.db_promedio": { $gt: 140 } }] },\n'
            '  { $set: { "calidad_dato.valido": false }, $addToSet: { "calidad_dato.flags": "fuera_de_rango" } })')
    if operacion == "deleteOne":
        r = c.delete_one({"sensor_id": "S-DEMO-005"})
        return {"deletedCount": r.deleted_count}, 'db.demo_crud.deleteOne({ sensor_id: "S-DEMO-005" })'
    raise ValueError(f"Operación desconocida: {operacion}. Válidas: {OPERACIONES_CRUD}")


def documentos_demo_crud() -> pd.DataFrame:
    return _df(db().demo_crud.find({}, {"_id": 0}))


# =====================================================================================
# 3. Consultas Spark SQL (B4.1)
# =====================================================================================
_DEF_SPARK = [
    ("SP-Q01", "Sensores que generan más información", "GROUP BY + ORDER BY sobre el Parquet curado."),
    ("SP-Q02", "Distribución geográfica de eventos", "JOIN eventos (MongoDB) × zonas."),
    ("SP-Q03", "Evolución temporal de indicadores", "date_trunc por mes/semana."),
    ("SP-Q04", "Correlación entre variables", "corr() de PM2.5 con humedad, precipitación, viento y temperatura."),
    ("SP-Q05", "Ranking de zonas con mayor actividad", "RANK() OVER."),
    ("SP-Q06", "Tendencias por período", "LAG() y variación porcentual."),
    ("SP-Q07", "Detección de anomalías", "z-score con AVG/STDDEV OVER (PARTITION BY sensor) + marcas de MongoDB."),
    ("SP-Q08", "Agrupamiento por categorías", "ROLLUP/CUBE categoría-tipo-zona."),
    ("SP-Q09", "Comparación histórica entre años", "PIVOT por año (2023 vs 2024)."),
    ("SP-Q10", "Reporte consolidado", "KPIs por zona (fuente para spark_kpi_zona)."),
]


def spark_consulta(ident: str, recalcular: bool = False) -> pd.DataFrame:
    """Resultado de una SP-Qxx. Con ``recalcular`` relanza el job (tarda) y luego lee el resultado de HDFS."""
    if recalcular:
        codigo, salida = lanzar_job_spark("ejecutar_consultas", "--consultas", ident, "--exportar")
        if codigo != 0:
            raise RuntimeError(f"Spark terminó con código {codigo}:\n{salida[-1500:]}")
    return leer_parquet_hdfs(f"{RUTAS_HDFS['consultas']}/{ident}")


def _sql(ident: str) -> str:
    ruta = DIR_SPARK_SQL / f"{ident}.sql"
    return ruta.read_text(encoding="utf-8") if ruta.exists() else f"-- {ident}: archivo no encontrado"


def _fn_spark(ident):
    return lambda recalcular=False: spark_consulta(ident, bool(recalcular))


CONSULTAS_SPARK: dict[str, Consulta] = {
    ident: Consulta(id=ident, tecnologia=FUENTE_SPARK, titulo=titulo, descripcion=desc, codigo=_sql(ident),
                    lenguaje="sql", parametros={"recalcular": False}, funcion=_fn_spark(ident))
    for ident, titulo, desc in _DEF_SPARK
}


def ejecutar(ident: str, **params) -> pd.DataFrame:
    """Ejecuta cualquier consulta registrada (MG-* o SP-*) con sus parámetros."""
    consulta = CONSULTAS_MONGO.get(ident) or CONSULTAS_SPARK.get(ident)
    if consulta is None:
        raise KeyError(f"Consulta desconocida: {ident}")
    return consulta.ejecutar(**params)


# =====================================================================================
# 4. Funciones para el dashboard (todas reciben Filtros)
# =====================================================================================


def _match_lectura(f: Filtros, tipo: Optional[str] = None) -> dict:
    m: dict = {}
    if tipo:
        m["tipo"] = tipo
    m.update(_rango("timestamp", f.fecha_inicio, f.fecha_fin))
    if f.zonas:
        m["ubicacion.zona"] = {"$in": list(f.zonas)}
    if f.id_sensor:
        m["sensor_id"] = f.id_sensor
    return m


def _etapa_perfil(f: Filtros, campo: str = "$timestamp") -> list:
    if f.perfil_horario == "todo_el_dia":
        return []
    return [{"$match": {"$expr": {"$in": [{"$hour": {"date": campo, "timezone": "UTC"}}, list(f.horas)]}}}]


def _etapa_valor(f: Filtros, variable: str) -> list:
    cond = {}
    if f.valor_min is not None:
        cond["$gte"] = float(f.valor_min)
    if f.valor_max is not None:
        cond["$lte"] = float(f.valor_max)
    return [{"$match": {f"mediciones.{variable}": cond}}] if cond else []


def _match_zona_fecha(f: Filtros, campo_fecha: str, campo_zona: str = "zona.nombre") -> dict:
    m = _rango(campo_fecha, f.fecha_inicio, f.fecha_fin)
    if f.zonas:
        m[campo_zona] = {"$in": list(f.zonas)}
    return m


def lecturas_mongo(f: Filtros, limite: int = 20_000) -> pd.DataFrame:
    """Lecturas en COLUMNAS_ESTANDAR (calidad_aire -> valor = PM2.5), ``fuente="MongoDB"``."""
    tipos = [t for t in (f.tipos_metrica or ("calidad_aire",)) if t == "calidad_aire"]
    if not tipos:
        return pd.DataFrame(columns=["zona", "timestamp", "id_sensor", "tipo_metrica", "valor", "fuente"])
    base = pipeline_lecturas(fecha_inicio=f.desde_utc, fecha_fin=f.hasta_utc, zonas=list(f.zonas) or None,
                             id_sensor=f.id_sensor, tipos=tipos)
    pipeline = [base[0], *_etapa_perfil(f), *_etapa_valor(f, "pm25"), {"$sort": {"timestamp": -1}},
                {"$limit": int(limite)}, base[1]]
    return a_contrato(db().lectura.aggregate(pipeline, allowDiskUse=True))


def kpis_ambiente(f: Filtros) -> dict:
    """Temperatura, PM2.5, CO y humedad promedio + nº de alertas ambientales (fuente MongoDB)."""
    pipeline = [{"$match": _match_lectura(f)}, *_etapa_perfil(f),
                {"$group": {"_id": None,
                            "temperatura": {"$avg": "$mediciones.temperatura"},
                            "humedad": {"$avg": "$mediciones.humedad"},
                            "pm25": {"$avg": "$mediciones.pm25"},
                            "pm10": {"$avg": "$mediciones.pm10"},
                            "co": {"$avg": "$mediciones.co"},
                            "no2": {"$avg": "$mediciones.no2"},
                            "co2": {"$avg": "$mediciones.co2"}}}]
    r = next(db().lectura.aggregate(pipeline, allowDiskUse=True), {}) or {}
    n_alertas = db().alerta.count_documents(_match_zona_fecha(f, "emitida"))
    return {
        "temperatura_promedio": r.get("temperatura"), "humedad_promedio": r.get("humedad"),
        "pm25_promedio": r.get("pm25"), "pm10_promedio": r.get("pm10"),
        "co_promedio": r.get("co"), "no2_promedio": r.get("no2"),
        "co2_promedio": r.get("co2"),  # solo sensores con firmware 2.x reportan CO₂ (ppm)
        "alertas_ambientales": n_alertas, "fuente": FUENTE_MONGO,
    }


def kpis_iot_mongo(f: Filtros) -> dict:
    """Mediciones recibidas, alertas generadas y ritmo de datos (por día, hora, minuto y segundo)."""
    pipeline = [{"$match": _match_lectura(f)}, *_etapa_perfil(f),
                {"$group": {"_id": None, "n": {"$sum": 1}, "desde": {"$min": "$timestamp"},
                            "hasta": {"$max": "$timestamp"}, "sensores": {"$addToSet": "$sensor_id"},
                            "invalidas": {"$sum": {"$cond": [{"$eq": ["$calidad_dato.valido", False]}, 1, 0]}}}}]
    r = next(db().lectura.aggregate(pipeline, allowDiskUse=True), None) or {}
    n = int(r.get("n", 0))
    segundos = max((r["hasta"] - r["desde"]).total_seconds(), 1.0) if r.get("desde") else 1.0
    return {
        "mediciones_recibidas": n,
        "sensores_reportando": len(r.get("sensores", [])),
        "lecturas_invalidas": int(r.get("invalidas", 0)),
        "alertas_generadas": db().alerta.count_documents(_match_zona_fecha(f, "emitida")),
        "datos_por_dia": n / segundos * 86400, "datos_por_hora": n / segundos * 3600,
        "datos_por_minuto": n / segundos * 60, "lecturas_por_segundo": n / segundos,
        "periodo_desde": r.get("desde"), "periodo_hasta": r.get("hasta"), "fuente": FUENTE_MONGO,
    }


def serie_temporal(f: Filtros, variable: str = "pm25", unidad: str = "day") -> pd.DataFrame:
    """Promedio de ``variable`` por zona y período (``unidad``: hour/day/week/month). Fuente MongoDB."""
    tipo = "clima" if variable in VARIABLES_CLIMA else "calidad_aire"
    df = _agg("lectura", [
        {"$match": {**_match_lectura(f, tipo), f"mediciones.{variable}": {"$type": "number"}}},
        *_etapa_perfil(f), *_etapa_valor(f, variable),
        {"$group": {"_id": {"zona": "$ubicacion.zona",
                            "periodo": {"$dateTrunc": {"date": "$timestamp", "unit": unidad, "timezone": "UTC"}}},
                    "valor": {"$avg": f"$mediciones.{variable}"}, "lecturas": {"$sum": 1}}},
        {"$project": {"_id": 0, "zona": "$_id.zona", "periodo": "$_id.periodo",
                      "valor": {"$round": ["$valor", 2]}, "lecturas": 1}},
        {"$sort": {"periodo": 1}},
    ])
    if not df.empty:
        df["variable"], df["unidad"], df["fuente"] = variable, UNIDADES_VARIABLE.get(variable, ""), FUENTE_MONGO
    return df


def comparativa_zonas(f: Filtros, variable: str = "pm25") -> pd.DataFrame:
    """Promedio, máximo y nº de lecturas de ``variable`` por zona. Fuente MongoDB."""
    tipo = "clima" if variable in VARIABLES_CLIMA else "calidad_aire"
    df = _agg("lectura", [
        {"$match": {**_match_lectura(f, tipo), f"mediciones.{variable}": {"$type": "number"}}},
        *_etapa_perfil(f), *_etapa_valor(f, variable),
        {"$group": {"_id": "$ubicacion.zona", "promedio": {"$avg": f"$mediciones.{variable}"},
                    "maximo": {"$max": f"$mediciones.{variable}"}, "lecturas": {"$sum": 1}}},
        {"$project": {"_id": 0, "zona": "$_id", "promedio": {"$round": ["$promedio", 2]},
                      "maximo": {"$round": ["$maximo", 2]}, "lecturas": 1}},
        {"$sort": {"promedio": -1}},
    ])
    if not df.empty:
        df["variable"], df["unidad"], df["fuente"] = variable, UNIDADES_VARIABLE.get(variable, ""), FUENTE_MONGO
    return df


def distribucion_tipos(f: Filtros) -> pd.DataFrame:
    """Lecturas y sensores por tipo de sensor (para la dona). Fuente MongoDB."""
    df = _agg("lectura", [
        {"$match": _match_lectura(f)}, *_etapa_perfil(f),
        {"$group": {"_id": "$tipo", "lecturas": {"$sum": 1}, "sensores": {"$addToSet": "$sensor_id"}}},
        {"$project": {"_id": 0, "tipo": "$_id", "lecturas": 1, "sensores": {"$size": "$sensores"}}},
        {"$sort": {"lecturas": -1}},
    ])
    if not df.empty:
        df["fuente"] = FUENTE_MONGO
    return df


def sensores_mongo(f: Filtros) -> pd.DataFrame:
    """Sensores que reportan en MongoDB, con posición y PM2.5 promedio (para el mapa)."""
    df = _agg("lectura", [
        {"$match": _match_lectura(f)}, *_etapa_perfil(f),
        {"$group": {"_id": "$sensor_id", "tipo": {"$first": "$tipo"}, "zona": {"$first": "$ubicacion.zona"},
                    "coords": {"$first": "$ubicacion.geo.coordinates"}, "lecturas": {"$sum": 1},
                    "pm25": {"$avg": "$mediciones.pm25"}, "ultima": {"$max": "$timestamp"},
                    "origen": {"$first": "$origen"}}},
        {"$project": {"_id": 0, "id_sensor": "$_id", "tipo": 1, "zona": 1, "lecturas": 1, "ultima": 1, "origen": 1,
                      "pm25": {"$round": ["$pm25", 2]},
                      "lon": {"$arrayElemAt": ["$coords", 0]}, "lat": {"$arrayElemAt": ["$coords", 1]}}},
        {"$sort": {"id_sensor": 1}},
    ])
    if not df.empty:
        df["fuente"] = FUENTE_MONGO
    return df


def incidentes(f: Filtros, limite: int = 500) -> pd.DataFrame:
    df = _df(db().incidente.find(_match_zona_fecha(f, "reportado"),
                                 {"_id": 0, "reportado": 1, "categoria": 1, "titulo": 1, "descripcion": 1,
                                  "zona.nombre": 1, "estado_actual": 1, "prioridad": 1})
             .sort("reportado", -1).limit(int(limite)))
    if not df.empty:
        df = df.rename(columns={"zona.nombre": "zona"})
        df["fuente"] = FUENTE_MONGO
    return df


def eventos(f: Filtros, limite: int = 500) -> pd.DataFrame:
    m = _match_zona_fecha(f, "inicio")
    if f.tipo_evento:
        m["tipo_evento"] = f.tipo_evento
    df = _df(db().evento.find(m, {"_id": 0, "inicio": 1, "fin": 1, "tipo_evento": 1, "severidad": 1,
                                  "zona.nombre": 1, "sensores": 1}).sort("inicio", -1).limit(int(limite)))
    if not df.empty:
        df = df.rename(columns={"zona.nombre": "zona"})
        df["fuente"] = FUENTE_MONGO
    return df


def alertas(f: Filtros, limite: int = 500) -> pd.DataFrame:
    m = _match_zona_fecha(f, "emitida")
    if f.id_sensor:
        m["sensor_id"] = f.id_sensor
    df = _df(db().alerta.find(m, {"_id": 0, "emitida": 1, "regla": 1, "variable": 1, "valor": 1, "umbral": 1,
                                  "nivel": 1, "estado": 1, "sensor_id": 1, "zona.nombre": 1})
             .sort("emitida", -1).limit(int(limite)))
    if not df.empty:
        df = df.rename(columns={"zona.nombre": "zona"})
        df["fuente"] = FUENTE_MONGO
    return df


def tipos_evento() -> list[str]:
    return sorted(db().evento.distinct("tipo_evento"))


def ids_sensores() -> list[str]:
    return sorted(db().lectura.distinct("sensor_id"))


# ---------------------------------------------------------------------- Spark (resultados en HDFS)
ANALISIS_SPARK = {
    "descriptivo": ["descriptivo"],
    "temporal": ["temporal_pm25_mensual", "temporal_temperatura_hora"],
    "correlacional": ["correlacional_matriz"],
    "predictivo": ["predictivo_metricas", "predictivo_estimaciones_2024"],
    "espacial": ["espacial_zonas_pm25", "espacial_sensores_pm25"],
    "movilidad": ["movilidad_proxy_perfil_horario", "movilidad_proxy_pico_valle", "movilidad_proxy_top3_pico_valle",
                  "movilidad_trafico_perfil_horario", "movilidad_trafico_horas_pico",
                  "movilidad_trafico_correlacion_pm25", "movilidad_trafico_cruce_meteo"],
    "anomalias": ["anomalias_resumen", "anomalias_sensores"],
    "comparativo": ["comparativo_perfil_anual", "comparativo_pm25_2023_2024", "comparativo_top3_incremento_interanual"],
}


def _con_fuente(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["fuente"] = FUENTE_SPARK
    return df


def spark_top_zonas_pico() -> pd.DataFrame:
    """Top 3 zonas con mayor incremento de PM2.5 en hora pico vs valle (B4.2, gráfico Spark del dashboard)."""
    return _con_fuente(leer_parquet_hdfs(ruta_analitica("movilidad_proxy_top3_pico_valle")))


def spark_analisis(tipo: str) -> dict[str, pd.DataFrame]:
    """Salidas Parquet de uno de los 8 análisis de B4.2: {nombre_salida: DataFrame}."""
    if tipo not in ANALISIS_SPARK:
        raise ValueError(f"Análisis desconocido: {tipo}. Válidos: {list(ANALISIS_SPARK)}")
    salida = {}
    for nombre in ANALISIS_SPARK[tipo]:
        try:
            salida[nombre] = _con_fuente(leer_parquet_hdfs(ruta_analitica(nombre)))
        except FileNotFoundError:
            salida[nombre] = pd.DataFrame()
    return salida


def spark_correlacion() -> pd.DataFrame:
    return _con_fuente(leer_parquet_hdfs(ruta_analitica("correlacional_matriz")))


def spark_pronostico() -> dict[str, pd.DataFrame]:
    return {
        "metricas": _con_fuente(leer_parquet_hdfs(ruta_analitica("predictivo_metricas"))),
        "estimaciones": _con_fuente(leer_parquet_hdfs(ruta_analitica("predictivo_estimaciones_2024"))),
    }


def recalcular_analisis(*tipos: str) -> tuple[int, str]:
    """Botón "Recalcular": relanza analitica.py con los análisis indicados (D-04)."""
    return lanzar_job_spark("analitica", "--analisis", *(tipos or ("movilidad",)))


# ---------------------------------------------------------------------- Movilidad y energía (D-03, sintéticos)
_CACHE_TE: dict = {}


def _trafico_energia_completo() -> pd.DataFrame:
    """Parquet completo de B3.3 (≈175 mil filas), en memoria 10 minutos."""
    if _CACHE_TE.get("t", 0) < time.time() - 600:
        df = leer_parquet_hdfs(RUTAS_HDFS["trafico_energia"])
        df["timestamp"] = normalizar_timestamp(df["timestamp"])
        _CACHE_TE.update(df=df, t=time.time())
    return _CACHE_TE["df"]


def trafico_energia(f: Filtros) -> pd.DataFrame:
    """Salida del ETL B3.3 (tráfico y energía SIMULADOS) filtrada; columnas del contrato + extras."""
    return Filtros(fecha_inicio=f.fecha_inicio, fecha_fin=f.fecha_fin, perfil_horario=f.perfil_horario,
                   zonas=f.zonas).aplicar(_trafico_energia_completo(), col_valor=None, col_sensor=None, col_tipo=None)


def _periodo_anterior(f: Filtros) -> Optional[Filtros]:
    """Mismo largo que el rango filtrado, inmediatamente antes (para «variación respecto al período anterior»)."""
    if not (f.fecha_inicio and f.fecha_fin):
        return None
    dias = (f.fecha_fin - f.fecha_inicio).days + 1
    return Filtros(fecha_inicio=f.fecha_inicio - timedelta(days=dias), fecha_fin=f.fecha_inicio - timedelta(days=1),
                   perfil_horario=f.perfil_horario, zonas=f.zonas)


def lecturas_trafico_energia(f: Filtros) -> pd.DataFrame:
    """Tráfico/energía en COLUMNAS_ESTANDAR con ``fuente="Spark"``."""
    df = trafico_energia(f)
    if f.tipos_metrica:
        df = df[df["tipo_metrica"].isin(f.tipos_metrica)]
    df = f.aplicar(df, col_timestamp="__sin__", col_zona="__sin__", col_tipo=None)
    validar_zonas(df)
    validar_tipo_metrica(df)
    return ordenar_columnas_estandar(df)


def kpis_movilidad(f: Filtros) -> dict:
    df = trafico_energia(f)
    t = df[df["tipo_metrica"] == "trafico"]
    if t.empty:
        return {"fuente": FUENTE_SPARK, "sintetico": True}
    por_hora = t.groupby(t["timestamp"].dt.hour)["valor"].mean()
    return {
        "vehiculos_detectados": float(t["valor"].sum()),
        "vehiculos_hora_promedio": float(t["valor"].mean()),
        "velocidad_promedio_kmh": float(t["velocidad_promedio_kmh"].mean()),
        "congestion_promedio_pct": float(t["congestion_pct"].mean()),
        "hora_pico_utc": int(por_hora.idxmax()),
        "unidad": UNIDADES["trafico"], "fuente": FUENTE_SPARK, "sintetico": True,
    }


def kpis_energia(f: Filtros) -> dict:
    df = trafico_energia(f)
    e = df[df["tipo_metrica"] == "energia"]
    if e.empty:
        return {"fuente": FUENTE_SPARK, "sintetico": True}
    por_hora = e.groupby(e["timestamp"].dt.hour)["valor"].mean()
    total = float(e["valor"].sum())
    variacion = None
    anterior = _periodo_anterior(f)
    if anterior is not None:
        prev = trafico_energia(anterior)
        prev_total = float(prev.loc[prev["tipo_metrica"] == "energia", "valor"].sum())
        variacion = 100 * (total - prev_total) / prev_total if prev_total > 0 else None
    return {
        "consumo_total_kwh": total,
        "consumo_promedio_por_zona_kwh": total / e["zona"].nunique(),
        "variacion_pct_periodo_anterior": variacion,
        "consumo_horario_promedio_kwh": float(e["valor"].mean()),
        "consumo_maximo_kwh": float(e["valor"].max()),
        "hora_pico_utc": int(por_hora.idxmax()),
        "unidad": UNIDADES["energia"], "fuente": FUENTE_SPARK, "sintetico": True,
    }


def kpis_incidentes(f: Filtros) -> dict:
    """Accidentes de tránsito e incidentes urbanos reportados (MongoDB, colección incidente)."""
    m = _match_zona_fecha(f, "reportado")
    return {
        "incidentes": db().incidente.count_documents(m),
        "accidentes": db().incidente.count_documents({**m, "categoria": "accidente_transito"}),
        "fuente": FUENTE_MONGO,
    }


# ---------------------------------------------------------------------- Streaming (reto adicional)
def stream_ventanas(minutos: int = 30) -> pd.DataFrame:
    """Ventanas de 1 minuto por zona calculadas por Spark Structured Streaming (colección stream_ventana)."""
    desde = datetime.now(timezone.utc) - timedelta(minutes=int(minutos))
    df = _df(db().stream_ventana.find({"inicio": {"$gte": desde}}, {"_id": 0}).sort("inicio", 1))
    if not df.empty:
        df["fuente"] = FUENTE_SPARK
    return df


def stream_alertas(limite: int = 50) -> pd.DataFrame:
    """Alertas generadas por el job de streaming (reglas stream_pm25 / stream_congestion)."""
    df = _df(db().alerta.find({"origen": "streaming"},
                              {"_id": 0, "emitida": 1, "regla": 1, "zona.nombre": 1, "variable": 1, "valor": 1,
                               "umbral": 1, "nivel": 1, "estado": 1}).sort("emitida", -1).limit(int(limite)))
    if not df.empty:
        df = df.rename(columns={"zona.nombre": "zona"})
        df["fuente"] = FUENTE_SPARK
    return df


def stream_enviar_lote(episodio: Optional[str] = None, congestion: Optional[str] = None) -> str:
    """Envía un lote simulado de lecturas a HDFS (/smartcity/streaming/entrada)."""
    from scripts.streaming_productor import enviar_lote, generar_lote
    return enviar_lote(generar_lote(episodio=episodio, congestion=congestion), CONFIG.contenedor_namenode)


def streaming_activo() -> bool:
    r = _docker("exec", CONFIG.contenedor_spark, "pgrep", "-f", "streaming_alertas.py", timeout=20)
    return r.returncode == 0 and bool(r.stdout.strip())


def streaming_iniciar(duracion: int = 900) -> str:
    """Lanza el job de streaming en segundo plano dentro del master (docker exec -d)."""
    if streaming_activo():
        return "El job de streaming ya está corriendo."
    r = _docker(
        "exec", "-d", "--user", "0:0", "-e", "HOME=/tmp", "-e", "HADOOP_USER_NAME=root", "-e", "MONGO_URI",
        CONFIG.contenedor_spark, "/opt/bitnami/spark/bin/spark-submit",
        "--master", "spark://spark-master:7077", "--total-executor-cores", "1",
        "--conf", "spark.jars.ivy=/tmp/.ivy2",
        "--packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.4.1",
        "--py-files", "/opt/smartcity/spark/jobs/common.py,/opt/smartcity/spark/jobs/rutas.py",
        "/opt/smartcity/spark/jobs/streaming_alertas.py", "--duracion", str(int(duracion)),
        env={"MONGO_URI": CONFIG.mongo_uri_spark}, timeout=30)
    return "Job de streaming iniciado." if r.returncode == 0 else f"No se pudo iniciar: {r.stderr[-300:]}"


def streaming_detener() -> str:
    r = _docker("exec", CONFIG.contenedor_spark, "pkill", "-f", "streaming_alertas.py", timeout=20)
    return "Job de streaming detenido." if r.returncode == 0 else "No había un job de streaming corriendo."


__all__ = [
    "estado", "db", "lanzar_job_spark", "leer_parquet_hdfs", "CONSULTAS_MONGO", "CONSULTAS_SPARK", "ejecutar",
    "crud", "OPERACIONES_CRUD", "documentos_demo_crud", "lecturas_mongo", "kpis_ambiente", "kpis_iot_mongo",
    "serie_temporal", "comparativa_zonas", "distribucion_tipos", "sensores_mongo", "incidentes", "eventos",
    "alertas", "tipos_evento", "ids_sensores", "spark_top_zonas_pico", "spark_analisis", "spark_correlacion",
    "spark_pronostico", "recalcular_analisis", "ANALISIS_SPARK", "trafico_energia", "lecturas_trafico_energia",
    "kpis_movilidad", "kpis_energia", "kpis_incidentes", "stream_ventanas", "stream_alertas", "stream_enviar_lote",
    "streaming_activo", "streaming_iniciar", "streaming_detener", "ZONAS",
]
