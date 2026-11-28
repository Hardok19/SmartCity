"""conectores_postgres_hadoop.py — Módulo de la pareja A (PostgreSQL + Hadoop) para el dashboard (A5).

La UI no contiene consultas: todo lo que muestra de PostgreSQL y HDFS/MapReduce sale de este módulo.

Secciones:
  1. Conexiones y estado      estado(), engine(), ejecutar_sql(), ejecutar_en_namenode()
  2. Consultas PostgreSQL     CONSULTAS_PG (PG-Q01…Q10, PG-P01…P07, leídas de sql/queries/*.sql), explain()
  3. HDFS                     resumen_hdfs() (JMX), operaciones hdfs_ls/cat/put/get/rm/mkdir/fsck
  4. MapReduce                MR_JOBS, mr_ejecutar(), mr_resultado(), CONSULTAS_HDFS (HD-OPS + HD-MR01…05)
  5. Funciones del dashboard  sensores(f), kpis_iot(f), lecturas_pg(f), lecturas_hdfs(f), kpis_ambiente()

Las operaciones que modifican HDFS (put, rm, mkdir) solo se permiten dentro de /smartcity/sandbox.
Cada operación HDFS devuelve también el comando `hdfs dfs` equivalente para mostrarlo en la UI.
"""
from __future__ import annotations

import json
import re
import shlex
import subprocess
import time
import urllib.request
from decimal import Decimal
from functools import lru_cache
from pathlib import Path
from typing import Any, Optional

import pandas as pd

from comun import CONFIG, Consulta, Filtros
from contrato import (
    COLUMNAS_ESTANDAR, FUENTE_HDFS, FUENTE_POSTGRES, METRICA_DE_CATEGORIA, TIPOS_METRICA, normalizar_timestamp,
    ordenar_columnas_estandar, tipos_sensor_de, validar_estado, validar_tipo_metrica, validar_zonas,
)

RAIZ = Path(__file__).resolve().parent
DIR_CONSULTAS = RAIZ / "sql" / "queries"
DIR_MAPREDUCE = RAIZ / "hadoop" / "mapreduce"
SANDBOX = "/smartcity/sandbox"
# Códigos de variables_medicion (= medicion_horaria.variable). Desde D-01 son también tipos del contrato, así que
# tipo_metrica = variable. "calidad_aire" se acepta como alias de pm25 (contrato.METRICA_DE_CATEGORIA).
VARIABLES_PG = ["pm25", "pm10", "co", "no2", "o3", "co2", "temperatura", "humedad", "precipitacion", "viento",
                "trafico", "energia"]
assert set(VARIABLES_PG) <= set(TIPOS_METRICA)


def _variables_pedidas(tipos) -> dict[str, str]:
    """{variable de medicion_horaria: etiqueta tipo_metrica de salida} para los tipos del filtro."""
    pedidas = {}
    for t in tipos:
        variable = METRICA_DE_CATEGORIA.get(t, t)
        if variable in VARIABLES_PG:
            pedidas[variable] = t
    return pedidas

# =====================================================================================
# 1. Conexiones y estado
# =====================================================================================


@lru_cache(maxsize=1)
def engine():
    from sqlalchemy import create_engine
    return create_engine(CONFIG.pg_url, pool_pre_ping=True, pool_size=3, max_overflow=2,
                         connect_args={"connect_timeout": 5})


def _a_float(df: pd.DataFrame) -> pd.DataFrame:
    """NUMERIC de PostgreSQL llega como Decimal: se convierte a float para pandas/Plotly."""
    for col in df.columns:
        if df[col].dtype == object and df[col].map(lambda v: isinstance(v, Decimal)).any():
            df[col] = df[col].map(lambda v: float(v) if isinstance(v, Decimal) else v)
    return df


def ejecutar_sql(sql: str, params: Optional[dict] = None) -> pd.DataFrame:
    """SQL parametrizado (estilo %(nombre)s) -> DataFrame."""
    with engine().connect() as con:
        res = con.exec_driver_sql(sql, params or {})
        if not res.returns_rows:
            return pd.DataFrame()
        return _a_float(pd.DataFrame(res.fetchall(), columns=list(res.keys())))


def _jmx(qry: str) -> dict:
    with urllib.request.urlopen(f"{CONFIG.hdfs_web.rstrip('/')}/jmx?qry={qry}", timeout=4) as r:
        beans = json.loads(r.read().decode("utf-8")).get("beans", [])
    return beans[0] if beans else {}


def estado() -> dict:
    """``{"PostgreSQL": bool, "HDFS": bool, "latencia_ms": {...}, "detalle": {...}}``."""
    salida = {"latencia_ms": {}, "detalle": {}}
    t0 = time.perf_counter()
    try:
        v = ejecutar_sql("SELECT current_database() AS base, split_part(version(), ',', 1) AS version")
        salida[FUENTE_POSTGRES] = True
        salida["detalle"][FUENTE_POSTGRES] = f"{v.at[0, 'base']} · {v.at[0, 'version']}"
    except Exception as e:  # noqa: BLE001 — el dashboard solo necesita saber si responde
        salida[FUENTE_POSTGRES] = False
        salida["detalle"][FUENTE_POSTGRES] = f"{type(e).__name__}: {str(e)[:160]}"
    salida["latencia_ms"][FUENTE_POSTGRES] = round((time.perf_counter() - t0) * 1000, 1)
    t0 = time.perf_counter()
    try:
        b = _jmx("Hadoop:service=NameNode,name=FSNamesystemState")
        vivos = int(b.get("NumLiveDataNodes", 0))
        salida[FUENTE_HDFS] = vivos > 0
        salida["detalle"][FUENTE_HDFS] = f"DataNodes vivos: {vivos}, caídos: {b.get('NumDeadDataNodes', 0)}"
    except Exception as e:  # noqa: BLE001
        salida[FUENTE_HDFS] = False
        salida["detalle"][FUENTE_HDFS] = f"{type(e).__name__}: {str(e)[:160]}"
    salida["latencia_ms"][FUENTE_HDFS] = round((time.perf_counter() - t0) * 1000, 1)
    return salida


def ejecutar_en_namenode(comando: str, timeout: int = 300, entrada: Optional[bytes] = None) -> tuple[int, str]:
    """Ejecuta ``bash -c comando`` dentro del NameNode. Devuelve (código, salida estándar + error)."""
    r = subprocess.run(["docker", "exec", "-i", CONFIG.contenedor_namenode, "bash", "-c", comando],
                       input=entrada, capture_output=True, timeout=timeout)
    salida = r.stdout.decode("utf-8", "replace")
    errores = "\n".join(ln for ln in r.stderr.decode("utf-8", "replace").splitlines()
                        if " WARN " not in ln and " INFO " not in ln)
    return r.returncode, salida + (("\n" + errores) if errores.strip() else "")


# =====================================================================================
# 2. Consultas PostgreSQL (A2.3)
# =====================================================================================


def _leer_consulta(ruta: Path) -> dict:
    """Lee la cabecera (-- id:, -- titulo:, -- tecnicas:, -- parametros: a=1; b=x) y el SQL."""
    texto = ruta.read_text(encoding="utf-8")
    cabecera = dict(re.findall(r"^--[ 	]*(id|titulo|tecnicas|parametros):[ 	]*(.*)$", texto, flags=re.M))
    parametros = {}
    for par in filter(None, (p.strip() for p in cabecera.get("parametros", "").split(";"))):
        nombre, _, valor = par.partition("=")
        valor = valor.strip()
        if re.fullmatch(r"-?\d+", valor):
            parametros[nombre.strip()] = int(valor)
        elif re.fullmatch(r"-?\d+\.\d+", valor):
            parametros[nombre.strip()] = float(valor)
        else:
            parametros[nombre.strip()] = valor
    return {"id": cabecera.get("id", ruta.stem[:6]), "titulo": cabecera.get("titulo", ruta.stem),
            "tecnicas": cabecera.get("tecnicas", ""), "parametros": parametros, "sql": texto}


def _sql_ejecutable(sql: str) -> str:
    """Quita comentarios de línea y el ';' final (psycopg2 no los necesita)."""
    lineas = [ln for ln in sql.splitlines() if not ln.strip().startswith("--")]
    return "\n".join(lineas).strip().rstrip(";")


def _registro_pg() -> dict[str, Consulta]:
    registro = {}
    for ruta in sorted(DIR_CONSULTAS.glob("PG-*.sql")):
        c = _leer_consulta(ruta)
        sql = _sql_ejecutable(c["sql"])
        registro[c["id"]] = Consulta(
            id=c["id"], tecnologia=FUENTE_POSTGRES, titulo=c["titulo"],
            descripcion=f"Técnicas: {c['tecnicas']}", codigo=c["sql"], lenguaje="sql",
            parametros=c["parametros"],
            funcion=(lambda _sql=sql, **p: ejecutar_sql(_sql, p)))
    return registro


CONSULTAS_PG: dict[str, Consulta] = _registro_pg()


def explain(ident: str, **params) -> str:
    """``EXPLAIN (ANALYZE, BUFFERS)`` de una consulta registrada (muestra índices y poda de particiones)."""
    c = CONSULTAS_PG[ident]
    sql = _sql_ejecutable(c.codigo)
    plan = ejecutar_sql("EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) " + sql, {**c.parametros, **params})
    return "\n".join(plan.iloc[:, 0].astype(str))


# =====================================================================================
# 3. HDFS
# =====================================================================================


def resumen_hdfs() -> dict:
    """Estado del clúster desde el JMX del NameNode: DataNodes, capacidad, bloques, archivos, replicación."""
    b = _jmx("Hadoop:service=NameNode,name=FSNamesystemState")
    r = _jmx("Hadoop:service=NameNode,name=FSNamesystem")
    return {
        "datanodes_vivos": int(b.get("NumLiveDataNodes", 0)),
        "datanodes_caidos": int(b.get("NumDeadDataNodes", 0)),
        "capacidad_gb": round(b.get("CapacityTotal", 0) / 1e9, 1),
        "usado_mb": round(b.get("CapacityUsed", 0) / 1e6, 1),
        "bloques": int(b.get("BlocksTotal", 0)),
        "archivos": int(b.get("FilesTotal", 0)),
        "bloques_subreplicados": int(b.get("UnderReplicatedBlocks", 0)),
        "bloques_faltantes": int(r.get("MissingBlocks", 0)),
        "fuente": FUENTE_HDFS,
    }


def _verificar_sandbox(ruta: str) -> None:
    if not (ruta == SANDBOX or ruta.startswith(SANDBOX + "/")) or ".." in ruta:
        raise PermissionError(f"Por seguridad, solo se modifican rutas dentro de {SANDBOX}: {ruta}")


def _hdfs(args: str, timeout: int = 120) -> tuple[str, str]:
    comando = f"hdfs dfs {args}"
    codigo, salida = ejecutar_en_namenode(comando + " 2>&1", timeout=timeout)
    if codigo != 0:
        raise RuntimeError(f"{comando} falló ({codigo}): {salida.strip()[:600]}")
    return salida, comando


def hdfs_ls(ruta: str = "/smartcity", recursivo: bool = False) -> tuple[pd.DataFrame, str]:
    salida, comando = _hdfs(f"-ls {'-R ' if recursivo else ''}{shlex.quote(ruta)}")
    filas = []
    for ln in salida.splitlines():
        p = ln.split(None, 7)
        if len(p) == 8 and p[0][0] in "-d":
            filas.append({"tipo": "directorio" if p[0].startswith("d") else "archivo", "permisos": p[0],
                          "replicacion": None if p[1] == "-" else int(p[1]), "bytes": int(p[4]),
                          "modificado": f"{p[5]} {p[6]}", "ruta": p[7]})
    return pd.DataFrame(filas), comando


def hdfs_cat(ruta: str, lineas: int = 20) -> tuple[str, str]:
    codigo, salida = ejecutar_en_namenode(f"hdfs dfs -cat {shlex.quote(ruta)} 2>/dev/null | head -n {int(lineas)}")
    return salida, f"hdfs dfs -cat {ruta} | head -n {int(lineas)}"


def hdfs_mkdir(ruta: str) -> tuple[str, str]:
    _verificar_sandbox(ruta)
    salida, comando = _hdfs(f"-mkdir -p {shlex.quote(ruta)}")
    return salida or "OK", comando


def hdfs_put(contenido: bytes, destino: str) -> tuple[str, str]:
    """Sube bytes (p. ej. un archivo elegido en la UI) a HDFS vía stdin (`hdfs dfs -put - destino`)."""
    _verificar_sandbox(destino)
    padre = destino.rsplit("/", 1)[0]
    comando = f"hdfs dfs -mkdir -p {shlex.quote(padre)} && hdfs dfs -put -f - {shlex.quote(destino)}"
    codigo, salida = ejecutar_en_namenode(comando + " 2>&1", entrada=contenido)
    if codigo != 0:
        raise RuntimeError(salida[:600])
    return f"{len(contenido):,} bytes subidos a {destino}", f"hdfs dfs -put <archivo> {destino}"


def hdfs_get(ruta: str) -> tuple[bytes, str]:
    """Descarga un archivo de HDFS (devuelve los bytes; la UI los ofrece para guardar)."""
    r = subprocess.run(["docker", "exec", CONFIG.contenedor_namenode, "hdfs", "dfs", "-cat", ruta],
                       capture_output=True, timeout=300)
    if r.returncode != 0:
        raise RuntimeError(r.stderr.decode("utf-8", "replace")[-600:])
    return r.stdout, f"hdfs dfs -get {ruta} <destino_local>"


def hdfs_rm(ruta: str) -> tuple[str, str]:
    _verificar_sandbox(ruta)
    salida, comando = _hdfs(f"-rm -r -skipTrash {shlex.quote(ruta)}")
    return salida.strip(), comando


def hdfs_fsck(ruta: str) -> tuple[pd.DataFrame, str]:
    """Bloques y DataNodes que guardan cada réplica (``hdfs fsck <ruta> -files -blocks -locations``)."""
    comando = f"hdfs fsck {ruta} -files -blocks -locations"
    codigo, salida = ejecutar_en_namenode(comando + " 2>/dev/null", timeout=180)
    filas = []
    for ln in salida.splitlines():
        m = re.match(r"\s*(\d+)\.\s+(BP-\S+:blk_\d+)_\d+\s+len=(\d+)\s+Live_repl=(\d+)\s+\[(.*)\]", ln)
        if m:
            nodos = re.findall(r"DatanodeInfoWithStorage\[([\d.]+):\d+", m.group(5))
            filas.append({"bloque": int(m.group(1)), "id_bloque": m.group(2).split(":")[1], "bytes": int(m.group(3)),
                          "replicas_vivas": int(m.group(4)), "datanodes": ", ".join(nodos)})
    df = pd.DataFrame(filas)
    df.attrs["resumen"] = "\n".join(ln for ln in salida.splitlines() if re.match(r"\s*(Status|Total|Default replication|Under-replicated|Missing)", ln))
    return df, comando


# =====================================================================================
# 4. MapReduce (A4)
# =====================================================================================
ENTRADA_LECTURAS = "/smartcity/raw/mongo_exports/lectura/*/lectura.jsonl"
ENTRADA_EVENTOS = "/smartcity/raw/mongo_exports/evento/*/evento.jsonl"

MR_JOBS = {
    "mr01": {"titulo": "¿Cuántas mediciones existen por sensor?", "entrada": ENTRADA_LECTURAS,
             "columnas": ["id_sensor", "mediciones"]},
    "mr02": {"titulo": "¿Qué sensor produjo más datos? (top 10)", "entrada": "/smartcity/mapreduce/mr01",
             "columnas": ["posicion", "id_sensor", "mediciones"]},
    "mr03": {"titulo": "Temperatura promedio histórica (global, por zona y por zona-año)", "entrada": ENTRADA_LECTURAS,
             "columnas": ["ambito", "zona", "anio", "temperatura_promedio", "lecturas"]},
    "mr04": {"titulo": "Cantidad de eventos por zona (y por tipo)", "entrada": ENTRADA_EVENTOS,
             "columnas": ["ambito", "zona", "tipo_evento", "eventos"]},
    "mr05": {"titulo": "Comportamiento de los sensores en un período (promedio diario)", "entrada": ENTRADA_LECTURAS,
             "columnas": ["id_sensor", "fecha", "variable", "promedio", "minimo", "maximo", "lecturas"]},
}


def mr_ejecutar(job: str, desde: str = "2024-01-01", hasta: str = "2024-01-31") -> tuple[int, str]:
    """Lanza un job de Hadoop Streaming en YARN (copia el job al NameNode y ejecuta run_job.sh)."""
    if job not in MR_JOBS:
        raise ValueError(f"Job desconocido: {job}")
    subprocess.run(["docker", "exec", CONFIG.contenedor_namenode, "mkdir", "-p", "/tmp/smartcity_mr"],
                   check=True, capture_output=True)
    for nombre in ("run_job.sh", job):
        subprocess.run(["docker", "cp", str(DIR_MAPREDUCE / nombre), f"{CONFIG.contenedor_namenode}:/tmp/smartcity_mr/"],
                       check=True, capture_output=True)
    comando = (f"cd /tmp/smartcity_mr && DESDE={shlex.quote(desde)} HASTA={shlex.quote(hasta)} "
               f"bash run_job.sh {job} {shlex.quote(MR_JOBS[job]['entrada'])} /smartcity/mapreduce/{job}")
    return ejecutar_en_namenode(comando + " 2>&1", timeout=1800)


def mr_resultado(job: str) -> pd.DataFrame:
    """Salida del job (/smartcity/mapreduce/<job>/part-*) como DataFrame, con fuente="HDFS"."""
    codigo, salida = ejecutar_en_namenode(f"hdfs dfs -cat /smartcity/mapreduce/{job}/part-* 2>/dev/null")
    if codigo != 0 or not salida.strip():
        raise FileNotFoundError(f"Sin resultado de {job}: ejecutar el job primero (mr_ejecutar('{job}'))")
    cols = MR_JOBS[job]["columnas"]
    filas = [ln.split("\t") for ln in salida.splitlines() if ln.strip()]
    df = pd.DataFrame([f[:len(cols)] for f in filas], columns=cols)
    for c in df.columns:
        convertido = pd.to_numeric(df[c], errors="coerce")
        if convertido.notna().all():
            df[c] = convertido
    df["fuente"] = FUENTE_HDFS
    return df


def _fn_mr(job):
    def correr(recalcular: bool = False, desde: str = "2024-01-01", hasta: str = "2024-01-31"):
        if recalcular:
            codigo, salida = mr_ejecutar(job, desde, hasta)
            if codigo != 0:
                raise RuntimeError(f"El job {job} terminó con código {codigo}:\n{salida[-1500:]}")
        return mr_resultado(job)
    return correr


def _codigo_mr(job: str) -> str:
    partes = []
    for nombre in ("mapper.py", "reducer.py"):
        ruta = DIR_MAPREDUCE / job / nombre
        if ruta.exists():
            partes.append(f"# ===== {job}/{nombre} =====\n" + ruta.read_text(encoding="utf-8"))
    return "\n\n".join(partes) or f"# {job}: código no encontrado"


def _hd_ls(ruta: str = "/smartcity", recursivo: bool = False):
    return hdfs_ls(ruta, recursivo)[0]


def _hd_du(ruta: str = "/smartcity"):
    codigo, salida = ejecutar_en_namenode(f"hdfs dfs -du -h {shlex.quote(ruta)} 2>/dev/null")
    filas = [ln.split(None, 4) for ln in salida.splitlines() if ln.strip()]
    return pd.DataFrame([{"tamano": f"{f[0]} {f[1]}", "con_replicas": f"{f[2]} {f[3]}", "ruta": f[4]}
                         for f in filas if len(f) == 5])


def _hd_fsck(ruta: str = "/smartcity/raw/mongo_exports"):
    return hdfs_fsck(ruta)[0]


CONSULTAS_HDFS: dict[str, Consulta] = {
    "HD-OPS-LS": Consulta(id="HD-OPS-LS", tecnologia=FUENTE_HDFS, titulo="Listar directorio (hdfs dfs -ls)",
                          descripcion="Estructura de /smartcity (raw, curated, analytics, mapreduce, sandbox).",
                          codigo="hdfs dfs -ls [-R] <ruta>", lenguaje="bash",
                          parametros={"ruta": "/smartcity", "recursivo": False}, funcion=_hd_ls),
    "HD-OPS-DU": Consulta(id="HD-OPS-DU", tecnologia=FUENTE_HDFS, titulo="Espacio usado (hdfs dfs -du -h)",
                          descripcion="Tamaño lógico y tamaño con réplicas de cada carpeta.",
                          codigo="hdfs dfs -du -h <ruta>", lenguaje="bash", parametros={"ruta": "/smartcity"},
                          funcion=_hd_du),
    "HD-OPS-FSCK": Consulta(id="HD-OPS-FSCK", tecnologia=FUENTE_HDFS, titulo="Bloques y réplicas (hdfs fsck)",
                            descripcion="Cada bloque del archivo y en qué DataNodes está cada réplica.",
                            codigo="hdfs fsck <ruta> -files -blocks -locations", lenguaje="bash",
                            parametros={"ruta": "/smartcity/raw/mongo_exports"}, funcion=_hd_fsck),
}
for _n, (_job, _def) in enumerate(MR_JOBS.items(), start=1):
    _id = f"HD-MR{_n:02d}"
    CONSULTAS_HDFS[_id] = Consulta(
        id=_id, tecnologia=FUENTE_HDFS, titulo=_def["titulo"],
        descripcion=f"Hadoop Streaming (Python) sobre YARN. Entrada: {_def['entrada']} · salida: /smartcity/mapreduce/{_job}. "
                    "Sin «recalcular» se muestra la última salida guardada.",
        codigo=_codigo_mr(_job), lenguaje="python",
        parametros={"recalcular": False, **({"desde": "2024-01-01", "hasta": "2024-01-31"} if _job == "mr05" else {})},
        funcion=_fn_mr(_job))


def ejecutar(ident: str, **params) -> pd.DataFrame:
    consulta = CONSULTAS_PG.get(ident) or CONSULTAS_HDFS.get(ident)
    if consulta is None:
        raise KeyError(f"Consulta desconocida: {ident}")
    return consulta.ejecutar(**params)


# =====================================================================================
# 5. Funciones para el dashboard (reciben comun.Filtros)
# =====================================================================================


def _f(filtros) -> Filtros:
    return filtros if isinstance(filtros, Filtros) else Filtros()


def sensores(filtros: Any = None) -> pd.DataFrame:
    """Catálogo con lat/lon y estado (mapa y tablas). Coordenadas = centroide de la zona del sensor."""
    f = _f(filtros)
    condiciones, params = [], {}
    if f.zonas:
        condiciones.append("zona = ANY(%(zonas)s)")
        params["zonas"] = list(f.zonas)
    if f.id_sensor:
        condiciones.append("id_sensor = %(id_sensor)s")
        params["id_sensor"] = f.id_sensor
    if f.tipos_metrica:                                   # métrica -> tipo de sensor que la mide
        condiciones.append("tipo_sensor = ANY(%(tipos)s)")
        params["tipos"] = tipos_sensor_de(f.tipos_metrica)
    sql = ("SELECT v.id_sensor, v.nombre, v.tipo_sensor, v.categoria, v.estado, v.zona, v.lat, v.lon, v.organismo, "
           "a.num_mediciones, a.ultima_lectura FROM v_sensor_detalle v LEFT JOIN mv_actividad_sensor a USING (id_sensor)"
           + (" WHERE " + " AND ".join(condiciones) if condiciones else "") + " ORDER BY v.zona, v.id_sensor")
    df = ejecutar_sql(sql, params)
    if not df.empty:
        df["lat"] = df["lat"].astype(float)
        df["lon"] = df["lon"].astype(float)
        validar_zonas(df)
        validar_estado(df)
        df["fuente"] = FUENTE_POSTGRES
    return df


def kpis_iot(filtros: Any = None) -> dict:
    """Nº de sensores, activos e inactivos (y cuántos tienen lecturas), por zona y tipo de sensor."""
    f = _f(filtros)
    condiciones, params = [], {}
    if f.zonas:
        condiciones.append("u.nombre_zona = ANY(%(zonas)s)")
        params["zonas"] = list(f.zonas)
    if f.tipos_metrica:
        condiciones.append("s.tipo_sensor = ANY(%(tipos)s)")
        params["tipos"] = tipos_sensor_de(f.tipos_metrica)
    filtro_zona = ("WHERE " + " AND ".join(condiciones)) if condiciones else ""
    r = ejecutar_sql(f"""
        SELECT COUNT(*) AS sensores,
               COUNT(*) FILTER (WHERE s.estado = 'activo')   AS activos,
               COUNT(*) FILTER (WHERE s.estado = 'inactivo') AS inactivos,
               COUNT(*) FILTER (WHERE a.num_mediciones > 0)   AS sensores_con_lecturas,
               COALESCE(SUM(a.num_mediciones), 0)             AS mediciones
        FROM sensores s JOIN ubicaciones u USING (id_ubicacion)
        LEFT JOIN mv_actividad_sensor a USING (id_sensor) {filtro_zona}""", params).iloc[0]
    return {k: int(r[k]) for k in ("sensores", "activos", "inactivos", "sensores_con_lecturas", "mediciones")} | {
        "fuente": FUENTE_POSTGRES}


def lecturas_pg(filtros: Any = None, *, limite: int = 20_000) -> pd.DataFrame:
    """Lecturas de medicion_horaria en COLUMNAS_ESTANDAR, una fila por variable (pm25, temperatura, trafico…).

    Sin tipo en el filtro se devuelven todas las variables.
    """
    f = _f(filtros)
    if not isinstance(limite, int) or not 1 <= limite <= 200_000:
        raise ValueError("limite debe ser un entero entre 1 y 200000")
    pedidas = _variables_pedidas(f.tipos_metrica) if f.tipos_metrica else {v: v for v in VARIABLES_PG}
    if not pedidas:
        return pd.DataFrame(columns=COLUMNAS_ESTANDAR)
    cond = ["m.variable = ANY(%(variables)s)"]
    params: dict = {"variables": list(pedidas), "limite": limite}
    if f.zonas:
        cond.append("u.nombre_zona = ANY(%(zonas)s)")
        params["zonas"] = list(f.zonas)
    if f.desde_utc:
        cond.append("m.timestamp >= %(desde)s")
        params["desde"] = f.desde_utc
    if f.hasta_utc:
        cond.append("m.timestamp <= %(hasta)s")
        params["hasta"] = f.hasta_utc
    if f.perfil_horario != "todo_el_dia":
        cond.append("EXTRACT(HOUR FROM m.timestamp AT TIME ZONE 'UTC')::int = ANY(%(horas)s)")
        params["horas"] = list(f.horas)
    if f.id_sensor:
        cond.append("m.id_sensor = %(id_sensor)s")
        params["id_sensor"] = f.id_sensor
    if f.valor_min is not None:
        cond.append("m.valor >= %(vmin)s")
        params["vmin"] = f.valor_min
    if f.valor_max is not None:
        cond.append("m.valor <= %(vmax)s")
        params["vmax"] = f.valor_max
    df = ejecutar_sql(f"""
        SELECT u.nombre_zona AS zona, m.timestamp, m.id_sensor,
               m.variable AS tipo_metrica,
               m.valor, 'PostgreSQL' AS fuente
        FROM medicion_horaria m
        JOIN sensores s    USING (id_sensor)
        JOIN ubicaciones u USING (id_ubicacion)
        WHERE {' AND '.join(cond)}
        ORDER BY m.timestamp DESC, m.id_sensor
        LIMIT %(limite)s""", params)
    if df.empty:
        return pd.DataFrame(columns=COLUMNAS_ESTANDAR)
    df["timestamp"] = normalizar_timestamp(df["timestamp"])
    df["tipo_metrica"] = df["tipo_metrica"].map(pedidas)          # alias: calidad_aire si así se pidió
    validar_zonas(df)
    validar_tipo_metrica(df)
    return ordenar_columnas_estandar(df)


def lecturas_hdfs(filtros: Any = None) -> pd.DataFrame:
    """Promedios diarios por sensor y variable calculados por MapReduce (mr05) en COLUMNAS_ESTANDAR, fuente="HDFS".

    Sin tipo en el filtro se devuelven todas las variables del contrato que calculó mr05.
    """
    f = _f(filtros)
    try:
        df = mr_resultado("mr05")
    except FileNotFoundError:          # aún no se ejecutó mr05: sin lecturas de HDFS
        return pd.DataFrame(columns=COLUMNAS_ESTANDAR)
    pedidas = _variables_pedidas(f.tipos_metrica) if f.tipos_metrica else {v: v for v in VARIABLES_PG}
    df = df[df["variable"].isin(list(pedidas))]
    if df.empty:
        return pd.DataFrame(columns=COLUMNAS_ESTANDAR)
    zonas = ejecutar_sql("SELECT id_sensor, zona FROM v_sensor_detalle")
    df = df.merge(zonas, on="id_sensor", how="inner")
    salida = pd.DataFrame({"zona": df["zona"], "timestamp": normalizar_timestamp(df["fecha"]),
                           "id_sensor": df["id_sensor"], "tipo_metrica": df["variable"].map(pedidas),
                           "valor": df["promedio"].astype(float), "fuente": FUENTE_HDFS})
    salida = f.aplicar(salida, col_tipo=None)                     # el tipo ya se filtró arriba (con alias)
    validar_zonas(salida)
    validar_tipo_metrica(salida)
    return ordenar_columnas_estandar(salida)


def kpis_ambiente(filtros: Any = None) -> pd.DataFrame:
    """Promedio de PM2.5, temperatura y humedad por zona (desde la vista materializada)."""
    f = _f(filtros)
    desde = f.fecha_inicio.isoformat() if f.fecha_inicio else "1900-01-01"
    hasta = f.fecha_fin.isoformat() if f.fecha_fin else "2100-12-31"
    return ejecutar_sql("SELECT * FROM fn_kpi_ambiente(%(d)s::date, %(h)s::date, NULL)", {"d": desde, "h": hasta})


__all__ = [
    "estado", "engine", "ejecutar_sql", "ejecutar_en_namenode", "CONSULTAS_PG", "CONSULTAS_HDFS", "ejecutar",
    "explain", "resumen_hdfs", "hdfs_ls", "hdfs_cat", "hdfs_put", "hdfs_get", "hdfs_rm", "hdfs_mkdir", "hdfs_fsck",
    "MR_JOBS", "mr_ejecutar", "mr_resultado", "sensores", "kpis_iot", "lecturas_pg", "lecturas_hdfs", "kpis_ambiente",
]
