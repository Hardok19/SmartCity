"""Acceso del dashboard a los dos módulos de conectores, con caché y tolerancia a fallos.

* Módulo B: ``conectores_mongo_spark`` (MongoDB + Spark) — siempre presente.
* Módulo A: ``conectores_postgres_hadoop`` (PostgreSQL + HDFS) — opcional mientras la pareja A no lo entregue.
  Si no existe se usa ``conectores_postgres`` (versión actual de A, solo PostgreSQL).

Para que una sección use una función de A basta con que exista en su módulo con el nombre que figura en
``FUNCIONES_A`` (el contrato de nombres viene del TODO, A5.1–A5.3). No hay que tocar el dashboard.
"""
from __future__ import annotations

import importlib
import time
from typing import Any, Optional

import streamlit as st

import importlib.util

MODULO_B = "conectores_mongo_spark"
CANDIDATOS_A = ("conectores_postgres_hadoop", "conectores_postgres")
MODULO_A = next((n for n in CANDIDATOS_A if importlib.util.find_spec(n) is not None), CANDIDATOS_A[0])

# Funciones de la pareja A que el dashboard sabe usar (TODO A5.1–A5.3)
FUNCIONES_A = {
    "estado": "Estado de PostgreSQL y HDFS",
    "sensores": "Catálogo de sensores con lat/lon y estado (mapa)",
    "kpis_iot": "Nº de sensores, activos e inactivos",
    "lecturas_pg": "Lecturas en COLUMNAS_ESTANDAR desde medicion_horaria",
    "lecturas_hdfs": "Lecturas/salidas MapReduce desde HDFS",
    "resumen_hdfs": "Estado del clúster HDFS (DataNodes, capacidad, bloques)",
    "CONSULTAS_PG": "Registro PG-Q01…Q10, PG-P01…P04",
    "CONSULTAS_HDFS": "Registro HD-OPS, HD-MR01…MR05",
    "explain": "EXPLAIN de una consulta PG",
}


@st.cache_resource(show_spinner=False)
def _importar(nombre: str):
    try:
        return importlib.import_module(nombre), None
    except Exception as e:  # noqa: BLE001 — el dashboard debe seguir aunque un módulo falle al importar
        return None, f"{type(e).__name__}: {e}"


def modulo(nombre: str):
    return _importar(nombre)[0]


def error_import(nombre: str) -> Optional[str]:
    return _importar(nombre)[1]


def b():
    return modulo(MODULO_B)


def a():
    return modulo(MODULO_A)


def tiene_a(funcion: str) -> bool:
    m = a()
    return m is not None and hasattr(m, funcion)


def registro_consultas(nombre_modulo: str, atributo: str, tecnologia: str) -> Optional[dict]:
    """Registro de consultas de un módulo como ``dict[str, comun.Consulta]``.

    Acepta también la forma actual de A: ``CONSULTAS_PG`` como función que devuelve
    ``{id: {"titulo", "codigo"}}`` y ``ejecutar_consulta(id)`` para correrlas.
    """
    from comun import Consulta
    m = modulo(nombre_modulo)
    reg = getattr(m, atributo, None) if m is not None else None
    if reg is None:
        return None
    if callable(reg):
        reg = reg()
    salida = {}
    for ident, c in dict(reg).items():
        if isinstance(c, Consulta):
            salida[ident] = c
            continue
        ejecutor = getattr(m, "ejecutar_consulta", None) or getattr(m, "ejecutar", None)
        salida[ident] = Consulta(
            id=ident, tecnologia=tecnologia, titulo=c.get("titulo", ident), descripcion=c.get("descripcion", ""),
            codigo=c.get("codigo", ""), lenguaje=c.get("lenguaje", "sql"), parametros=c.get("parametros", {}),
            funcion=(lambda i=ident, **kw: ejecutor(i, **kw)) if ejecutor else None)
    return salida


# 12 h: la primera carga de la vista principal con los datos completos tarda ~30 s (agregaciones de MongoDB sobre
# 1 M documentos); con la caché llena tarda < 1 s. Solo se guardan lecturas: CRUD, put/rm de HDFS y MapReduce llaman
# al conector sin caché. «Actualizar datos» (barra lateral) y «Recalcular» (Spark) la vacían.
CACHE_TTL_S = 12 * 3600


@st.cache_data(ttl=CACHE_TTL_S, show_spinner=False)
def _llamar_cache(nombre_modulo: str, funcion: str, *args, **kwargs):
    return getattr(modulo(nombre_modulo), funcion)(*args, **kwargs)


def llamar(nombre_modulo: str, funcion: str, *args, cache: bool = True, **kwargs) -> tuple[Any, Optional[str], float]:
    """Llama ``modulo.funcion(*args)``. Devuelve (resultado, error, segundos); nunca lanza excepción."""
    m = modulo(nombre_modulo)
    if m is None:
        return None, f"Módulo {nombre_modulo} no disponible ({error_import(nombre_modulo)})", 0.0
    if not hasattr(m, funcion):
        return None, f"{nombre_modulo}.{funcion} aún no existe", 0.0
    t0 = time.perf_counter()
    try:
        if cache:
            r = _llamar_cache(nombre_modulo, funcion, *args, **kwargs)
        else:
            r = getattr(m, funcion)(*args, **kwargs)
        return r, None, time.perf_counter() - t0
    except Exception as e:  # noqa: BLE001
        return None, f"{type(e).__name__}: {str(e)[:500]}", time.perf_counter() - t0


# Funciones de B que consultan MongoDB: si Mongo está caído se avisa sin esperar el timeout de cada una.
FUNCIONES_MONGO_B = {"kpis_ambiente", "kpis_iot_mongo", "serie_temporal", "comparativa_zonas", "distribucion_tipos",
                     "sensores_mongo", "lecturas_mongo", "incidentes", "eventos", "alertas", "tipos_evento",
                     "ids_sensores", "kpis_incidentes"}


def llamar_b(funcion: str, *args, **kwargs):
    if funcion in FUNCIONES_MONGO_B and estado_servicios().get("MongoDB") is False:
        return None, "MongoDB sin conexión (ver estado en la barra lateral)", 0.0
    return llamar(MODULO_B, funcion, *args, **kwargs)


def llamar_a(funcion: str, *args, **kwargs):
    return llamar(MODULO_A, funcion, *args, **kwargs)


@st.cache_data(ttl=30, show_spinner=False)
def estado_servicios() -> dict:
    """Estado de las 4 tecnologías (caché 30 s). Las de A quedan en None si su módulo no existe."""
    from contrato import FUENTE_HDFS, FUENTE_MONGO, FUENTE_POSTGRES, FUENTE_SPARK
    salida = {FUENTE_POSTGRES: None, FUENTE_MONGO: None, FUENTE_HDFS: None, FUENTE_SPARK: None,
              "latencia_ms": {}, "detalle": {}}
    for nombre in (MODULO_B, MODULO_A):
        m = modulo(nombre)
        if m is None or not hasattr(m, "estado"):
            continue
        try:
            e = m.estado()
        except Exception as ex:  # noqa: BLE001
            salida["detalle"][nombre] = str(ex)
            continue
        for clave in (FUENTE_POSTGRES, FUENTE_MONGO, FUENTE_HDFS, FUENTE_SPARK):
            if clave in e:
                salida[clave] = bool(e[clave])
        salida["latencia_ms"].update(e.get("latencia_ms", {}) or {})
        salida["detalle"].update(e.get("detalle", {}) or {})
    return salida
