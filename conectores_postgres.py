"""SmartCity Analytics - conector de PostgreSQL (Pareja A).

Interfaz independiente para que el integrador la importe sin modificar la rama
MongoDB/Spark. Requiere pandas, psycopg2-binary y el contrato.py del repositorio.

La base contiene lecturas sintéticas de prueba. No atribuirlas a OpenAQ/Open-Meteo.
Para trabajar en Docker usar PGHOST=127.0.0.1 cuando se comparte la red del
contenedor de PostgreSQL, o PGHOST=postgres dentro de docker_default.
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any, Mapping

import pandas as pd
import psycopg2

from contrato import (
    COLUMNAS_ESTANDAR,
    FUENTE_POSTGRES,
    normalizar_timestamp,
    ordenar_columnas_estandar,
    validar_tipo_metrica,
    validar_zonas,
)

RAIZ = Path(__file__).resolve().parent
ARCHIVO_CONSULTAS = RAIZ / "sql" / "queries" / "consultas_obligatorias.sql"


def conexion():
    """Conexión real (no mock); se cierra al finalizar cada consulta."""
    return psycopg2.connect(
        host=os.getenv("PGHOST", "127.0.0.1"),
        port=int(os.getenv("PGPORT", "5432")),
        dbname=os.getenv("PGDATABASE", "smartcity_db"),
        user=os.getenv("PGUSER", "smartcity"),
        password=os.getenv("PGPASSWORD", "smartcity123"),
        connect_timeout=8,
    )


def ejecutar_sql(sql: str, params: tuple | list | None = None) -> pd.DataFrame:
    """Consulta parametrizada SQL -> DataFrame, sin mantener conexiones abiertas."""
    con = conexion()
    try:
        with con.cursor() as cur:
            cur.execute(sql, tuple(params or ()))
            if cur.description is None:
                return pd.DataFrame()
            columns = [c[0] for c in cur.description]
            return pd.DataFrame(cur.fetchall(), columns=columns)
    finally:
        con.close()


def estado() -> dict[str, Any]:
    """Resultado simple para el dashboard."""
    try:
        df = ejecutar_sql("SELECT current_database() AS base, version() AS version")
        return {"PostgreSQL": True, "base": df.at[0, "base"]}
    except (psycopg2.Error, OSError) as exc:
        return {"PostgreSQL": False, "error": str(exc)}


def kpis_iot(filtros: Any = None) -> dict[str, int]:
    """Indicadores agregados (no confundir sensores activos con sensores que han enviado)."""
    sensores = ejecutar_sql(
        "SELECT COUNT(*) AS total, "
        "COUNT(*) FILTER (WHERE estado='activo') AS activos, "
        "COUNT(*) FILTER (WHERE estado='inactivo') AS inactivos FROM sensores"
    ).iloc[0]
    lecturas = ejecutar_sql("SELECT COUNT(*) AS mediciones FROM medicion_horaria").iloc[0]
    con_lecturas = ejecutar_sql(
        "SELECT COUNT(DISTINCT id_sensor) AS con_lecturas FROM medicion_horaria"
    ).iloc[0]
    return {
        "sensores": int(sensores["total"]),
        "activos": int(sensores["activos"]),
        "inactivos": int(sensores["inactivos"]),
        "mediciones": int(lecturas["mediciones"]),
        "sensores_con_lecturas": int(con_lecturas["con_lecturas"]),
    }


def sensores(filtros: Any = None) -> pd.DataFrame:
    """Catálogo geográfico para tabla y mapa de Streamlit."""
    df = ejecutar_sql(
        "SELECT id_sensor,nombre,tipo_sensor,estado,zona,lat,lon,categoria,organismo "
        "FROM v_sensor_detalle ORDER BY zona,id_sensor"
    )
    if not df.empty:
        validar_zonas(df)
    return df


def lecturas_pg(filtros: Any = None, *, limite: int = 1000) -> pd.DataFrame:
    """PM2.5 -> contrato estándar de seis columnas.

    'tipo_metrica=calidad_aire' y 'valor=PM2.5' son decisiones del puente de
    contrato de la pareja B. Clima se consulta por separado en kpis_ambiente().
    Recibe filtros como dict o dataclass con zonas, fecha_inicio y fecha_fin.
    """
    if not isinstance(limite, int) or not 1 <= limite <= 50000:
        raise ValueError("limite debe ser un entero entre 1 y 50000")

    def valor(nombre: str):
        return filtros.get(nombre) if isinstance(filtros, Mapping) else getattr(filtros, nombre, None)

    clauses = ["m.variable='pm25'", "s.tipo_sensor='calidad_aire'"]
    params: list[Any] = []

    zonas = valor("zonas") if filtros is not None else None
    if zonas:
        clauses.append("u.nombre_zona = ANY(%s)")
        params.append(list(zonas))
    fi = valor("fecha_inicio") if filtros is not None else None
    ff = valor("fecha_fin") if filtros is not None else None
    if fi is not None:
        clauses.append("m.timestamp >= %s")
        params.append(fi)
    if ff is not None:
        clauses.append("m.timestamp < %s")
        params.append(ff)

    sql = (
        "SELECT u.nombre_zona AS zona, m.timestamp, m.id_sensor, "
        "'calidad_aire'::text AS tipo_metrica, m.valor, "
        "'PostgreSQL'::text AS fuente "
        "FROM medicion_horaria m JOIN sensores s USING(id_sensor) "
        "JOIN ubicaciones u USING(id_ubicacion) "
        f"WHERE {' AND '.join(clauses)} "
        "ORDER BY m.timestamp DESC, m.id_sensor LIMIT %s"
    )
    df = ejecutar_sql(sql, [*params, limite])
    if df.empty:
        return pd.DataFrame(columns=COLUMNAS_ESTANDAR)
    df["timestamp"] = normalizar_timestamp(df["timestamp"])
    validar_zonas(df)
    validar_tipo_metrica(df)
    return ordenar_columnas_estandar(df)


def kpis_ambiente() -> pd.DataFrame:
    """Promedio de variables por zona; muestra naturaleza sintética en interfaz."""
    return ejecutar_sql(
        "SELECT u.nombre_zona AS zona, "
        "ROUND(AVG(m.valor) FILTER (WHERE m.variable='pm25')::numeric,2) AS pm25_promedio, "
        "ROUND(AVG(m.valor) FILTER (WHERE m.variable='temperatura')::numeric,2) AS temperatura_promedio, "
        "ROUND(AVG(m.valor) FILTER (WHERE m.variable='humedad')::numeric,2) AS humedad_promedio, "
        "COUNT(m.valor) AS total_mediciones "
        "FROM ubicaciones u LEFT JOIN sensores s USING(id_ubicacion) "
        "LEFT JOIN medicion_horaria m USING(id_sensor) "
        "GROUP BY u.nombre_zona ORDER BY u.nombre_zona"
    )


def consultas_disponibles() -> dict[str, dict[str, str]]:
    """Extrae PG-Q01..PG-Q10 de su archivo SQL verdadero: evita duplicarlos."""
    if not ARCHIVO_CONSULTAS.exists():
        raise FileNotFoundError(f"No existe {ARCHIVO_CONSULTAS}")
    raw = ARCHIVO_CONSULTAS.read_text(encoding="utf-8")
    patron = re.compile(r"(?ms)^--\s*(PG-Q\d{2})\s+([^\n]+)\n(.*?)(?=^--\s*PG-(?:Q\d{2}|extra)|\Z)")
    reg = {}
    for ident, titulo, sql in patron.findall(raw):
        reg[ident] = {"titulo": titulo.strip(), "codigo": sql.strip().rstrip(";")}
    faltantes = {f"PG-Q{i:02d}" for i in range(1, 11)} - set(reg)
    if faltantes:
        raise ValueError(f"No se localizaron consultas obligatorias: {sorted(faltantes)}")
    return reg


def ejecutar_consulta(consulta_id: str) -> pd.DataFrame:
    """Ejecuta únicamente los ID previamente registrados, no SQL arbitrario del UI."""
    registro = consultas_disponibles()
    if consulta_id not in registro:
        raise ValueError(f"Consulta no permitida: {consulta_id}")
    return ejecutar_sql(registro[consulta_id]["codigo"])


# Alias amigables si el integrador espera estos nombres.
CONSULTAS_PG = consultas_disponibles
