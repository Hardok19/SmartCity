"""conectores_postgres.py — Capa de compatibilidad (versión anterior del módulo de la pareja A).

El módulo oficial es conectores_postgres_hadoop.py (PostgreSQL + HDFS + MapReduce). Este archivo conserva
los nombres que usan demo/dashboard_postgres_demo.py y scripts/probar_conector_postgres.py.
"""
from __future__ import annotations

import pandas as pd

from conectores_postgres_hadoop import (  # noqa: F401  (re-exportados)
    CONSULTAS_PG, ejecutar_sql, estado, kpis_ambiente, kpis_iot, lecturas_pg, sensores,
)


def consultas_disponibles() -> dict[str, dict[str, str]]:
    """{id: {"titulo", "codigo"}} de las consultas PG-Q01…Q10 y PG-P01…P07."""
    return {i: {"titulo": c.titulo, "codigo": c.codigo} for i, c in CONSULTAS_PG.items()}


def ejecutar_consulta(consulta_id: str, **params) -> pd.DataFrame:
    """Ejecuta únicamente consultas registradas (nunca SQL arbitrario de la UI)."""
    if consulta_id not in CONSULTAS_PG:
        raise ValueError(f"Consulta no permitida: {consulta_id}")
    return CONSULTAS_PG[consulta_id].ejecutar(**params)
