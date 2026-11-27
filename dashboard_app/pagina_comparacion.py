"""Comparación tecnológica (D7) — tarea AB, se completa con los experimentos C1–C3.

Por ahora muestra los resultados de experimentos que ya existan como archivos en docs/resultados/.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd
import streamlit as st

from contrato import FUENTE_MONGO, FUENTE_POSTGRES, FUENTE_SPARK

from . import componentes as ui

RESULTADOS = Path("docs/resultados")


def mostrar() -> None:
    st.title("Comparación tecnológica")
    st.info("Sección compartida por ambas parejas (D7, C1–C3): matriz comparativa, gráficos de los experimentos "
            "E1–E7, diagrama de arquitectura y respuestas a las 7 preguntas del profesor. Se completa cuando "
            "estén los experimentos.", icon="🧩")

    benchmarks = RESULTADOS / "benchmarks.csv"
    if benchmarks.exists():
        st.subheader("Experimentos E1–E7")
        ui.tabla(pd.read_csv(benchmarks), "benchmarks")
    else:
        ui.vacio("Aún no existe docs/resultados/benchmarks.csv (C1).")

    indices = RESULTADOS / "mongo_indices.csv"
    if indices.exists():
        st.subheader("E7 · Índices en MongoDB (B2.5)")
        df = pd.read_csv(indices)
        ui.grafico_barras(df, "consulta", "docs", color="variante", etiqueta_y="documentos examinados")
        ui.tabla(df, "mongo_indices")
        ui.etiqueta_origen(FUENTE_MONGO)

    optimizacion = RESULTADOS / "spark_optimizacion.csv"
    if optimizacion.exists():
        st.subheader("Optimización Spark (B4.3)")
        df = pd.read_csv(optimizacion)
        ui.tabla(df, "spark_opt")
        ui.etiqueta_origen(FUENTE_SPARK)

    pg = RESULTADOS / "postgres_optimizacion.csv"
    if pg.exists():
        st.subheader("Optimización PostgreSQL (A2.5)")
        ui.tabla(pd.read_csv(pg), "pg_opt")
        ui.etiqueta_origen(FUENTE_POSTGRES)
