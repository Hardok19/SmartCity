"""C3 — Gráficos de los experimentos E1–E7 (Plotly), a partir de docs/resultados/benchmarks.csv.

Sin dependencias de Streamlit en las figuras: lo usan la página «Comparación tecnológica» y
scripts/graficos_benchmarks.py (PNG para el documento). Convenciones:
* El color identifica la tecnología (los mismos colores en todo el dashboard); una segunda variante de la misma
  tecnología (sin índice, 1 núcleo, $lookup por documento) se distingue con trama, no con otro color.
* Los tiempos van de milisegundos a minutos: el eje Y es logarítmico y lo dice su título.
* Etiquetas directas en las barras y al final de cada línea, además de la leyenda.
"""
from __future__ import annotations

import json
import math
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go

from contrato import FUENTE_HDFS, FUENTE_MONGO, FUENTE_POSTGRES, FUENTE_SPARK

CSV = Path(__file__).resolve().parents[1] / "docs" / "resultados" / "benchmarks.csv"
COLORES = {FUENTE_POSTGRES: "#336791", FUENTE_MONGO: "#47A248", FUENTE_HDFS: "#F5B800", FUENTE_SPARK: "#E25A1C"}
TINTA, TINTA_2, REJILLA = "#1d1d1f", "#5f6368", "#e6e8eb"


def cargar(ruta: Path = CSV) -> pd.DataFrame:
    if not ruta.exists():
        return pd.DataFrame()
    df = pd.read_csv(ruta, dtype={"escala_filas": "Int64"})
    df = df[df["experimento"] != "INFO"].copy()
    df["detalle"] = df["detalle"].map(lambda t: json.loads(t) if isinstance(t, str) and t else {})
    return df


def entorno(ruta: Path = CSV) -> dict:
    if not ruta.exists():
        return {}
    df = pd.read_csv(ruta)
    fila = df[df["experimento"] == "INFO"]
    return json.loads(fila["detalle"].iloc[0]) if len(fila) else {}


def escala_txt(n) -> str:
    n = int(n)
    return f"{n // 1_000_000} M" if n % 1_000_000 == 0 else f"{n // 1000}k"


def tiempo_txt(s: float) -> str:
    if s < 1:
        return f"{s * 1000:.1f} ms".replace(".", ",")
    if s < 100:
        return f"{s:.2f} s".replace(".", ",")
    return f"{s:,.0f} s".replace(",", ".")


def _base(fig: go.Figure, titulo_y: str, alto: int = 380, log: bool = True) -> go.Figure:
    fig.update_layout(
        height=alto, margin=dict(l=10, r=10, t=40, b=10), plot_bgcolor="white", paper_bgcolor="white",
        font=dict(color=TINTA, size=12), legend=dict(orientation="h", y=1.08, x=0, title_text=""),
        separators=",.", bargap=0.25, bargroupgap=0.08, hoverlabel=dict(bgcolor="white"))
    fig.update_yaxes(title_text=titulo_y, type="log" if log else "linear", gridcolor=REJILLA, zeroline=False,
                     tickfont=dict(color=TINTA_2))
    fig.update_xaxes(showgrid=False, tickfont=dict(color=TINTA_2), linecolor=REJILLA)
    return fig


def _barras(df: pd.DataFrame, series: list[tuple[str, str, str, str]], titulo_y="segundos (escala logarítmica)",
            log=True) -> go.Figure:
    """series = [(tecnologia, variante, nombre en la leyenda, trama '' | '/')] ; x = escala."""
    fig = go.Figure()
    escalas = sorted(df["escala_filas"].dropna().unique())
    for tec, var, nombre, trama in series:
        d = df[(df["tecnologia"] == tec) & (df["variante"] == var)].set_index("escala_filas")
        if d.empty:
            continue
        y = [float(d.loc[e, "segundos"]) if e in d.index else None for e in escalas]
        fig.add_bar(x=[escala_txt(e) for e in escalas], y=y, name=nombre,
                    marker=dict(color=COLORES[tec], line=dict(color="white", width=2),
                                pattern=dict(shape=trama, fgcolor="white", size=6, solidity=0.35) if trama else None),
                    text=[tiempo_txt(v) if v is not None else "" for v in y], textposition="outside",
                    textfont=dict(color=TINTA, size=11), cliponaxis=False,
                    hovertemplate=f"<b>{nombre}</b><br>%{{x}} filas<br>%{{text}}<extra></extra>")
    fig.update_xaxes(title_text="filas")
    return _base(fig, titulo_y, log=log)


def fig_e1(df: pd.DataFrame) -> go.Figure:
    """Línea por motor: tiempo de la agregación zona-hora según la escala."""
    d = df[df["experimento"] == "E1"]
    series = [(FUENTE_POSTGRES, "group_by_sql", "PostgreSQL (GROUP BY)"),
              (FUENTE_MONGO, "aggregate_group", "MongoDB ($group)"),
              (FUENTE_SPARK, "dataframe_cores2", "Spark (DataFrame, 2 núcleos)")]
    fig = go.Figure()
    finales = []                                   # (log10 segundos, log10 filas, texto) del último punto
    for tec, var, nombre in series:
        s = d[(d["tecnologia"] == tec) & (d["variante"] == var)].sort_values("escala_filas")
        if s.empty:
            continue
        x = s["escala_filas"].astype(float)
        fig.add_scatter(x=x, y=s["segundos"], name=nombre, mode="lines+markers",
                        line=dict(color=COLORES[tec], width=2), marker=dict(size=9, line=dict(color="white", width=2)),
                        customdata=[tiempo_txt(v) for v in s["segundos"]],
                        hovertemplate=f"<b>{nombre}</b><br>%{{x:,.0f}} filas<br>%{{customdata}}<extra></extra>")
        ultimo = float(s["segundos"].iloc[-1])
        finales.append([math.log10(ultimo), math.log10(float(x.iloc[-1])), f"{tec}: {tiempo_txt(ultimo)}"])
    # En ejes logarítmicos Plotly ubica las anotaciones en log10; se separan las que quedarían encimadas.
    finales.sort()
    for i in range(1, len(finales)):
        finales[i][0] = max(finales[i][0], finales[i - 1][0] + 0.16)
    for y_log, x_log, texto in finales:
        fig.add_annotation(x=x_log, y=y_log, xref="x", yref="y", text=texto, showarrow=False,
                           xanchor="left", xshift=10, font=dict(color=TINTA, size=11))
    fig.update_xaxes(type="log", title_text="filas (escala logarítmica)", tickvals=[1e5, 1e6, 5e6],
                     ticktext=["100k", "1 M", "5 M"])
    fig = _base(fig, "segundos (escala logarítmica)")
    fig.update_layout(margin=dict(r=150))
    return fig


def fig_e2(df: pd.DataFrame) -> go.Figure:
    d = df[df["experimento"] == "E2"]
    return _barras(d, [(FUENTE_POSTGRES, "con_indice", "PostgreSQL con índice", ""),
                       (FUENTE_MONGO, "con_indice", "MongoDB con índice", ""),
                       (FUENTE_SPARK, "full_scan_parquet", "Spark (recorre el Parquet)", ""),
                       (FUENTE_SPARK, "job_completo_con_arranque", "Spark incluyendo arrancar el job", "/")])


def fig_e3(df: pd.DataFrame) -> go.Figure:
    d = df[df["experimento"] == "E3"]
    return _barras(d, [(FUENTE_POSTGRES, "join_sql", "PostgreSQL JOIN (5 tablas)", ""),
                       (FUENTE_MONGO, "lookup_tras_agrupar", "MongoDB: agrupar y luego $lookup", ""),
                       (FUENTE_MONGO, "lookup_por_documento", "MongoDB: $lookup por documento", "/"),
                       (FUENTE_SPARK, "join_broadcast_cores2", "Spark join broadcast", "")])


def fig_e4(df: pd.DataFrame) -> go.Figure:
    d = df[df["experimento"] == "E4"].copy()
    nombres = {
        "pg_insert_tipo_sensor": "PG: INSERT en tipos_sensor (FK)",
        "pg_add_column_nullable": "PG: ADD COLUMN sin valor por defecto",
        "pg_add_column_default_constante": "PG: ADD COLUMN DEFAULT constante",
        "pg_add_column_default_volatil": "PG: ADD COLUMN DEFAULT volátil (reescribe la tabla)",
        "pg_add_column_jsonb": "PG: ADD COLUMN JSONB",
        "pg_insert_10k_nuevo_tipo": "PG: insertar 10.000 lecturas nuevas",
        "insert_10k_campos_nuevos": "Mongo: insertar 10.000 documentos con campos nuevos",
        "consulta_exists_campo_nuevo": "Mongo: consultar el campo nuevo ($exists)",
    }
    d["nombre"] = d["variante"].map(nombres).fillna(d["variante"])
    d = d.iloc[::-1]
    fig = go.Figure()
    for tec in (FUENTE_MONGO, FUENTE_POSTGRES):
        s = d[d["tecnologia"] == tec]
        fig.add_bar(y=s["nombre"], x=s["segundos"], orientation="h", name=tec,
                    marker=dict(color=COLORES[tec], line=dict(color="white", width=2)),
                    text=[tiempo_txt(v) for v in s["segundos"]], textposition="outside", cliponaxis=False,
                    textfont=dict(color=TINTA, size=11),
                    hovertemplate="<b>%{y}</b><br>%{text}<extra></extra>")
    escala = d["escala_filas"].dropna()
    fig = _base(fig, "", alto=420)
    fig.update_xaxes(type="log", title_text=f"segundos (escala logarítmica) · tabla de "
                                            f"{escala_txt(escala.iloc[0]) if len(escala) else '?'} filas",
                     showgrid=True, gridcolor=REJILLA)
    fig.update_yaxes(type="category", title_text="", gridcolor="white")
    fig.update_layout(margin=dict(l=10, r=70))
    return fig


def fig_e5(df: pd.DataFrame) -> go.Figure:
    d = df[df["experimento"] == "E5"]
    fig = _barras(d, [(FUENTE_SPARK, "cores1", "Spark, 1 núcleo", "/"), (FUENTE_SPARK, "cores2", "Spark, 2 núcleos", "")],
                  titulo_y="segundos", log=False)
    return fig


def fig_e6(df: pd.DataFrame) -> go.Figure:
    d = df[df["experimento"] == "E6"]
    nombres = {"lectura_con_nodo_caido": "Lectura completa con el nodo apagado",
               "reingreso_nodo": "El nodo vuelve a reportarse al encenderlo",
               "deteccion_nodo_muerto": "El NameNode declara muerto el DataNode",
               "re_replicacion_completa": "Todos los bloques vuelven a tener 2 réplicas"}
    orden = list(nombres)
    d = d.set_index("variante").reindex([o for o in orden if o in set(d["variante"])]).reset_index()
    fig = go.Figure(go.Bar(y=[nombres[v] for v in d["variante"]], x=d["segundos"], orientation="h",
                           marker=dict(color=COLORES[FUENTE_HDFS], line=dict(color="white", width=2)),
                           text=[tiempo_txt(v) for v in d["segundos"]], textposition="outside", cliponaxis=False,
                           textfont=dict(color=TINTA, size=11), name="HDFS",
                           hovertemplate="<b>%{y}</b><br>%{text} después de apagar el nodo<extra></extra>"))
    fig = _base(fig, "", alto=300, log=False)
    fig.update_xaxes(title_text="segundos desde que se apaga smartcity_datanode2", showgrid=True, gridcolor=REJILLA)
    fig.update_yaxes(type="category", autorange="reversed", gridcolor="white")
    fig.update_layout(showlegend=False, margin=dict(r=60))
    return fig


def fig_e7(df: pd.DataFrame) -> go.Figure:
    d = df[df["experimento"] == "E7"]
    return _barras(d, [(FUENTE_POSTGRES, "con_indice", "PostgreSQL con índice", ""),
                       (FUENTE_POSTGRES, "sin_indice", "PostgreSQL sin índice", "/"),
                       (FUENTE_MONGO, "con_indice", "MongoDB con índice", ""),
                       (FUENTE_MONGO, "sin_indice", "MongoDB sin índice", "/")])


FIGURAS = {
    "E1": ("Agregación promedio por zona-hora", fig_e1),
    "E2": ("Búsqueda puntual: últimas 100 lecturas de un sensor", fig_e2),
    "E3": ("Join sensor → tipo → categoría → ubicación → organismo", fig_e3),
    "E4": ("Nuevo tipo de sensor con campos nuevos", fig_e4),
    "E5": ("Spark con 1 frente a 2 núcleos (agregación E1)", fig_e5),
    "E6": ("HDFS con un DataNode caído", fig_e6),
    "E7": ("Índices: con y sin índice (búsqueda puntual)", fig_e7),
}


def mediana(df: pd.DataFrame, exp: str, tec: str, var: str, escala=None):
    d = df[(df["experimento"] == exp) & (df["tecnologia"] == tec) & (df["variante"] == var)]
    if escala is not None:
        d = d[d["escala_filas"] == escala]
    return float(d["segundos"].iloc[0]) if len(d) else None
