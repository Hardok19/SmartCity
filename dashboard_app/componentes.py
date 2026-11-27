"""Componentes visuales reutilizables (D2). Cada KPI, gráfico, tabla y mapa lleva su etiqueta de origen."""
from __future__ import annotations

import math
from typing import Iterable, Optional

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from contrato import FUENTE_HDFS, FUENTE_MONGO, FUENTE_POSTGRES, FUENTE_SPARK

COLORES = {FUENTE_POSTGRES: "#336791", FUENTE_MONGO: "#47A248", FUENTE_HDFS: "#F5B800", FUENTE_SPARK: "#E25A1C"}
COLORES_ZONA = {"Zona Norte": "#4C78A8", "Zona Sur": "#F58518", "Zona Centro": "#E45756",
                "Zona Este": "#72B7B2", "Zona Oeste": "#54A24B"}


# ------------------------------------------------------------------ formato numérico (1.234.567,8)
def num(valor, decimales: int = 0, sufijo: str = "") -> str:
    if valor is None or (isinstance(valor, float) and math.isnan(valor)):
        return "—"
    texto = f"{float(valor):,.{decimales}f}".replace(",", "_").replace(".", ",").replace("_", ".")
    return f"{texto}{(' ' + sufijo) if sufijo else ''}"


# ------------------------------------------------------------------ etiqueta de origen (obligatoria)
def etiqueta_origen(fuente: str | Iterable[str], nota: str = "") -> None:
    fuentes = [fuente] if isinstance(fuente, str) else list(fuente)
    chips = " ".join(
        f"<span style='background:{COLORES.get(f, '#888')};color:white;padding:1px 8px;border-radius:10px;"
        f"font-size:0.72rem;font-weight:600'>{f}</span>" for f in fuentes)
    extra = f" <span style='font-size:0.72rem;opacity:0.75'>{nota}</span>" if nota else ""
    st.markdown(f"<div style='margin:-4px 0 6px 0'>Origen: {chips}{extra}</div>", unsafe_allow_html=True)


def tarjeta_kpi(titulo: str, valor: str, fuente: str, ayuda: Optional[str] = None, nota: str = "") -> None:
    color = COLORES.get(fuente, "#888")
    with st.container(border=True):
        st.markdown(
            f"<div style='font-size:0.8rem;opacity:0.8'>{titulo}</div>"
            f"<div style='font-size:1.55rem;font-weight:700;line-height:1.3'>{valor}</div>"
            f"<div style='font-size:0.68rem'><span style='color:{color};font-weight:700'>● {fuente}</span>"
            f"{(' · ' + nota) if nota else ''}</div>",
            unsafe_allow_html=True, help=ayuda)


def aviso_error(seccion: str, error: str) -> None:
    st.warning(f"**{seccion}** no disponible: {error}", icon="⚠️")


def pendiente_a(que: str) -> None:
    st.info(f"**Pendiente de la pareja A** — {que}. Aparecerá automáticamente cuando "
            "`conectores_postgres_hadoop.py` exponga la función correspondiente.", icon="🧩")


def vacio(mensaje: str = "Sin datos para los filtros seleccionados.") -> None:
    st.caption(f"ℹ️ {mensaje}")


# ------------------------------------------------------------------ gráficos Plotly
def _layout(fig: go.Figure, alto: int = 340) -> go.Figure:
    fig.update_layout(height=alto, margin=dict(l=10, r=10, t=30, b=10), legend_title_text="",
                      separators=",.")
    return fig


def grafico_serie(df: pd.DataFrame, x: str, y: str, color: Optional[str] = None, titulo: str = "",
                  etiqueta_y: str = "") -> None:
    fig = px.line(df, x=x, y=y, color=color, color_discrete_map=COLORES_ZONA, title=titulo,
                  labels={y: etiqueta_y or y, x: ""})
    st.plotly_chart(_layout(fig), width="stretch")


def grafico_barras(df: pd.DataFrame, x: str, y: str, color: Optional[str] = None, titulo: str = "",
                   etiqueta_y: str = "", horizontal: bool = False, texto: Optional[str] = None) -> None:
    mapa = COLORES_ZONA if color == "zona" or (color is None and x == "zona") else None
    fig = px.bar(df, x=y if horizontal else x, y=x if horizontal else y, color=color or (x if x == "zona" else None),
                 color_discrete_map=mapa, orientation="h" if horizontal else "v", title=titulo,
                 labels={y: etiqueta_y or y, x: ""}, text=texto)
    fig.update_layout(showlegend=bool(color and color != x))
    st.plotly_chart(_layout(fig), width="stretch")


def grafico_dona(df: pd.DataFrame, nombres: str, valores: str, titulo: str = "") -> None:
    fig = px.pie(df, names=nombres, values=valores, hole=0.55, title=titulo)
    fig.update_traces(textinfo="percent+label")
    st.plotly_chart(_layout(fig), width="stretch")


def grafico_heatmap(df: pd.DataFrame, x: str, y: str, z: str, titulo: str = "") -> None:
    tabla = df.pivot_table(index=y, columns=x, values=z, aggfunc="mean")
    fig = px.imshow(tabla, aspect="auto", color_continuous_scale="RdBu_r", origin="lower", title=titulo,
                    zmin=-1 if tabla.min().min() < 0 else None, zmax=1 if tabla.max().max() <= 1 else None)
    st.plotly_chart(_layout(fig, 380), width="stretch")


def grafico_automatico(df: pd.DataFrame, clave: str) -> None:
    """Gráfico rápido para resultados de consultas: 1ª columna de texto vs 1ª numérica."""
    if df is None or df.empty or len(df) > 2000:
        return
    texto = [c for c in df.columns if df[c].dtype == object and df[c].nunique() <= 60]
    fechas = [c for c in df.columns if pd.api.types.is_datetime64_any_dtype(df[c])]
    numericas = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c]) and not pd.api.types.is_bool_dtype(df[c])]
    if not numericas or not (texto or fechas):
        return
    y = st.selectbox("Columna a graficar", numericas, key=f"auto_y_{clave}")
    if fechas:
        fig = px.line(df.sort_values(fechas[0]), x=fechas[0], y=y, color=texto[0] if texto else None)
    else:
        fig = px.bar(df, x=texto[0], y=y, color=texto[1] if len(texto) > 1 else None, barmode="group")
    st.plotly_chart(_layout(fig), width="stretch")


# ------------------------------------------------------------------ tabla con búsqueda y paginación
def tabla(df: pd.DataFrame, clave: str, filas_por_pagina: int = 25) -> None:
    if df is None or df.empty:
        vacio()
        return
    c1, c2 = st.columns([3, 1])
    texto = c1.text_input("Buscar", key=f"buscar_{clave}", placeholder="Texto en cualquier columna…",
                          label_visibility="collapsed")
    datos = df
    if texto:
        mascara = df.astype(str).apply(lambda col: col.str.contains(texto, case=False, regex=False)).any(axis=1)
        datos = df[mascara]
    paginas = max(1, math.ceil(len(datos) / filas_por_pagina))
    pagina = c2.number_input("Página", 1, paginas, 1, key=f"pag_{clave}", label_visibility="collapsed")
    inicio = (pagina - 1) * filas_por_pagina
    st.dataframe(datos.iloc[inicio:inicio + filas_por_pagina], width="stretch", hide_index=True)
    st.caption(f"{num(len(datos))} filas · página {pagina} de {paginas}")


# ------------------------------------------------------------------ mapa pydeck
def mapa_sensores(df: pd.DataFrame, columna_valor: Optional[str] = "pm25") -> None:
    import pydeck as pdk
    datos = df.copy()
    datos["lat"] = pd.to_numeric(datos["lat"], errors="coerce")   # PostgreSQL DECIMAL -> float
    datos["lon"] = pd.to_numeric(datos["lon"], errors="coerce")
    datos = datos.dropna(subset=["lat", "lon"])
    if datos.empty:
        vacio("Sin sensores con coordenadas.")
        return
    datos["color"] = datos["zona"].map(lambda z: _rgb(COLORES_ZONA.get(z, "#888888"))) if "zona" in datos else [[120, 120, 120]] * len(datos)
    capas = [pdk.Layer("ScatterplotLayer", data=datos, get_position="[lon, lat]", get_fill_color="color",
                       get_radius=220, pickable=True, opacity=0.85)]
    if columna_valor and columna_valor in datos and datos[columna_valor].notna().any():
        calor = datos.dropna(subset=[columna_valor])
        capas.insert(0, pdk.Layer("HeatmapLayer", data=calor, get_position="[lon, lat]",
                                  get_weight=columna_valor, radius_pixels=70, opacity=0.55))
    vista = pdk.ViewState(latitude=float(datos["lat"].mean()), longitude=float(datos["lon"].mean()), zoom=9.6)
    tooltip = {"text": "{id_sensor} · {zona}\n" + (f"{columna_valor}: {{{columna_valor}}}" if columna_valor in datos else "")}
    st.pydeck_chart(pdk.Deck(layers=capas, initial_view_state=vista, tooltip=tooltip, map_style=None),
                    width="stretch")


def _rgb(hexa: str) -> list[int]:
    hexa = hexa.lstrip("#")
    return [int(hexa[i:i + 2], 16) for i in (0, 2, 4)]
