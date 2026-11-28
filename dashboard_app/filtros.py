"""Filtros globales de la barra lateral (D3) -> comun.Filtros + variable a graficar."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import streamlit as st
import yaml

from comun import ETIQUETAS_PERFIL, Filtros
from contrato import FUENTES_VALIDAS, ZONAS

from . import fuentes

# Variables medidas (tipo de medición): mismos códigos que contrato.TIPOS_METRICA (D-01).
VARIABLES = {
    "pm25": "PM2.5 (µg/m³)", "pm10": "PM10 (µg/m³)", "no2": "NO₂ (µg/m³)", "o3": "O₃ (µg/m³)", "co": "CO (µg/m³)",
    "co2": "CO₂ (ppm)", "temperatura": "Temperatura (°C)", "humedad": "Humedad (%)",
    "precipitacion": "Precipitación (mm)", "viento": "Viento (km/h)",
}
VARIABLES_AIRE = {"pm25", "pm10", "no2", "o3", "co", "co2"}
TODAS = "todas"                     # sin filtro de tipo; los gráficos de variable muestran PM2.5
VARIABLE_POR_DEFECTO = "pm25"


@dataclass(frozen=True)
class Seleccion:
    filtros: Filtros
    variable: str


@st.cache_data(show_spinner=False)
def _periodo() -> tuple[date, date]:
    with open("config/zonas.yaml", encoding="utf-8") as fh:
        p = yaml.safe_load(fh)["periodo"]
    return date.fromisoformat(str(p["inicio"])), date.fromisoformat(str(p["fin"]))


def barra_lateral() -> Seleccion:
    st.sidebar.markdown("### Filtros")
    inicio, fin = _periodo()
    rango = st.sidebar.date_input("Rango de fechas (UTC)", value=(inicio, fin), min_value=inicio, max_value=fin,
                                  format="DD/MM/YYYY")
    f_ini, f_fin = (rango if isinstance(rango, (tuple, list)) and len(rango) == 2 else (inicio, fin))
    perfil = st.sidebar.selectbox("Perfil horario", list(ETIQUETAS_PERFIL), format_func=ETIQUETAS_PERFIL.get)
    zonas = st.sidebar.multiselect("Zona(s)", ZONAS, default=ZONAS)
    eleccion = st.sidebar.selectbox("Tipo de medición", [TODAS, *VARIABLES],
                                    format_func=lambda v: "Todas las mediciones" if v == TODAS else VARIABLES[v],
                                    help="Filtra KPIs, tabla y mapa por la métrica elegida. Con «Todas», los "
                                         "gráficos por variable muestran PM2.5.")
    variable = VARIABLE_POR_DEFECTO if eleccion == TODAS else eleccion

    with st.sidebar.expander("Más filtros"):
        sensores, _, _ = fuentes.llamar_b("ids_sensores")
        id_sensor = st.selectbox("Sensor", ["(todos)"] + list(sensores or []))
        tipos_ev, _, _ = fuentes.llamar_b("tipos_evento")
        tipo_evento = st.selectbox("Tipo de evento", ["(todos)"] + list(tipos_ev or []))
        fuente = st.selectbox("Fuente", ["(todas)"] + FUENTES_VALIDAS)
        usar_valor = st.checkbox("Filtrar por rango de valores")
        vmin = vmax = None
        if usar_valor:
            c1, c2 = st.columns(2)
            vmin = c1.number_input("Mínimo", value=0.0)
            vmax = c2.number_input("Máximo", value=100.0)

    filtros = Filtros(
        fecha_inicio=f_ini, fecha_fin=f_fin, perfil_horario=perfil,
        zonas=tuple(zonas) if zonas and len(zonas) < len(ZONAS) else (),
        tipos_metrica=() if eleccion == TODAS else (eleccion,),
        id_sensor=None if id_sensor == "(todos)" else id_sensor,
        tipo_evento=None if tipo_evento == "(todos)" else tipo_evento,
        fuente=None if fuente == "(todas)" else fuente,
        valor_min=vmin, valor_max=vmax,
    )
    return Seleccion(filtros=filtros, variable=variable)


def visible(filtros: Filtros, fuente: str) -> bool:
    """Filtro «Fuente»: oculta los elementos de otras tecnologías."""
    return filtros.fuente is None or filtros.fuente == fuente
