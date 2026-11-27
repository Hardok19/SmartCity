"""Demostración separada de PostgreSQL; NO sustituye el dashboard grupal."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import streamlit as st
from conectores_postgres import (estado,kpis_iot,sensores,lecturas_pg,
                                 kpis_ambiente,consultas_disponibles,ejecutar_consulta)

st.set_page_config(page_title="SmartCity - PostgreSQL",layout="wide")
st.title("SmartCity Analytics · PostgreSQL")
st.caption("Fabiana | Londres | mediciones sintéticas de prueba (2023–2024)")
health=estado()
if not health["PostgreSQL"]:
    st.error(f"PostgreSQL no disponible: {health.get('error','Error desconocido')}")
    st.stop()
st.success("PostgreSQL conectado: " + health["base"])
k=kpis_iot()
cols=st.columns(4)
for col,(titulo,numero) in zip(cols,[("Sensores",k["sensores"]),("Activos",k["activos"]),
                                ("Inactivos",k["inactivos"]),("Mediciones",k["mediciones"])]):
    col.metric(titulo, f"{numero:,}")

st.subheader("Sensores y ubicación")
cat=sensores()
if not cat.empty:
    mapa = cat.rename(columns={"lat": "latitude", "lon": "longitude"}).dropna(
        subset=["latitude", "longitude"]
    ).copy()
    # PostgreSQL DECIMAL devuelve decimal.Decimal; st.map necesita coordenadas float.
    mapa[["latitude", "longitude"]] = mapa[["latitude", "longitude"]].astype(float)
    st.map(mapa, latitude="latitude", longitude="longitude")
    st.dataframe(cat, use_container_width=True)

st.subheader("Indicadores ambientales")
st.caption("Datos de demostración sintéticos; no son resultados de sensores reales de OpenAQ")
st.dataframe(kpis_ambiente(),use_container_width=True)

st.subheader("Consultas obligatorias")
reg=consultas_disponibles()
op=st.selectbox("Seleccionar consulta PostgreSQL", list(reg), format_func=lambda i:f"{i}: {reg[i]['titulo']}")
with st.expander("Mostrar código SQL"):
    st.code(reg[op]["codigo"],language="sql")
st.dataframe(ejecutar_consulta(op),use_container_width=True)

st.subheader("Muestra del contrato de integración")
st.dataframe(lecturas_pg(limite=100),use_container_width=True)
