"""Reto adicional: streaming en vivo (Spark Structured Streaming -> MongoDB). Se actualiza cada 5 s."""
from __future__ import annotations

import plotly.express as px
import streamlit as st

from contrato import FUENTE_HDFS, FUENTE_MONGO, FUENTE_SPARK, ZONAS

from . import componentes as ui
from . import fuentes


def mostrar() -> None:
    st.title("Streaming en vivo (reto adicional)")
    st.caption("Lecturas simuladas llegan a HDFS (/smartcity/streaming/entrada); Spark Structured Streaming las "
               "agrupa en ventanas de 1 minuto por zona y, si el PM2.5 promedio supera 35,4 µg/m³ o el tráfico "
               "supera 1.200 vehículos/h, genera una alerta en MongoDB. Batch = todo el histórico una vez; "
               "streaming = solo lo nuevo, cada 10 s.")
    m = fuentes.b()
    if m is None:
        ui.aviso_error("Streaming", fuentes.error_import(fuentes.MODULO_B))
        return

    c1, c2, c3, c4 = st.columns([1, 1, 2, 1])
    if c1.button("Iniciar job", width="stretch"):
        st.toast(m.streaming_iniciar(900))
    if c2.button("Detener job", width="stretch"):
        st.toast(m.streaming_detener())
    episodio = c3.selectbox("Simular episodio de contaminación en", ["(ninguna)"] + ZONAS, key="stream_episodio")
    if c4.button("Enviar lote", type="primary", width="stretch"):
        try:
            ruta = m.stream_enviar_lote(episodio=None if episodio == "(ninguna)" else episodio)
            st.toast(f"Lote enviado: {ruta}")
        except Exception as e:  # noqa: BLE001
            ui.aviso_error("Productor", str(e))
    st.caption("Para un flujo continuo: `python scripts/streaming_productor.py --lotes 60 --intervalo 5 "
               "--episodio \"Zona Centro\"`")
    _en_vivo()


@st.fragment(run_every=5)
def _en_vivo() -> None:
    m = fuentes.b()
    try:
        activo = m.streaming_activo()
    except Exception:  # noqa: BLE001
        activo = None
    st.markdown(f"**Job de streaming:** {'🟢 corriendo' if activo else '⚪ detenido'}")
    try:
        ventanas = m.stream_ventanas(30)
        alertas = m.stream_alertas(30)
    except Exception as e:  # noqa: BLE001
        ui.aviso_error("Streaming", str(e))
        return
    c1, c2 = st.columns(2)
    with c1:
        st.markdown("**PM2.5 promedio por ventana de 1 minuto**")
        if ventanas.empty:
            ui.vacio("Aún no hay ventanas: iniciar el job y enviar lotes.")
        else:
            fig = px.line(ventanas, x="inicio", y="pm25_promedio", color="zona", markers=True,
                          color_discrete_map=ui.COLORES_ZONA, labels={"inicio": "", "pm25_promedio": "PM2.5 (µg/m³)"})
            fig.add_hline(y=35.4, line_dash="dot", annotation_text="umbral 35,4")
            fig.update_layout(height=320, margin=dict(l=10, r=10, t=10, b=10))
            st.plotly_chart(fig, width="stretch")
        ui.etiqueta_origen([FUENTE_SPARK, FUENTE_MONGO], "Structured Streaming · colección stream_ventana")
    with c2:
        st.markdown("**Alertas generadas en streaming**")
        if alertas.empty:
            ui.vacio("Sin alertas de streaming todavía.")
        else:
            st.dataframe(alertas, hide_index=True, width="stretch", height=320)
        ui.etiqueta_origen([FUENTE_SPARK, FUENTE_MONGO, FUENTE_HDFS], "alerta (origen = streaming) · histórico en HDFS")
