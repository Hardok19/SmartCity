"""SmartCity Analytics — dashboard Streamlit (D).

    streamlit run dashboard.py

La UI no contiene SQL ni pipelines: solo importa conectores_mongo_spark (pareja B) y, cuando exista,
conectores_postgres_hadoop (pareja A). Ver dashboard_app/fuentes.py para el contrato de funciones.
"""
import streamlit as st

st.set_page_config(page_title="SmartCity Analytics", page_icon="🌆", layout="wide")

from contrato import FUENTES_VALIDAS  # noqa: E402

from dashboard_app import componentes as ui  # noqa: E402
from dashboard_app import fuentes, pagina_comparacion, pagina_consultas, pagina_principal, pagina_streaming  # noqa: E402
from dashboard_app.filtros import barra_lateral  # noqa: E402

PAGINAS = {
    "Vista principal": None,
    "Consultas por tecnología": pagina_consultas.mostrar,
    "Streaming en vivo": pagina_streaming.mostrar,
    "Comparación tecnológica": pagina_comparacion.mostrar,
}


def estado_lateral() -> None:
    st.sidebar.markdown("### Estado de servicios")
    est = fuentes.estado_servicios()
    for tec in FUENTES_VALIDAS:
        valor = est.get(tec)
        icono = "🟢" if valor else ("🔴" if valor is False else "⚪")
        texto = "en línea" if valor else ("sin conexión" if valor is False else "módulo de la pareja A pendiente")
        lat = est["latencia_ms"].get(tec)
        color = ui.COLORES[tec]
        st.sidebar.markdown(
            f"{icono} <span style='color:{color};font-weight:700'>{tec}</span> — {texto}"
            f"{f' · {lat:.0f} ms' if lat is not None and valor else ''}",
            unsafe_allow_html=True, help=est["detalle"].get(tec))
    if st.sidebar.button("Actualizar datos", width="stretch"):
        st.cache_data.clear()
        st.rerun()


def main() -> None:
    pagina = st.sidebar.radio("Sección", list(PAGINAS), label_visibility="collapsed")
    estado_lateral()
    if pagina == "Vista principal":
        sel = barra_lateral()
        pagina_principal.mostrar(sel)
    else:
        PAGINAS[pagina]()


main()
