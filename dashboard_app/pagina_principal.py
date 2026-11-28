"""Vista principal (D4–D5): KPIs, 4 gráficos, mapa y tabla de detalle. Una sola pantalla con secciones."""
from __future__ import annotations

import pandas as pd
import streamlit as st

from contrato import FUENTE_HDFS, FUENTE_MONGO, FUENTE_POSTGRES, FUENTE_SPARK

from . import componentes as ui
from . import fuentes
from .filtros import VARIABLES, Seleccion, visible

UNIDAD = {"pm25": "µg/m³", "pm10": "µg/m³", "no2": "µg/m³", "o3": "µg/m³", "co": "µg/m³", "temperatura": "°C",
          "humedad": "%", "precipitacion": "mm", "viento": "km/h"}


def mostrar(sel: Seleccion) -> None:
    f, variable = sel.filtros, sel.variable
    st.title("SmartCity Analytics — Ambiente")
    st.caption("Londres (5 zonas) · 2023–2024 · calidad del aire (OpenAQ), clima (Open-Meteo) y sensores IoT "
               "virtuales. Movilidad y energía son series **simuladas** (D-03).")

    _kpis_ambiente(f)
    _kpis_iot(f)
    _kpis_movilidad_energia(f)
    st.divider()
    _graficos(f, variable)
    st.divider()
    _mapa_y_detalle(f, variable)
    st.divider()
    _eventos_alertas_incidentes(f)


# --------------------------------------------------------------------------- KPIs
def _kpis_ambiente(f) -> None:
    st.subheader("Ambiente")
    if not visible(f, FUENTE_MONGO):
        ui.vacio("Oculto por el filtro de fuente.")
        return
    k, err, _ = fuentes.llamar_b("kpis_ambiente", f)
    if err:
        ui.aviso_error("KPIs de ambiente", err)
        return
    cols = st.columns(6)
    with cols[5]:
        ui.tarjeta_kpi("CO₂ promedio", ui.num(k.get("co2_promedio"), 0, "ppm"), FUENTE_MONGO,
                       nota="sensores con firmware 2.x")
    with cols[0]:
        ui.tarjeta_kpi("Temperatura promedio", ui.num(k["temperatura_promedio"], 1, "°C"), FUENTE_MONGO)
    with cols[1]:
        ui.tarjeta_kpi("PM2.5 promedio", ui.num(k["pm25_promedio"], 1, "µg/m³"), FUENTE_MONGO)
    with cols[2]:
        ui.tarjeta_kpi("CO promedio", ui.num(k["co_promedio"], 0, "µg/m³"), FUENTE_MONGO)
    with cols[3]:
        ui.tarjeta_kpi("Humedad promedio", ui.num(k["humedad_promedio"], 0, "%"), FUENTE_MONGO)
    with cols[4]:
        ui.tarjeta_kpi("Alertas ambientales", ui.num(k["alertas_ambientales"]), FUENTE_MONGO)


def _kpis_iot(f) -> None:
    st.subheader("IoT")
    cols = st.columns(6)
    # --- Pareja A: catálogo de sensores (PostgreSQL)
    if visible(f, FUENTE_POSTGRES):
        if fuentes.tiene_a("kpis_iot"):
            k, err, _ = fuentes.llamar_a("kpis_iot", f)
            if err:
                with cols[0]:
                    ui.aviso_error("KPIs IoT (PostgreSQL)", err)
            else:
                with cols[0]:
                    ui.tarjeta_kpi("Sensores registrados", ui.num(k.get("sensores")), FUENTE_POSTGRES)
                with cols[1]:
                    ui.tarjeta_kpi("Activos / inactivos",
                                   f"{ui.num(k.get('activos'))} / {ui.num(k.get('inactivos'))}", FUENTE_POSTGRES)
        else:
            with cols[0]:
                ui.tarjeta_kpi("Sensores registrados", "—", FUENTE_POSTGRES, nota="pendiente pareja A")
            with cols[1]:
                ui.tarjeta_kpi("Activos / inactivos", "—", FUENTE_POSTGRES, nota="pendiente pareja A")
    # --- Pareja B: mediciones, alertas y ritmo (MongoDB)
    if visible(f, FUENTE_MONGO):
        k, err, _ = fuentes.llamar_b("kpis_iot_mongo", f)
        if err:
            with cols[2]:
                ui.aviso_error("KPIs IoT (MongoDB)", err)
            return
        with cols[2]:
            ui.tarjeta_kpi("Mediciones recibidas", ui.num(k["mediciones_recibidas"]), FUENTE_MONGO,
                           nota=f"{ui.num(k['sensores_reportando'])} sensores")
        with cols[3]:
            ui.tarjeta_kpi("Alertas generadas", ui.num(k["alertas_generadas"]), FUENTE_MONGO)
        with cols[4]:
            ui.tarjeta_kpi("Datos por hora", ui.num(k["datos_por_hora"], 1), FUENTE_MONGO,
                           nota=f"{ui.num(k['datos_por_dia'])} / día · {ui.num(k['datos_por_minuto'], 2)} / min")
        with cols[5]:
            ui.tarjeta_kpi("Lecturas por segundo", ui.num(k["lecturas_por_segundo"], 3), FUENTE_MONGO,
                           ayuda="Promedio en el período: mediciones / segundos entre la primera y la última.")


def _kpis_movilidad_energia(f) -> None:
    if not visible(f, FUENTE_SPARK):
        return
    st.subheader("Movilidad (tráfico simulado)")
    cols = st.columns(5)
    km, err_m, _ = fuentes.llamar_b("kpis_movilidad", f)
    if err_m:
        with cols[0]:
            ui.aviso_error("Movilidad", err_m)
    elif km.get("vehiculos_detectados") is not None:
        with cols[0]:
            ui.tarjeta_kpi("Vehículos detectados", ui.num(km["vehiculos_detectados"]), FUENTE_SPARK,
                           nota=f"{ui.num(km['vehiculos_hora_promedio'])} / h por contador · simulado")
        with cols[1]:
            ui.tarjeta_kpi("Velocidad promedio", ui.num(km["velocidad_promedio_kmh"], 1, "km/h"), FUENTE_SPARK, nota="simulado")
        with cols[2]:
            ui.tarjeta_kpi("Congestión promedio", ui.num(km["congestion_promedio_pct"], 1, "%"), FUENTE_SPARK,
                           nota=f"pico {km['hora_pico_utc']}:00 UTC · simulado")
    with cols[3]:
        if visible(f, FUENTE_MONGO):
            ki, err_i, _ = fuentes.llamar_b("kpis_incidentes", f)
            if err_i:
                ui.aviso_error("Incidentes", err_i)
            else:
                ui.tarjeta_kpi("Accidentes / incidentes", f"{ui.num(ki['accidentes'])} / {ui.num(ki['incidentes'])}",
                               FUENTE_MONGO, nota="accidentes de tránsito / incidentes urbanos")
    with cols[4]:
        if km and km.get("tiempo_recorrido_promedio_min") is not None:
            ui.tarjeta_kpi("Tiempo promedio de recorrido", ui.num(km["tiempo_recorrido_promedio_min"], 1, "min"),
                           FUENTE_SPARK, nota="simulado")
        else:
            ui.tarjeta_kpi("Tiempo promedio de recorrido", "sin dato", FUENTE_SPARK,
                           nota="las fuentes no miden tiempos de recorrido",
                           ayuda="Ninguna fuente del proyecto (OpenAQ, Open-Meteo, contadores simulados) mide tiempos de "
                                 "viaje; no se inventa el dato. Justificación en el documento técnico.")

    st.subheader("Energía (consumo simulado)")
    cols = st.columns(5)
    ke, err_e, _ = fuentes.llamar_b("kpis_energia", f)
    if err_e:
        with cols[0]:
            ui.aviso_error("Energía", err_e)
    elif ke.get("consumo_total_kwh") is not None:
        with cols[0]:
            ui.tarjeta_kpi("Consumo total", ui.num(ke["consumo_total_kwh"] / 1e6, 1, "GWh"), FUENTE_SPARK, nota="simulado")
        with cols[1]:
            ui.tarjeta_kpi("Consumo promedio por zona", ui.num(ke["consumo_promedio_por_zona_kwh"] / 1e6, 1, "GWh"),
                           FUENTE_SPARK, nota="simulado")
        with cols[2]:
            ui.tarjeta_kpi("Consumo por hora", ui.num(ke["consumo_horario_promedio_kwh"] / 1000, 1, "MWh"),
                           FUENTE_SPARK, nota="promedio por medidor · simulado")
        with cols[3]:
            ui.tarjeta_kpi("Pico de consumo", ui.num(ke["consumo_maximo_kwh"] / 1000, 1, "MWh"), FUENTE_SPARK,
                           nota=f"hora de mayor consumo {ke['hora_pico_utc']}:00 UTC")
        with cols[4]:
            var = ke.get("variacion_pct_periodo_anterior")
            ui.tarjeta_kpi("Variación vs. período anterior",
                           "—" if var is None else f"{'+' if var >= 0 else ''}{ui.num(var, 1)} %", FUENTE_SPARK,
                           nota="mismo largo, inmediatamente antes" if var is not None else "sin datos del período anterior")


# --------------------------------------------------------------------------- 4 gráficos
def _graficos(f, variable: str) -> None:
    nombre = VARIABLES[variable]
    c1, c2 = st.columns(2)
    with c1:
        st.markdown(f"**Serie temporal — {nombre}**")
        if visible(f, FUENTE_MONGO):
            unidad = st.radio("Agrupar por", ["hour", "day", "week", "month"], index=1, horizontal=True,
                              format_func={"hour": "hora", "day": "día", "week": "semana", "month": "mes"}.get,
                              key="serie_unidad", label_visibility="collapsed")
            df, err, _ = fuentes.llamar_b("serie_temporal", f, variable, unidad)
            if err:
                ui.aviso_error("Serie temporal", err)
            elif df is None or df.empty:
                ui.vacio()
            else:
                ui.grafico_serie(df, "periodo", "valor", color="zona", etiqueta_y=f"{variable} ({UNIDAD[variable]})")
            ui.etiqueta_origen(FUENTE_MONGO)
    with c2:
        st.markdown(f"**Comparativa por zonas — {nombre}**")
        if visible(f, FUENTE_MONGO):
            df, err, _ = fuentes.llamar_b("comparativa_zonas", f, variable)
            if err:
                ui.aviso_error("Comparativa por zonas", err)
            elif df is None or df.empty:
                ui.vacio()
            else:
                ui.grafico_barras(df, "zona", "promedio", etiqueta_y=f"promedio ({UNIDAD[variable]})", texto="promedio")
            ui.etiqueta_origen(FUENTE_MONGO)

    c3, c4 = st.columns(2)
    with c3:
        st.markdown("**Distribución por tipo de sensor**")
        if visible(f, FUENTE_MONGO):
            df, err, _ = fuentes.llamar_b("distribucion_tipos", f)
            if err:
                ui.aviso_error("Distribución por tipo", err)
            elif df is None or df.empty:
                ui.vacio()
            else:
                ui.grafico_dona(df, "tipo", "lecturas")
            ui.etiqueta_origen(FUENTE_MONGO)
    with c4:
        st.markdown("**Top 3 zonas con mayor incremento de PM2.5 en hora pico vs. valle**")
        if visible(f, FUENTE_SPARK):
            df, err, _ = fuentes.llamar_b("spark_top_zonas_pico")
            if err:
                ui.aviso_error("Resultado Spark", err)
            elif df is None or df.empty:
                ui.vacio("Aún no hay resultado: ejecutar el análisis de movilidad.")
            else:
                df = df.sort_values("diferencia_pm25", ascending=False)
                ui.grafico_barras(df, "zona", "diferencia_pm25", etiqueta_y="PM2.5 pico − valle (µg/m³)",
                                  texto="variacion_pct")
                st.caption("Días laborables, hora local de Londres: pico 07–09 h y 17–19 h vs. valle 02–05 h. "
                           "Sensores sintéticos (únicos con PM2.5 en las 5 zonas).")
            ui.etiqueta_origen([FUENTE_SPARK, FUENTE_HDFS], "análisis B4.2 · movilidad")
            if st.button("Recalcular (Spark)", key="recalcular_top3",
                         help="Relanza analitica.py --analisis movilidad en el clúster (puede tardar minutos)."):
                with st.spinner("Ejecutando Spark…"):
                    codigo, salida = fuentes.b().recalcular_analisis("movilidad")
                st.cache_data.clear()
                (st.success if codigo == 0 else st.error)(f"spark-submit terminó con código {codigo}")
                with st.expander("Salida del job"):
                    st.code(salida or "(sin salida)", language="text")


# --------------------------------------------------------------------------- mapa y tabla
def _mapa_y_detalle(f, variable: str) -> None:
    c1, c2 = st.columns([1, 1])
    with c1:
        st.markdown("**Mapa de sensores** (color = zona, calor = PM2.5 promedio)")
        df, err, origen = None, "", FUENTE_POSTGRES
        if fuentes.tiene_a("sensores") and visible(f, FUENTE_POSTGRES):
            df, err, _ = fuentes.llamar_a("sensores", f)
            if err:
                st.caption(f"PostgreSQL no respondió ({err[:80]}…); se usan las posiciones de MongoDB.")
        if df is None or err:
            df, err, _ = fuentes.llamar_b("sensores_mongo", f)
            origen = FUENTE_MONGO
        if err:
            ui.aviso_error("Mapa", err)
        elif df is not None and not df.empty:
            ui.mapa_sensores(df, "pm25" if "pm25" in df.columns else None)
        else:
            ui.vacio()
        ui.etiqueta_origen(origen, "" if origen == FUENTE_POSTGRES else
                           "posiciones desde MongoDB (PostgreSQL no disponible)")
    with c2:
        st.markdown("**Detalle de lecturas** (COLUMNAS_ESTANDAR)")
        partes, origenes, errores = [], [], []
        if visible(f, FUENTE_MONGO):
            df, err, _ = fuentes.llamar_b("lecturas_mongo", f)
            (errores.append(f"MongoDB: {err}") if err else partes.append(df))
            origenes.append(FUENTE_MONGO)
        if visible(f, FUENTE_SPARK):
            df, err, _ = fuentes.llamar_b("lecturas_trafico_energia", f)
            if not err:
                partes.append(df)
                origenes.append(FUENTE_SPARK)
        for funcion, fuente in (("lecturas_pg", FUENTE_POSTGRES), ("lecturas_hdfs", FUENTE_HDFS)):
            if fuentes.tiene_a(funcion) and visible(f, fuente):
                df, err, _ = fuentes.llamar_a(funcion, f)
                (errores.append(f"{fuente}: {err}") if err else partes.append(df))
                origenes.append(fuente)
        for e in errores:
            ui.aviso_error("Detalle", e)
        con_filas = [p for p in partes if p is not None and not p.empty]
        if con_filas:
            datos = pd.concat(con_filas, ignore_index=True).sort_values("timestamp", ascending=False)
            ui.tabla(datos, "detalle")
        else:
            ui.vacio()
        ui.etiqueta_origen(origenes or [FUENTE_MONGO], "columna «fuente» por fila")


def _eventos_alertas_incidentes(f) -> None:
    if not visible(f, FUENTE_MONGO):
        return
    st.subheader("Eventos, alertas e incidentes")
    t1, t2, t3 = st.tabs(["Alertas", "Eventos", "Incidentes"])
    for pestaña, funcion in ((t1, "alertas"), (t2, "eventos"), (t3, "incidentes")):
        with pestaña:
            df, err, _ = fuentes.llamar_b(funcion, f)
            if err:
                ui.aviso_error(funcion.capitalize(), err)
            else:
                ui.tabla(df, f"tabla_{funcion}", filas_por_pagina=15)
            ui.etiqueta_origen(FUENTE_MONGO)
