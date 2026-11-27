"""Consultas por tecnología (D6): PostgreSQL / MongoDB / HDFS / Spark.

Cada tecnología expone un registro ``dict[str, comun.Consulta]``:
  MongoDB -> conectores_mongo_spark.CONSULTAS_MONGO      Spark -> conectores_mongo_spark.CONSULTAS_SPARK
  PostgreSQL -> conectores_postgres_hadoop.CONSULTAS_PG  HDFS  -> conectores_postgres_hadoop.CONSULTAS_HDFS
"""
from __future__ import annotations

import time
from datetime import date

import pandas as pd
import streamlit as st

from contrato import FUENTE_HDFS, FUENTE_MONGO, FUENTE_POSTGRES, FUENTE_SPARK

from . import componentes as ui
from . import fuentes

REGISTROS = {
    FUENTE_POSTGRES: (fuentes.MODULO_A, "CONSULTAS_PG"),
    FUENTE_MONGO: (fuentes.MODULO_B, "CONSULTAS_MONGO"),
    FUENTE_HDFS: (fuentes.MODULO_A, "CONSULTAS_HDFS"),
    FUENTE_SPARK: (fuentes.MODULO_B, "CONSULTAS_SPARK"),
}


def mostrar() -> None:
    st.title("Consultas por tecnología")
    tecnologia = st.radio("Tecnología", list(REGISTROS), horizontal=True, key="tec_consultas")
    nombre_modulo, registro = REGISTROS[tecnologia]
    try:
        consultas = fuentes.registro_consultas(nombre_modulo, registro, tecnologia)
    except Exception as e:  # noqa: BLE001
        ui.aviso_error(f"Registro {registro}", f"{type(e).__name__}: {e}")
        consultas = None
    if not consultas:
        ui.pendiente_a(f"registro `{registro}` en `{nombre_modulo}.py`")
    else:
        _panel_consulta(tecnologia, consultas)

    st.divider()
    if tecnologia == FUENTE_MONGO:
        _panel_crud()
    elif tecnologia == FUENTE_SPARK:
        _panel_analisis_spark()
    elif tecnologia == FUENTE_POSTGRES:
        if fuentes.tiene_a("explain"):
            st.caption("EXPLAIN disponible desde el panel de cada consulta.")
        else:
            ui.pendiente_a("EXPLAIN de consultas PostgreSQL (`explain(id, **params)`)")
    elif tecnologia == FUENTE_HDFS:
        if fuentes.tiene_a("hdfs_ls"):
            _panel_hdfs()
        else:
            ui.pendiente_a("explorador HDFS, operaciones sandbox, fsck y MapReduce (A5.3)")


def _widget(clave: str, nombre: str, defecto):
    k = f"param_{clave}_{nombre}"
    if isinstance(defecto, bool):
        return st.checkbox(nombre, value=defecto, key=k)
    if isinstance(defecto, int):
        return int(st.number_input(nombre, value=defecto, step=1, key=k))
    if isinstance(defecto, float):
        return float(st.number_input(nombre, value=defecto, key=k, format="%.4f"))
    if isinstance(defecto, date):
        return st.date_input(nombre, value=defecto, key=k)
    return st.text_input(nombre, value="" if defecto is None else str(defecto), key=k)


def _panel_consulta(tecnologia: str, consultas: dict) -> None:
    ids = list(consultas)
    ident = st.selectbox("Consulta", ids, format_func=lambda i: f"{i} — {consultas[i].titulo}", key=f"sel_{tecnologia}")
    c = consultas[ident]
    st.markdown(f"**{c.id} — {c.titulo}**  \n{c.descripcion}")
    ui.etiqueta_origen(c.tecnologia)
    with st.expander("Código", expanded=False):
        st.code(c.codigo, language=c.lenguaje)

    params = {}
    if c.parametros:
        st.markdown("**Parámetros**")
        cols = st.columns(min(4, len(c.parametros)))
        for i, (nombre, defecto) in enumerate(c.parametros.items()):
            with cols[i % len(cols)]:
                params[nombre] = _widget(c.id, nombre, defecto)
    if c.tecnologia == FUENTE_SPARK:
        st.caption("Sin «recalcular» se muestra el último resultado guardado en HDFS; con «recalcular» se lanza "
                   "spark-submit en el clúster (tarda).")

    b1, b2 = st.columns([1, 5])
    ejecutar = b1.button("Ejecutar", type="primary", key=f"run_{c.id}")
    explain = (c.tecnologia == FUENTE_POSTGRES and fuentes.tiene_a("explain")
               and b2.button("EXPLAIN ANALYZE", key=f"explain_{c.id}"))
    if ejecutar:
        t0 = time.perf_counter()
        try:
            with st.spinner("Ejecutando…"):
                df = c.ejecutar(**params)
            st.session_state[f"res_{c.id}"] = (df, time.perf_counter() - t0, None)
        except Exception as e:  # noqa: BLE001
            st.session_state[f"res_{c.id}"] = (None, time.perf_counter() - t0, f"{type(e).__name__}: {str(e)[:800]}")
    if explain:
        plan, err, _ = fuentes.llamar_a("explain", c.id, cache=False, **params)
        if err:
            ui.aviso_error("EXPLAIN", err)
        else:
            st.code(str(plan), language="text")

    if f"res_{c.id}" in st.session_state:
        df, segundos, err = st.session_state[f"res_{c.id}"]
        if err:
            ui.aviso_error(c.id, err)
            return
        st.success(f"{ui.num(0 if df is None else len(df))} filas en {ui.num(segundos, 2)} s")
        if isinstance(df, pd.DataFrame):
            ui.tabla(df, f"res_{c.id}")
            ui.grafico_automatico(df, c.id)
        else:
            st.write(df)
        ui.etiqueta_origen(c.tecnologia)


def _panel_crud() -> None:
    st.subheader("CRUD en MongoDB (colección `demo_crud`)")
    st.caption("Las operaciones se ejecutan sobre `demo_crud`, nunca sobre `lectura`.")
    m = fuentes.b()
    if m is None:
        ui.aviso_error("CRUD", fuentes.error_import(fuentes.MODULO_B))
        return
    cols = st.columns(len(m.OPERACIONES_CRUD))
    for col, op in zip(cols, m.OPERACIONES_CRUD):
        if col.button(op, key=f"crud_{op}", width="stretch"):
            try:
                st.session_state["crud_ultimo"] = (op, *m.crud(op), None)
            except Exception as e:  # noqa: BLE001
                st.session_state["crud_ultimo"] = (op, None, "", f"{type(e).__name__}: {e}")
    if "crud_ultimo" in st.session_state:
        op, resultado, comando, err = st.session_state["crud_ultimo"]
        if err:
            ui.aviso_error(op, err)
        else:
            c1, c2 = st.columns(2)
            with c1:
                st.markdown("**Comando mongosh equivalente**")
                st.code(comando, language="javascript")
            with c2:
                st.markdown("**Resultado**")
                if isinstance(resultado, pd.DataFrame):
                    st.dataframe(resultado, hide_index=True, width="stretch")
                else:
                    st.json(resultado, expanded=True)
    with st.expander("Documentos actuales en demo_crud"):
        try:
            st.dataframe(m.documentos_demo_crud(), hide_index=True, width="stretch")
        except Exception as e:  # noqa: BLE001
            ui.aviso_error("demo_crud", str(e))
    ui.etiqueta_origen(FUENTE_MONGO)


def _panel_analisis_spark() -> None:
    st.subheader("Análisis Spark (B4.2)")
    m = fuentes.b()
    if m is None:
        ui.aviso_error("Spark", fuentes.error_import(fuentes.MODULO_B))
        return
    tipo = st.selectbox("Análisis", list(m.ANALISIS_SPARK), key="analisis_tipo")
    salidas, err, segundos = fuentes.llamar_b("spark_analisis", tipo)
    if err:
        ui.aviso_error("Análisis", err)
    else:
        for nombre, df in salidas.items():
            st.markdown(f"**{nombre}** — `{m.ruta_analitica(nombre)}`")
            if df is None or df.empty:
                ui.vacio("Salida no encontrada en HDFS (ejecutar el análisis).")
                continue
            if nombre == "correlacional_matriz" and {"variable_a", "variable_b", "correlacion_pearson"} <= set(df.columns):
                grupo = st.selectbox("Zona / grupo", sorted((df["zona"] + " · " + df["grupo_fuente"]).unique()),
                                     key="corr_grupo")
                sub = df[(df["zona"] + " · " + df["grupo_fuente"]) == grupo]
                ui.grafico_heatmap(sub, "variable_b", "variable_a", "correlacion_pearson")
            ui.tabla(df, f"an_{nombre}", filas_por_pagina=10)
        ui.etiqueta_origen([FUENTE_SPARK, FUENTE_HDFS], f"leído en {ui.num(segundos, 1)} s")
    if st.button("Recalcular este análisis (Spark)", key="recalcular_analisis"):
        with st.spinner("Ejecutando spark-submit…"):
            codigo, salida = m.recalcular_analisis(tipo)
        st.cache_data.clear()
        (st.success if codigo == 0 else st.error)(f"spark-submit terminó con código {codigo}")
        with st.expander("Salida del job"):
            st.code(salida or "(sin salida)", language="text")


def _panel_hdfs() -> None:
    m = fuentes.a()
    st.subheader("HDFS: clúster, explorador y operaciones")
    r, err, _ = fuentes.llamar_a("resumen_hdfs", cache=False)
    if err:
        ui.aviso_error("Estado de HDFS", err)
    else:
        cols = st.columns(6)
        for col, (titulo, valor) in zip(cols, [
                ("DataNodes vivos", f"{r['datanodes_vivos']} / {r['datanodes_vivos'] + r['datanodes_caidos']}"),
                ("Archivos", ui.num(r["archivos"])), ("Bloques", ui.num(r["bloques"])),
                ("Usado", ui.num(r["usado_mb"], 1, "MB")), ("Subreplicados", ui.num(r["bloques_subreplicados"])),
                ("Faltantes", ui.num(r["bloques_faltantes"]))]):
            with col:
                ui.tarjeta_kpi(titulo, valor, FUENTE_HDFS)

    t1, t2, t3, t4 = st.tabs(["Explorador", "Bloques y réplicas (fsck)", "Subir / borrar (sandbox)", "MapReduce"])
    with t1:
        ruta = st.text_input("Ruta", "/smartcity", key="hdfs_ruta")
        recursivo = st.checkbox("Recursivo (-R)", key="hdfs_rec")
        try:
            df, comando = m.hdfs_ls(ruta, recursivo)
            st.code(comando, language="bash")
            ui.tabla(df, "hdfs_ls")
            archivos = df[df["tipo"] == "archivo"]["ruta"].tolist() if not df.empty else []
            if archivos:
                elegido = st.selectbox("Ver primeras líneas de", archivos, key="hdfs_cat_sel")
                texto, comando = m.hdfs_cat(elegido, 15)
                st.code(comando, language="bash")
                st.code(texto or "(vacío o binario)", language="text")
        except Exception as e:  # noqa: BLE001
            ui.aviso_error("hdfs dfs -ls", str(e))
    with t2:
        ruta = st.text_input("Archivo o carpeta", "/smartcity/raw", key="hdfs_fsck_ruta")
        if st.button("Ejecutar fsck", key="hdfs_fsck"):
            try:
                df, comando = m.hdfs_fsck(ruta)
                st.code(comando, language="bash")
                st.text(df.attrs.get("resumen", ""))
                ui.tabla(df, "hdfs_fsck")
            except Exception as e:  # noqa: BLE001
                ui.aviso_error("fsck", str(e))
    with t3:
        st.caption("Solo se permite escribir o borrar dentro de /smartcity/sandbox.")
        archivo = st.file_uploader("Archivo a subir", key="hdfs_subir")
        destino = st.text_input("Destino", "/smartcity/sandbox/ui/" + (archivo.name if archivo else "archivo.txt"),
                                key="hdfs_destino")
        if archivo and st.button("hdfs dfs -put", key="hdfs_put"):
            try:
                msg, comando = m.hdfs_put(archivo.getvalue(), destino)
                st.code(comando, language="bash")
                st.success(msg)
            except Exception as e:  # noqa: BLE001
                ui.aviso_error("put", str(e))
        borrar = st.text_input("Ruta a borrar", "/smartcity/sandbox/ui", key="hdfs_borrar")
        if st.button("hdfs dfs -rm -r", key="hdfs_rm"):
            try:
                msg, comando = m.hdfs_rm(borrar)
                st.code(comando, language="bash")
                st.success(msg or "Borrado")
            except Exception as e:  # noqa: BLE001
                ui.aviso_error("rm", str(e))
    with t4:
        job = st.selectbox("Job", list(m.MR_JOBS), format_func=lambda j: f"{j} — {m.MR_JOBS[j]['titulo']}", key="mr_job")
        c1, c2 = st.columns(2)
        desde = c1.text_input("DESDE (mr05)", "2024-01-01", key="mr_desde")
        hasta = c2.text_input("HASTA (mr05)", "2024-01-31", key="mr_hasta")
        if st.button("Ejecutar en YARN", key="mr_run", help="Tarda ~20 s más el procesamiento; ver http://localhost:8088"):
            with st.spinner("Hadoop Streaming en YARN…"):
                codigo, salida = m.mr_ejecutar(job, desde, hasta)
            (st.success if codigo == 0 else st.error)(f"Job {job} terminó con código {codigo}")
            with st.expander("Salida del job"):
                st.code(chr(10).join(l for l in salida.splitlines() if "SMARTCITY" in l or "rror" in l) or salida[-2000:],
                        language="text")
        try:
            ui.tabla(m.mr_resultado(job), f"mr_{job}")
            ui.etiqueta_origen(FUENTE_HDFS, f"/smartcity/mapreduce/{job}")
        except Exception as e:  # noqa: BLE001
            st.caption(f"ℹ️ {e}")
