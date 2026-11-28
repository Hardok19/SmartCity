"""Comparación tecnológica (D7 / C2 / C3): matriz justificada, experimentos E1–E7, arquitectura y respuestas.

El texto (matriz y respuestas) se lee de los documentos para que haya una sola versión:
docs/tecnico/04_justificacion_tecnologica.md y docs/defensa/PREGUNTAS_RESPUESTAS.md. Los gráficos y los hallazgos
numéricos se calculan en vivo desde docs/resultados/benchmarks.csv (scripts/benchmark_comparacion.py).
"""
from __future__ import annotations

import re
from pathlib import Path

import pandas as pd
import streamlit as st

from contrato import FUENTE_HDFS, FUENTE_MONGO, FUENTE_POSTGRES, FUENTE_SPARK

from . import componentes as ui
from . import graficos_experimentos as gx

RAIZ = Path(__file__).resolve().parents[1]
RESULTADOS = RAIZ / "docs" / "resultados"
DOC_04 = RAIZ / "docs" / "tecnico" / "04_justificacion_tecnologica.md"
DOC_PREGUNTAS = RAIZ / "docs" / "defensa" / "PREGUNTAS_RESPUESTAS.md"
IMG = RAIZ / "docs" / "diagramas" / "img"
TECNOLOGIAS = {"E1": [FUENTE_POSTGRES, FUENTE_MONGO, FUENTE_SPARK], "E2": [FUENTE_POSTGRES, FUENTE_MONGO, FUENTE_SPARK],
               "E3": [FUENTE_POSTGRES, FUENTE_MONGO, FUENTE_SPARK], "E4": [FUENTE_POSTGRES, FUENTE_MONGO],
               "E5": [FUENTE_SPARK], "E6": [FUENTE_HDFS], "E7": [FUENTE_POSTGRES, FUENTE_MONGO]}


def _seccion(ruta: Path, inicio: str) -> str:
    """Texto de la sección '## <inicio>…' hasta la siguiente '## ' (sin imágenes ni vínculos relativos)."""
    if not ruta.exists():
        return ""
    texto = ruta.read_text(encoding="utf-8")
    m = re.search(rf"^## {re.escape(inicio)}.*?$(.*?)(?=^## |\Z)", texto, re.S | re.M)
    if not m:
        return ""
    cuerpo = re.sub(r"!\[[^\]]*\]\([^)]+\)", "", m.group(1))
    return re.sub(r"\[([^\]]+)\]\((?!http)[^)]+\)", r"\1", cuerpo).strip()


def _veces(a, b) -> str:
    if not a or not b:
        return "—"
    return f"×{a / b:,.0f}".replace(",", ".") if a / b >= 10 else f"×{a / b:.1f}".replace(".", ",")


def hallazgo(exp: str, df: pd.DataFrame) -> str:
    m = gx.mediana
    escalas = sorted(df.loc[df["experimento"] == exp, "escala_filas"].dropna().unique())
    if not escalas and exp != "E6":
        return ""
    chica, grande = (escalas[0], escalas[-1]) if escalas else (None, None)
    t = gx.tiempo_txt
    if exp == "E1":
        pg_c, pg_g = m(df, "E1", FUENTE_POSTGRES, "group_by_sql", chica), m(df, "E1", FUENTE_POSTGRES, "group_by_sql", grande)
        sp_c, sp_g = m(df, "E1", FUENTE_SPARK, "dataframe_cores2", chica), m(df, "E1", FUENTE_SPARK, "dataframe_cores2", grande)
        mg_g = m(df, "E1", FUENTE_MONGO, "aggregate_group", grande)
        if None in (pg_c, pg_g, sp_c, sp_g):
            return ""
        cierre = (f"con {gx.escala_txt(grande)} filas Spark ya es el más rápido ({_veces(pg_g, sp_g)} frente a "
                  "PostgreSQL)" if sp_g < pg_g else
                  f"con {gx.escala_txt(grande)} filas la diferencia baja a {_veces(sp_g, pg_g)}")
        return (f"Con {gx.escala_txt(grande)} filas: PostgreSQL {t(pg_g)}, MongoDB {t(mg_g) if mg_g else '—'}, Spark "
                f"{t(sp_g)}. Con {gx.escala_txt(chica)} filas Spark es {_veces(sp_c, pg_c)} más lento que PostgreSQL; "
                f"{cierre}. Spark paga un costo fijo por acción (planificar y repartir tareas) que pesa menos a medida "
                f"que crece el volumen: su tiempo creció {_veces(sp_g, sp_c)} mientras que el de PostgreSQL creció "
                f"{_veces(pg_g, pg_c)}. Parte de la ventaja es el formato: Spark lee Parquet columnar y comprimido.")
    if exp == "E2":
        pg, mg = m(df, "E2", FUENTE_POSTGRES, "con_indice", grande), m(df, "E2", FUENTE_MONGO, "con_indice", grande)
        sp = m(df, "E2", FUENTE_SPARK, "full_scan_parquet", grande)
        arr = m(df, "E2", FUENTE_SPARK, "job_completo_con_arranque")
        if None in (pg, sp):
            return ""
        return (f"Con {gx.escala_txt(grande)} filas el índice responde en {t(pg)} (PostgreSQL) y {t(mg) if mg else '—'} "
                f"(MongoDB); Spark necesita {t(sp)} porque no tiene índices y recorre el archivo ({_veces(sp, pg)}). "
                + (f"Si además hay que lanzar el job, la consulta tarda {t(arr)}: Spark no sirve para consultas puntuales." if arr else ""))
    if exp == "E3":
        pg = m(df, "E3", FUENTE_POSTGRES, "join_sql", chica)
        doc = m(df, "E3", FUENTE_MONGO, "lookup_por_documento", chica)
        agr = m(df, "E3", FUENTE_MONGO, "lookup_tras_agrupar", grande)
        pg_g = m(df, "E3", FUENTE_POSTGRES, "join_sql", grande)
        if None in (pg, doc):
            return ""
        return (f"Con {gx.escala_txt(chica)} filas, el JOIN de PostgreSQL tarda {t(pg)} y el `$lookup` por documento de "
                f"MongoDB {t(doc)} ({_veces(doc, pg)}). MongoDB solo se acerca si se reescribe la consulta para agrupar "
                f"antes del `$lookup` ({t(agr) if agr else '—'} frente a {t(pg_g) if pg_g else '—'} de PostgreSQL con "
                f"{gx.escala_txt(grande)}), es decir, si el desarrollador hace a mano el trabajo del optimizador relacional.")
    if exp == "E4":
        rew = m(df, "E4", FUENTE_POSTGRES, "pg_add_column_default_volatil")
        nul = m(df, "E4", FUENTE_POSTGRES, "pg_add_column_nullable")
        mg = m(df, "E4", FUENTE_MONGO, "insert_10k_campos_nuevos")
        if None in (rew, nul, mg):
            return ""
        return (f"MongoDB guarda los documentos con campos nuevos sin ninguna sentencia DDL ({t(mg)} para 10.000). En "
                f"PostgreSQL hace falta registrar el tipo (FK) y alterar la tabla: agregar una columna anulable es "
                f"instantáneo ({t(nul)}), pero una que obliga a reescribir la tabla tarda {t(rew)} y la bloquea mientras "
                "tanto. La flexibilidad de Mongo es real; el costo en PostgreSQL depende de cómo se diseñe el cambio (JSONB es la salida intermedia).")
    if exp == "E5":
        partes = []
        for e in escalas:
            c1, c2 = m(df, "E5", FUENTE_SPARK, "cores1", e), m(df, "E5", FUENTE_SPARK, "cores2", e)
            if c1 and c2:
                partes.append(f"{gx.escala_txt(e)}: {t(c1)} → {t(c2)} ({_veces(c1, c2)})")
        return ("Tiempo de la agregación con 1 → 2 núcleos: " + "; ".join(partes) +
                ". La mejora aparece con volumen: con pocos datos domina el costo fijo de coordinar las tareas, y "
                "agregar núcleos no ayuda o incluso empeora."
                ) if partes else ""
    if exp == "E6":
        lec = m(df, "E6", FUENTE_HDFS, "lectura_con_nodo_caido")
        det = m(df, "E6", FUENTE_HDFS, "deteccion_nodo_muerto")
        rep = m(df, "E6", FUENTE_HDFS, "re_replicacion_completa")
        if None in (lec, det, rep):
            return ""
        return (f"Con un DataNode apagado el archivo se leyó completo y con el mismo md5 ({t(lec)}). El NameNode lo declaró "
                f"muerto a los {det:.0f} s y a los {rep:.0f} s todos los bloques tenían de nuevo 2 réplicas, sin intervención.")
    if exp == "E7":
        textos = []
        for tec in (FUENTE_POSTGRES, FUENTE_MONGO):
            con, sin = m(df, "E7", tec, "con_indice", grande), m(df, "E7", tec, "sin_indice", grande)
            if con and sin:
                textos.append(f"{tec} {t(sin)} → {t(con)} ({_veces(sin, con)})")
        d = df[(df["experimento"] == "E7") & (df["tecnologia"] == FUENTE_MONGO) & (df["escala_filas"] == grande)]
        examinados = {r.variante: r.detalle.get("plan", {}).get("docs_examinados") for r in d.itertuples()}
        extra = (f" MongoDB examina {examinados.get('con_indice'):,} documentos con índice frente a "
                 f"{examinados.get('sin_indice'):,} sin él.".replace(",", ".")
                 if examinados.get("con_indice") is not None and examinados.get("sin_indice") is not None else "")
        return (f"Con {gx.escala_txt(grande)} filas, sin índice frente a con índice: " + "; ".join(textos) + "." + extra
                ) if textos else ""
    return ""


def _experimentos() -> None:
    df = gx.cargar()
    if df.empty:
        ui.vacio("Aún no existe docs/resultados/benchmarks.csv: ejecute `python scripts/benchmark_comparacion.py "
                 "--preparar --medir`.")
        return
    info = gx.entorno()
    if info:
        st.caption("Entorno: " + " · ".join(f"{k}: {v}" for k, v in info.items() if k != "tiempos"))
    st.caption("Mismo dato lógico (lecturas de 50 sensores cada 15 min) cargado en los tres motores a 100k, 1 M y "
               "5 M filas. Mediana de 3 repeticiones tras 1 calentamiento.")
    pestanas = st.tabs(list(gx.FIGURAS))
    for pestana, (exp, (titulo, funcion)) in zip(pestanas, gx.FIGURAS.items()):
        with pestana:
            st.markdown(f"**{exp} · {titulo}**")
            fig = funcion(df)
            if not fig.data:
                ui.vacio(f"Sin resultados de {exp}.")
                continue
            st.plotly_chart(fig, width="stretch", key=f"fig_{exp}")
            ui.etiqueta_origen(TECNOLOGIAS[exp], "medido con scripts/benchmark_comparacion.py")
            texto = hallazgo(exp, df)
            if texto:
                st.markdown(texto)
            with st.expander("Datos"):
                tabla = df[df["experimento"] == exp][["tecnologia", "variante", "escala_filas", "segundos"]]
                ui.tabla(tabla.reset_index(drop=True), f"bench_{exp}")


def _diagrama(nombre: str, alto: int) -> None:
    """PNG exportado si existe; si no, el bloque Mermaid de docs/diagramas/<nombre>.md renderizado en el navegador."""
    png = IMG / f"{nombre}.png"
    if png.exists():
        st.image(str(png), width="stretch")
        return
    md = RAIZ / "docs" / "diagramas" / f"{nombre}.md"
    bloque = re.search(r"```mermaid\n(.*?)```", md.read_text(encoding="utf-8"), re.S) if md.exists() else None
    if not bloque:
        ui.vacio(f"Falta docs/diagramas/{nombre}.md")
        return
    import html
    # Contenido propio del repositorio (no proviene de usuarios): se puede incrustar como HTML.
    st.iframe(
        f"""<pre class="mermaid">{html.escape(bloque.group(1))}</pre>
        <script type="module">
          import mermaid from "https://cdn.jsdelivr.net/npm/mermaid@10.9.1/dist/mermaid.esm.min.mjs";
          mermaid.initialize({{startOnLoad: true, theme: "default", flowchart: {{useMaxWidth: true}}}});
        </script>""", height=alto)
    st.caption(f"Fuente: docs/diagramas/{nombre}.md")


def _optimizacion() -> None:
    hay = False
    indices = RESULTADOS / "mongo_indices.csv"
    if indices.exists():
        hay = True
        st.subheader("Índices en MongoDB (B2.5)")
        ui.tabla(pd.read_csv(indices), "mongo_indices")
        ui.etiqueta_origen(FUENTE_MONGO)
    pg = RESULTADOS / "postgres_optimizacion.csv"
    if pg.exists():
        hay = True
        st.subheader("Optimización PostgreSQL (A2.5)")
        ui.tabla(pd.read_csv(pg), "pg_opt")
        ui.etiqueta_origen(FUENTE_POSTGRES)
    spark = RESULTADOS / "spark_optimizacion.csv"
    if spark.exists():
        hay = True
        st.subheader("Optimización Spark (B4.3)")
        ui.tabla(pd.read_csv(spark), "spark_opt")
        ui.etiqueta_origen(FUENTE_SPARK)
    if not hay:
        ui.vacio("Sin resultados de optimización todavía.")


def mostrar() -> None:
    st.title("Comparación tecnológica")
    st.caption("¿Qué tecnología es la más apropiada para cada tipo de dato y cada necesidad de procesamiento dentro "
               "de una ciudad inteligente? Respuesta con experimentos propios.")
    matriz, exps, arq, preg, opt = st.tabs(["Matriz comparativa", "Experimentos E1–E7", "Arquitectura",
                                            "Preguntas del profesor", "Optimización por motor"])
    with matriz:
        texto = _seccion(DOC_04, "4.2")
        st.markdown(texto or "Pendiente: docs/tecnico/04_justificacion_tecnologica.md §4.2.")
        tipos = _seccion(DOC_04, "4.3")
        if tipos:
            st.markdown("#### Tipo de dato → tecnología\n\n" + tipos)
    with exps:
        _experimentos()
    with arq:
        for nombre, titulo, alto in (("arquitectura", "Arquitectura", 620), ("flujo_datos", "Flujo de datos", 1150),
                                     ("despliegue_docker", "Despliegue", 640)):
            st.markdown(f"**{titulo}**")
            _diagrama(nombre, alto)
    with preg:
        texto = _seccion(DOC_PREGUNTAS, "1.")
        st.markdown(texto or "Pendiente: docs/defensa/PREGUNTAS_RESPUESTAS.md.")
    with opt:
        _optimizacion()
