#!/usr/bin/env python3
"""F2.3 — Perfilado de las fuentes en data/raw (y de los JSONL de data/interim/mongo si existen).

Por fuente y variable: archivos, tamaño en disco, filas, rango de fechas, frecuencia, nulos, mínimo y máximo.
Las fuentes que todavía no están en disco aparecen como «no descargada» (el script se puede volver a correr).
Escribe docs/datos/perfil_fuentes.md, que complementa docs/datos/FUENTES.md.

  python scripts/perfilar_fuentes.py
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

RAIZ = Path(__file__).resolve().parents[1]
RAW = RAIZ / "data" / "raw"
INTERIM = RAIZ / "data" / "interim" / "mongo"
SALIDA = RAIZ / "docs" / "datos" / "perfil_fuentes.md"


def mb(nbytes):
    return f"{nbytes / 2**20:,.1f} MB".replace(",", "X").replace(".", ",").replace("X", ".")


def n(x):
    return f"{int(x):,}".replace(",", ".")


def num(x, d=2):
    if pd.isna(x):
        return "—"
    return f"{x:,.{d}f}".replace(",", "X").replace(".", ",").replace("X", ".")


def frecuencia(ts: pd.Series) -> str:
    dif = ts.sort_values().diff().dropna()
    if dif.empty:
        return "—"
    moda = dif.mode().iloc[0]
    minutos = int(moda.total_seconds() // 60)
    return f"{minutos // 60} h" if minutos % 60 == 0 else f"{minutos} min"


def perfil_variables(df, col_ts, columnas, agrupar=None):
    """Una fila por variable (y grupo): filas, nulos, % nulos, min, max."""
    filas = []
    grupos = df.groupby(agrupar) if agrupar else [(None, df)]
    for clave, g in grupos:
        for c in columnas:
            serie = pd.to_numeric(g[c], errors="coerce")
            filas.append({"grupo": clave, "variable": c, "filas": len(g), "nulos": int(serie.isna().sum()),
                          "min": serie.min(), "max": serie.max(),
                          "desde": g[col_ts].min(), "hasta": g[col_ts].max(), "frecuencia": frecuencia(g[col_ts])})
    return filas


def tabla(filas, con_grupo=True, nombre_grupo="Grupo"):
    cab = ([nombre_grupo] if con_grupo else []) + ["Variable", "Filas", "Nulos", "% nulos", "Mín", "Máx", "Desde",
                                                     "Hasta", "Frecuencia"]
    lineas = ["| " + " | ".join(cab) + " |", "|" + "---|" * len(cab)]
    for f in filas:
        pct = 100 * f["nulos"] / f["filas"] if f["filas"] else 0
        celdas = ([str(f["grupo"])] if con_grupo else []) + [
            f["variable"], n(f["filas"]), n(f["nulos"]), num(pct, 1), num(f["min"]), num(f["max"]),
            str(f["desde"])[:16], str(f["hasta"])[:16], f["frecuencia"]]
        lineas.append("| " + " | ".join(celdas) + " |")
    return lineas


def seccion_csv(titulo, carpeta: Path, col_ts, columnas, agrupar):
    archivos = sorted(carpeta.glob("**/*.csv")) if carpeta.exists() else []
    lineas = [f"## {titulo}", ""]
    if not archivos:
        return lineas + [f"**No descargada** (`{carpeta.relative_to(RAIZ).as_posix()}` vacío). Volver a correr el script "
                         "después de obtener los datos.", ""]
    partes = []
    for a in archivos:
        d = pd.read_csv(a)
        if agrupar == "estacion":
            d["estacion"] = a.parent.name
        partes.append(d)
    df = pd.concat(partes, ignore_index=True)
    df[col_ts] = pd.to_datetime(df[col_ts], utc=True)
    tam = sum(a.stat().st_size for a in archivos)
    lineas += [f"Archivos: **{len(archivos)}** · tamaño: **{mb(tam)}** · filas: **{n(len(df))}** · "
               f"columnas: `{', '.join(c for c in df.columns if c != 'estacion')}`", ""]
    if agrupar == "estacion":                       # OpenAQ viene en formato largo (parameter, value)
        filas = []
        for (est, par), g in df.groupby(["estacion", "parameter"]):
            filas += [{**f, "grupo": est, "variable": par}
                      for f in perfil_variables(g.rename(columns={"value": par}), col_ts, [par])]
        lineas += tabla(filas, nombre_grupo="Estación")
    else:
        lineas += tabla(perfil_variables(df, col_ts, columnas, agrupar), nombre_grupo="Zona")
    duplicados = int(df.duplicated(subset=[c for c in df.columns if c not in columnas]).sum())
    return lineas + ["", f"Filas duplicadas por clave (todas las columnas no numéricas): **{n(duplicados)}**.", ""]


def seccion_openmeteo():
    carpeta = RAW / "openmeteo"
    archivos = sorted(carpeta.glob("*.json")) if carpeta.exists() else []
    lineas = ["## Open-Meteo (clima histórico)", ""]
    if not archivos:
        return lineas + ["**No descargada** (`data/raw/openmeteo` vacío). Volver a correr el script después de "
                         "obtener los datos.", ""]
    filas, tam = [], 0
    for a in archivos:
        tam += a.stat().st_size
        datos = json.loads(a.read_text(encoding="utf-8"))
        h = pd.DataFrame(datos["hourly"])
        h["time"] = pd.to_datetime(h["time"], utc=True)
        zona = a.stem.replace("zona_", "").rsplit("_", 1)[0]
        filas += perfil_variables(h.assign(zona=zona), "time", [c for c in h.columns if c not in ("time", "zona")],
                                  "zona")
    df = pd.DataFrame(filas)
    agg = (df.groupby(["grupo", "variable"])
           .agg(filas=("filas", "sum"), nulos=("nulos", "sum"), min=("min", "min"), max=("max", "max"),
                desde=("desde", "min"), hasta=("hasta", "max"), frecuencia=("frecuencia", "first"))
           .reset_index().to_dict("records"))
    return lineas + [f"Archivos: **{len(archivos)}** · tamaño: **{mb(tam)}**", ""] + tabla(agg, nombre_grupo="Zona") + [""]


def seccion_mongo():
    archivos = sorted(INTERIM.glob("**/*.jsonl")) if INTERIM.exists() else []
    lineas = ["## Documentos generados para MongoDB (`data/interim/mongo`)", ""]
    if not archivos:
        return lineas + ["**No generados en esta copia** (ver `scripts/generar_sinteticos.py`).", ""]
    lineas += ["| Archivo | Documentos | Tamaño |", "|---|---|---|"]
    for a in archivos:
        with a.open("rb") as f:
            docs = sum(1 for _ in f)
        lineas.append(f"| `{a.name}` | {n(docs)} | {mb(a.stat().st_size)} |")
    return lineas + [""]


def main():
    lineas = ["# Perfil de las fuentes (F2.3)", "",
              f"Generado por `scripts/perfilar_fuentes.py` el {datetime.now(timezone.utc):%Y-%m-%d %H:%M} UTC. "
              "Descripción, licencias y destino de cada fuente: [FUENTES.md](FUENTES.md).", ""]
    lineas += seccion_csv("OpenAQ (calidad del aire)", RAW / "openaq", "timestamp_utc", ["value"], "estacion")
    lineas += seccion_openmeteo()
    lineas += seccion_csv("Tráfico simulado", RAW / "trafico", "timestamp_utc",
                          ["vehiculos_hora", "velocidad_promedio_kmh", "congestion_pct"], "zona")
    lineas += seccion_csv("Energía simulada", RAW / "energia", "timestamp_utc", ["consumo_kwh"], "zona")
    lineas += seccion_mongo()
    SALIDA.write_text("\n".join(lineas), encoding="utf-8")
    print(f"-> {SALIDA.relative_to(RAIZ)}")


if __name__ == "__main__":
    main()
