#!/usr/bin/env python3
"""E3 (anexo) — Diccionario de datos: cada atributo con su tipo, fuente y restricciones.

* PostgreSQL: se lee en vivo de information_schema / pg_catalog (columnas, tipos, NOT NULL, DEFAULT, PK, FK,
  UNIQUE, CHECK y COMMENT ON). No lista las particiones mensuales de medicion_horaria ni el esquema bench.
* MongoDB: validadores $jsonSchema de scripts/mongo_admin.py (la misma definición que se aplica a la base) más los
  campos opcionales conocidos de cada colección.
* HDFS / Spark: esquemas explícitos de los jobs.

  python scripts/generar_diccionario.py        # -> docs/atributos/DICCIONARIO_DATOS.md
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

import psycopg2

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ))
sys.path.insert(0, str(RAIZ / "scripts"))
from comun import CONFIG  # noqa: E402
from mongo_admin import ESQUEMAS  # noqa: E402

SALIDA = RAIZ / "docs" / "atributos" / "DICCIONARIO_DATOS.md"

FUENTE_TABLA = {
    "ubicaciones": "`config/zonas.yaml` (5 distritos de Londres) — `pg_cargar_maestros.py`",
    "categorias_sensor": "Semilla en `001_tablas.sql`",
    "tipos_sensor": "Semilla en `001_tablas.sql`",
    "organismos": "Catálogo ficticio plausible — `pg_cargar_maestros.py`",
    "sensores": "`data/sensores.csv` (estaciones OpenAQ + virtuales + T/E) — `pg_cargar_maestros.py`",
    "usuarios": "40 usuarios ficticios (`@smartcity.example`, semilla 2026) — `pg_cargar_maestros.py`",
    "variables_medicion": "Semilla en `002_extensiones.sql` (umbrales de `config/umbrales.yaml`)",
    "medicion_horaria": "Spark `carga_pg.py` (B3.4) desde el Parquet curado",
    "agg_ambiente_hora_zona": "Spark `carga_pg.py` (agregado zona-hora)",
    "spark_kpi_zona": "Spark SP-Q10 vía `carga_pg.py`",
    "spark_anomalia": "Spark `analitica.py --analisis anomalias` vía `carga_pg.py`",
    "calendario": "Generado en `006_calendario.sql` (feriados de Inglaterra 2023–2024)",
    "benchmark_resultado": "`pg_optimizacion.py` (A2.5) y `benchmark_comparacion.py` (C1)",
}

OPCIONALES_MONGO = {
    "lectura": [("origen", "string", "openaq · open-meteo · sintetico"),
                ("ubicacion.geo", "Point GeoJSON", "[lon, lat]; índice 2dsphere"),
                ("ubicacion.tipo_zona", "string", "mixta · residencial · comercial · industrial"),
                ("mediciones.<variable>", "double", "pm25, pm10, no2, o3, co, co2 · temperatura, humedad, "
                                                    "precipitacion, viento — solo las que mide cada tipo/firmware"),
                ("dispositivo.firmware", "string", "1.x / 2.x (2.x desde 2024-03-01 agrega o3, co2, bateria_pct)"),
                ("dispositivo.bateria_pct", "double", "solo firmware 2.x"),
                ("calidad_dato.valido", "bool", "false en anomalías inyectadas"),
                ("calidad_dato.flags", "array<string>", "pico, negativo, fuera_rango, pegado")],
    "evento": [("detalles", "object", "distinto por tipo de evento (p. ej. mm_h en lluvia_intensa)"),
               ("fin", "date", "cierre del evento"), ("id_sensor", "string", "cuando lo origina un sensor")],
    "alerta": [("regla", "string", "pm25_umbral, temperatura_umbral, stream_pm25, stream_congestion"),
               ("nivel", "string", "media · alta · critica (config/umbrales.yaml)"),
               ("origen", "string", "batch · streaming"),
               ("acciones", "array<object>", "historial de atención")],
    "incidente": [("reportes_ciudadanos", "array<object>", "largo variable"),
                  ("historial", "array<object>", "cambios de estado"),
                  ("heridos", "int", "solo accidentes"), ("adjuntos", "array", "~20 % de los incidentes")],
}

SQL_COLUMNAS = """
SELECT c.table_name, c.column_name, c.data_type, c.character_maximum_length, c.numeric_precision,
       c.numeric_scale, c.is_nullable, c.column_default, col_description(pc.oid, c.ordinal_position)
FROM information_schema.columns c
JOIN pg_class pc ON pc.relname = c.table_name
JOIN pg_namespace n ON n.oid = pc.relnamespace AND n.nspname = c.table_schema
WHERE c.table_schema = 'public' AND pc.relkind IN ('r', 'p') AND NOT pc.relispartition
ORDER BY c.table_name, c.ordinal_position"""

SQL_RESTRICCIONES = """
SELECT cl.relname, con.conname, con.contype, pg_get_constraintdef(con.oid),
       ARRAY(SELECT a.attname FROM unnest(con.conkey) k JOIN pg_attribute a
             ON a.attrelid = con.conrelid AND a.attnum = k)
FROM pg_constraint con JOIN pg_class cl ON cl.oid = con.conrelid
JOIN pg_namespace n ON n.oid = cl.relnamespace
WHERE n.nspname = 'public' AND NOT cl.relispartition
ORDER BY cl.relname, con.contype"""

SQL_COMENTARIOS = """
SELECT c.relname, obj_description(c.oid) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'public' AND c.relkind IN ('r', 'p') AND NOT c.relispartition"""


def tipo_pg(dt, largo, prec, escala):
    if dt == "character varying":
        return f"varchar({largo})" if largo else "varchar"
    if dt == "numeric" and prec:
        return f"numeric({prec},{escala})"
    if dt == "timestamp with time zone":
        return "timestamptz"
    if dt == "timestamp without time zone":
        return "timestamp"
    return dt


def seccion_pg():
    con = psycopg2.connect(host=CONFIG.pg_host, port=CONFIG.pg_port, dbname=CONFIG.pg_db, user=CONFIG.pg_user,
                           password=CONFIG.pg_password)
    with con, con.cursor() as cur:
        cur.execute(SQL_COLUMNAS)
        columnas = cur.fetchall()
        cur.execute(SQL_RESTRICCIONES)
        restricciones = cur.fetchall()
        cur.execute(SQL_COMENTARIOS)
        comentarios = dict(cur.fetchall())
    con.close()
    por_col: dict[tuple[str, str], list[str]] = {}
    for tabla, nombre, tipo, definicion, cols in restricciones:
        etiqueta = {"p": "PK", "u": "UNIQUE", "f": "FK", "c": "CHECK"}.get(tipo)
        if not etiqueta:
            continue
        if tipo == "f":
            texto = "FK → " + definicion.split("REFERENCES ", 1)[1]
        elif tipo == "c":
            texto = definicion.replace("CHECK ", "CHECK ")
        else:
            texto = etiqueta + (f" ({', '.join(cols)})" if len(cols) > 1 else "")
        for c in cols or []:
            por_col.setdefault((tabla, c), []).append(texto.replace("|", "\\|"))
    lineas = ["## 1. PostgreSQL (`smartcity_db`, esquema `public`)", "",
              "Generado en vivo desde el catálogo de la base. `medicion_horaria` está particionada por mes "
              "(24 particiones 2023-01…2024-12 + `DEFAULT`); las particiones heredan estas columnas y restricciones.", ""]
    actual = None
    for tabla, col, dt, largo, prec, escala, nulo, defecto, comentario in columnas:
        if tabla != actual:
            actual = tabla
            lineas += ["", f"### `{tabla}`", ""]
            if comentarios.get(tabla):
                lineas.append(f"{comentarios[tabla]}.  ")
            lineas += [f"**Fuente:** {FUENTE_TABLA.get(tabla, '—')}", "",
                       "| Atributo | Tipo | Nulo | Restricciones | Descripción |", "|---|---|---|---|---|"]
        rest = list(por_col.get((tabla, col), []))
        if defecto:
            rest.append(f"DEFAULT {defecto}")
        lineas.append(f"| `{col}` | {tipo_pg(dt, largo, prec, escala)} | {'sí' if nulo == 'YES' else 'no'} | "
                      f"{'<br>'.join(rest) or '—'} | {(comentario or '').replace('|', '/')} |")
    return lineas + [""]


def tipo_mongo(prop):
    if "enum" in prop:
        return "enum: " + ", ".join(map(str, prop["enum"]))
    t = prop.get("bsonType", "—")
    return " \\| ".join(t) if isinstance(t, list) else t


def seccion_mongo():
    lineas = ["## 2. MongoDB (`smartcity_db`)", "",
              "Validadores `$jsonSchema` con `validationLevel: moderate` (definidos en `scripts/mongo_admin.py` y "
              "`mongo/01_colecciones.js`): exigen solo los campos comunes; el resto del documento es libre, que es "
              "justamente lo que permite guardar lecturas de tipos y firmwares distintos sin migraciones.", ""]
    for coleccion, esquema in ESQUEMAS.items():
        req = set(esquema.get("required", []))
        lineas += [f"### `{coleccion}`", "", "| Atributo | Tipo | Obligatorio | Restricciones / notas |",
                   "|---|---|---|---|"]
        for nombre, prop in esquema.get("properties", {}).items():
            notas = ""
            if prop.get("bsonType") == "object" and prop.get("properties"):
                sub = ", ".join(f"{k}: {tipo_mongo(v)}" for k, v in prop["properties"].items())
                notas = f"subdocumento {{{sub}}}; obligatorios: {', '.join(prop.get('required', []))}"
            elif nombre == "mediciones":
                notas = "sin propiedades fijas: cada tipo/firmware trae las suyas"
            lineas.append(f"| `{nombre}` | {tipo_mongo(prop)} | {'sí' if nombre in req else 'no'} | {notas or '—'} |")
        for nombre in sorted(req - set(esquema.get("properties", {}))):
            lineas.append(f"| `{nombre}` | — | sí | — |")
        for nombre, tipo, nota in OPCIONALES_MONGO.get(coleccion, []):
            lineas.append(f"| `{nombre}` | {tipo} | no | {nota} |")
        lineas.append("")
    lineas += ["Colecciones auxiliares sin validador: `sensor` (catálogo para `$lookup`), `stream_ventana` (streaming), "
               "`demo_crud` (demostración CRUD) y `bench_*` (experimentos C1).", ""]
    return lineas


def seccion_hdfs_spark():
    return [
        "## 3. HDFS y Spark",
        "",
        "### Crudos en `/smartcity/raw` (inmutables)",
        "",
        "| Archivo | Atributos | Fuente |",
        "|---|---|---|",
        "| `openaq/<estación>/<año>.csv` | `timestamp_utc` (ISO 8601 UTC), `parameter` (pm25 · pm10 · no2 · o3 · co), "
        "`value` (double, ≥ 0 tras limpieza), `unit` | OpenAQ API v3 |",
        "| `openmeteo/zona_<zona>_<año>.json` | `hourly.time`, `temperature_2m` (°C), `relative_humidity_2m` (%), "
        "`precipitation` (mm), `wind_speed_10m` (km/h) | Open-Meteo |",
        "| `trafico/trafico_<año>.csv` | `timestamp_utc`, `zona`, `id_sensor` (T-00x), `vehiculos_hora` (int ≥ 0), "
        "`velocidad_promedio_kmh`, `congestion_pct` (0–100), `origen` | Sintético |",
        "| `energia/energia_<año>.csv` | `timestamp_utc`, `zona`, `id_sensor` (E-00x), `consumo_kwh` (≥ 0), `origen` | Sintético |",
        "| `mongo_exports/<colección>/fecha=…/*.jsonl` | Documento de MongoDB en JSON extendido (`$date`, `$oid`) | MongoDB |",
        "",
        "### Parquet curado (formato largo del contrato)",
        "",
        "| Atributo | Tipo Spark | Restricciones |",
        "|---|---|---|",
        "| `zona` | string | ∈ `contrato.ZONAS` |",
        "| `timestamp` | timestamp (UTC) | no nulo |",
        "| `id_sensor` | string | existe en `sensores` de PostgreSQL |",
        "| `tipo_metrica` | string | variable (pm25, temperatura, …) o trafico / energia |",
        "| `valor` | double | no nulo, no NaN, en rango físico de la variable |",
        "| `fuente` / `origen` | string | openaq · open-meteo · sintetico |",
        "| `anio`, `mes` | int | columnas de partición (`anio=AAAA/mes=MM`) |",
        "",
        "### Datos de benchmark (C1)",
        "",
        "`lectura_<n>` = `id_sensor` (string), `zona` (string), `tipo_metrica` (string), `timestamp` (UTC), "
        "`valor` (double, 2 decimales, ≥ 0). Mismo contenido en PostgreSQL (`bench.lectura_<n>`), MongoDB "
        "(`bench_lectura_<n>`) y HDFS (`/smartcity/sandbox/bench`).",
        "",
    ]


def main():
    lineas = ["# Diccionario de datos — SmartCity Analytics", "",
              f"Generado por `scripts/generar_diccionario.py` el {datetime.now(timezone.utc):%Y-%m-%d %H:%M} UTC. "
              "Para cada atributo: tipo, si admite nulos, restricciones y fuente.", ""]
    lineas += seccion_pg() + seccion_mongo() + seccion_hdfs_spark()
    SALIDA.parent.mkdir(parents=True, exist_ok=True)
    SALIDA.write_text("\n".join(lineas), encoding="utf-8")
    print(f"-> {SALIDA.relative_to(RAIZ).as_posix()}")


if __name__ == "__main__":
    main()
