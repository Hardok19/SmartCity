#!/usr/bin/env python3
"""A2.5 — Optimización en PostgreSQL con EXPLAIN ANALYZE (y experimento E7 de la matriz comparativa).

Cuatro casos, cada uno con 1 calentamiento + N repeticiones (mediana):
  1. Índice B-tree (id_sensor, timestamp DESC) frente a recorrido secuencial (enable_indexscan/bitmapscan off).
  2. Poda de particiones: filtro directo sobre `timestamp` frente a una expresión que impide la poda.
  3. BRIN frente a B-tree en `timestamp`: tamaño del índice y tiempo de una consulta por rango.
  4. Vista materializada frente a la misma agregación en vivo sobre medicion_horaria.
Resultados: tabla benchmark_resultado, docs/resultados/postgres_optimizacion.md y .csv.

    python scripts/pg_optimizacion.py --repeticiones 5
"""
import argparse
import csv
import re
import statistics
import sys
from datetime import datetime, timezone
from pathlib import Path

import psycopg2

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ))
from comun import CONFIG  # noqa: E402


def num(v, d=0):
    return f"{v:,.{d}f}".replace(",", "_").replace(".", ",").replace("_", ".")


def explain(cur, sql, ajustes=()):
    """Ejecuta EXPLAIN ANALYZE dentro de una transacción que se revierte (los SET y DROP no persisten)."""
    cur.execute("BEGIN")
    try:
        for a in ajustes:
            cur.execute(a)
        cur.execute("EXPLAIN (ANALYZE, BUFFERS) " + sql)
        plan = "\n".join(r[0] for r in cur.fetchall())
    finally:
        cur.execute("ROLLBACK")
    ms = float(re.search(r"Execution Time: ([\d.]+) ms", plan).group(1))
    return ms, plan


def medir(cur, sql, ajustes, reps):
    explain(cur, sql, ajustes)                                   # calentamiento (caché)
    corridas = [explain(cur, sql, ajustes) for _ in range(reps)]
    return statistics.median(c[0] for c in corridas), corridas[-1][1]


def lineas_clave(plan, patron=r"Scan|Append|Subplans Removed|Index Cond|Recheck|Rows Removed|Execution Time|Heap Blocks"):
    return "\n".join(ln for ln in plan.splitlines() if re.search(patron, ln))


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--repeticiones", type=int, default=5)
    a = ap.parse_args()
    con = psycopg2.connect(host=CONFIG.pg_host, port=CONFIG.pg_port, dbname=CONFIG.pg_db, user=CONFIG.pg_user,
                           password=CONFIG.pg_password)
    con.autocommit = True
    cur = con.cursor()
    cur.execute("SELECT COUNT(*) FROM medicion_horaria")
    filas = cur.fetchone()[0]
    if filas == 0:
        sys.exit("medicion_horaria está vacía: ejecutar antes spark/jobs/carga_pg.py")
    cur.execute("SELECT id_sensor FROM medicion_horaria GROUP BY id_sensor ORDER BY COUNT(*) DESC LIMIT 1")
    sensor = cur.fetchone()[0]
    cur.execute("SELECT max(timestamp) FROM medicion_horaria")
    ultimo = cur.fetchone()[0]
    mes = ultimo.strftime("%Y-%m-01")
    cur.execute("ANALYZE medicion_horaria")

    casos = []
    # 1. índice B-tree compuesto vs recorrido secuencial
    q1 = f"SELECT timestamp, variable, valor FROM medicion_horaria WHERE id_sensor = '{sensor}' ORDER BY timestamp DESC LIMIT 50"
    sin = medir(cur, q1, ["SET LOCAL enable_indexscan = off", "SET LOCAL enable_bitmapscan = off"], a.repeticiones)
    con_i = medir(cur, q1, [], a.repeticiones)
    casos.append(("A2.5-1", "Últimas 50 lecturas de un sensor", q1, ("sin_indice", sin), ("con_indice_btree", con_i)))

    # 2. poda de particiones
    q2a = (f"SELECT variable, AVG(valor) FROM medicion_horaria WHERE date_trunc('month', timestamp AT TIME ZONE 'UTC') = '{mes}' "
           "GROUP BY variable")
    q2b = (f"SELECT variable, AVG(valor) FROM medicion_horaria WHERE timestamp >= '{mes}'::timestamptz "
           f"AND timestamp < '{mes}'::timestamptz + interval '1 month' GROUP BY variable")
    casos.append(("A2.5-2", "Promedio por variable de un mes", q2b, ("sin_poda_expresion", medir(cur, q2a, [], a.repeticiones)),
                  ("con_poda_particiones", medir(cur, q2b, [], a.repeticiones))))

    # 3. BRIN vs B-tree sobre timestamp (índices temporales dentro de la transacción revertida)
    q3 = f"SELECT COUNT(*), AVG(valor) FROM medicion_horaria WHERE timestamp >= '{mes}'::timestamptz - interval '3 months' AND timestamp < '{mes}'::timestamptz"
    cur.execute("BEGIN")
    cur.execute("CREATE INDEX idx_tmp_btree_ts ON medicion_horaria (timestamp)")
    cur.execute("""SELECT
        SUM(pg_relation_size(ci.oid)) FILTER (WHERE am.amname = 'brin'),
        SUM(pg_relation_size(ci.oid)) FILTER (WHERE am.amname = 'btree' AND ci.relname LIKE '%%timestamp_idx%%')
        FROM pg_inherits h JOIN pg_class p ON p.oid = h.inhparent
        JOIN pg_index i ON i.indrelid = h.inhrelid JOIN pg_class ci ON ci.oid = i.indexrelid
        JOIN pg_am am ON am.oid = ci.relam
        WHERE p.relname = 'medicion_horaria'""")
    bytes_brin, bytes_btree = cur.fetchone()
    cur.execute("ROLLBACK")
    brin = medir(cur, q3, ["SET LOCAL enable_seqscan = off", "DROP INDEX idx_medicion_variable_fecha",
                           "DROP INDEX idx_medicion_sensor_fecha"], a.repeticiones)
    btree = medir(cur, q3, ["SET LOCAL enable_seqscan = off", "CREATE INDEX idx_tmp_btree_ts ON medicion_horaria (timestamp)",
                            "DROP INDEX idx_medicion_fecha_brin", "DROP INDEX idx_medicion_variable_fecha",
                            "DROP INDEX idx_medicion_sensor_fecha"], a.repeticiones)
    secuencial = medir(cur, q3, [], a.repeticiones)
    casos.append(("A2.5-3", "Conteo y promedio de 3 meses", q3, ("btree_timestamp", btree), ("brin_timestamp", brin),
                  ("plan_elegido_por_postgres", secuencial)))

    # 4. vista materializada vs consulta en vivo
    q4a = ("SELECT u.nombre_zona, (m.timestamp AT TIME ZONE 'UTC')::date AS fecha, COUNT(*) FROM medicion_horaria m "
           "JOIN sensores s USING (id_sensor) JOIN ubicaciones u USING (id_ubicacion) GROUP BY 1, 2")
    q4b = "SELECT zona, fecha, mediciones FROM mv_kpi_diario_zona"
    casos.append(("A2.5-4", "Mediciones por zona y día", q4b, ("consulta_en_vivo", medir(cur, q4a, [], a.repeticiones)),
                  ("vista_materializada", medir(cur, q4b, [], a.repeticiones))))

    # ---- guardar
    ahora = datetime.now(timezone.utc)
    filas_csv = []
    for ident, desc, _, *variantes in casos:
        for nombre, (ms, _) in variantes:
            cur.execute("INSERT INTO benchmark_resultado (experimento, tecnologia, variante, escala_filas, segundos, detalle) "
                        "VALUES (%s, 'PostgreSQL', %s, %s, %s, %s)", (ident, nombre, filas, ms / 1000, desc))
            filas_csv.append({"experimento": ident, "descripcion": desc, "variante": nombre, "escala_filas": filas,
                              "ms_mediana": round(ms, 3)})
    destino = RAIZ / "docs" / "resultados"
    destino.mkdir(parents=True, exist_ok=True)
    with open(destino / "postgres_optimizacion.csv", "w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(filas_csv[0]))
        w.writeheader()
        w.writerows(filas_csv)

    md = [f"# Optimización en PostgreSQL (A2.5)", "",
          f"Generado por `scripts/pg_optimizacion.py` el {ahora:%Y-%m-%d %H:%M} UTC sobre `medicion_horaria` con "
          f"**{num(filas)} filas** (24 particiones mensuales + DEFAULT). Mediana de {a.repeticiones} repeticiones tras "
          "1 calentamiento; cada `EXPLAIN ANALYZE` corre en una transacción revertida (los `SET` y `DROP INDEX` de "
          "prueba no persisten).", "",
          "| Caso | Variante | Tiempo (ms, mediana) |", "|---|---|---:|"]
    for ident, desc, _, *variantes in casos:
        for nombre, (ms, _) in variantes:
            md.append(f"| {ident} {desc} | {nombre} | {num(ms, 3)} |")
    md += ["", f"Tamaño de los índices sobre `timestamp` (suma de las particiones): **BRIN {num(bytes_brin / 1024, 0)} KB** "
           f"frente a **B-tree {num(bytes_btree / 1024, 0)} KB** (×{num(bytes_btree / max(bytes_brin, 1), 0)}).", ""]
    for ident, desc, sql, *variantes in casos:
        md += [f"## {ident} — {desc}", "", "```sql", sql, "```", ""]
        for nombre, (ms, plan) in variantes:
            md += [f"**{nombre}** ({num(ms, 3)} ms)", "", "```", lineas_clave(plan), "```", ""]
    (destino / "postgres_optimizacion.md").write_text("\n".join(md), encoding="utf-8")
    print("\n".join(md[:20 + 3 * len(casos)]))
    con.close()


if __name__ == "__main__":
    main()
