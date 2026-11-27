#!/usr/bin/env python3
"""B2.5 / E7: experimento de índices en MongoDB con explain("executionStats").

Para MG-Q01, MG-Q02 y MG-Q05 (parámetros por defecto de mongo/queries) compara:
  * con índice   -> plan elegido por MongoDB (IXSCAN esperado)
  * sin índice   -> hint({$natural: 1}) (COLLSCAN forzado)
Métricas: etapa ganadora, totalDocsExamined, totalKeysExamined, nReturned, executionTimeMillis
(mediana de N repeticiones después de 1 calentamiento). Escribe docs/resultados/mongo_indices.md
y docs/resultados/mongo_indices.csv.

  python scripts/mongo_experimento_indices.py --repeticiones 3
"""
import argparse
import csv
import os
import statistics
import sys
from datetime import datetime, timezone
from pathlib import Path

from pymongo import MongoClient

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

RAIZ = Path(__file__).resolve().parents[1]
URI_DEFECTO = "mongodb://smartcity:smartcity123@localhost:27017/smartcity_db?authSource=admin"
D = lambda *a: datetime(*a, tzinfo=timezone.utc)  # noqa: E731

CONSULTAS = {
    "MG-Q01": {
        "descripcion": "Lecturas de un sensor (S-011), últimas 50",
        "indice": "ix_sensor_ts",
        "comando": {"find": "lectura", "filter": {"sensor_id": "S-011"}, "sort": {"timestamp": -1}, "limit": 50,
                    "projection": {"_id": 0, "timestamp": 1, "mediciones": 1}},
    },
    "MG-Q02": {
        "descripcion": "Lecturas de Zona Norte del 2024-04-01",
        "indice": "ix_tipo_zona_ts / ix_ts",
        "comando": {"find": "lectura",
                    "filter": {"ubicacion.zona": "Zona Norte",
                               "timestamp": {"$gte": D(2024, 4, 1), "$lte": D(2024, 4, 1, 23, 59, 59)}},
                    "sort": {"timestamp": 1}, "limit": 500,
                    "projection": {"_id": 0, "sensor_id": 1, "timestamp": 1, "mediciones": 1}},
    },
    "MG-Q05": {
        "descripcion": "Alertas por zona entre 2024-02-15 y 2024-06-15",
        "indice": "ix_zona_emitida",
        "comando": {"aggregate": "alerta", "cursor": {},
                    "pipeline": [
                        {"$match": {"emitida": {"$gte": D(2024, 2, 15), "$lte": D(2024, 6, 15, 23, 59, 59)}}},
                        {"$group": {"_id": "$zona.nombre", "alertas": {"$sum": 1}}}]},
    },
}


def num(valor, decimales=0):
    """Formato es-CR: 1.234.567 / 12,5."""
    texto = f"{valor:,.{decimales}f}"
    return texto.replace(",", "_").replace(".", ",").replace("_", ".")


def buscar(nodo, clave):
    """Primer valor de ``clave`` en una estructura anidada de explain."""
    if isinstance(nodo, dict):
        if clave in nodo:
            return nodo[clave]
        for v in nodo.values():
            r = buscar(v, clave)
            if r is not None:
                return r
    elif isinstance(nodo, list):
        for v in nodo:
            r = buscar(v, clave)
            if r is not None:
                return r
    return None


def etapas(nodo, acc=None):
    acc = [] if acc is None else acc
    if isinstance(nodo, dict):
        if "stage" in nodo:
            acc.append(nodo["stage"])
        for v in nodo.values():
            etapas(v, acc)
    elif isinstance(nodo, list):
        for v in nodo:
            etapas(v, acc)
    return acc


def explicar(db, comando, sin_indice):
    cmd = dict(comando)
    if sin_indice:
        cmd["hint"] = {"$natural": 1}
    r = db.command("explain", cmd, verbosity="executionStats")
    stats = buscar(r, "executionStats") or {}
    plan = buscar(r, "winningPlan") or {}
    nombres = etapas(plan)
    acceso = "IXSCAN" if "IXSCAN" in nombres else "COLLSCAN" if "COLLSCAN" in nombres else "/".join(nombres[:3])
    return {
        "etapa": acceso,
        "docs": int(stats.get("totalDocsExamined", 0)),
        "claves": int(stats.get("totalKeysExamined", 0)),
        "devueltos": int(stats.get("nReturned", 0)),
        "ms": float(stats.get("executionTimeMillis", 0)),
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--uri", default=os.environ.get("MONGO_URI", URI_DEFECTO))
    ap.add_argument("--db", default=os.environ.get("MONGO_DB", "smartcity_db"))
    ap.add_argument("--repeticiones", type=int, default=3)
    a = ap.parse_args()
    db = MongoClient(a.uri, serverSelectionTimeoutMS=5000)[a.db]
    try:
        db.command("ping")
    except Exception as e:  # noqa: BLE001
        sys.exit(f"No se pudo conectar a MongoDB: {e}")

    filas = []
    for ident, q in CONSULTAS.items():
        for variante, sin in (("con_indice", False), ("sin_indice", True)):
            explicar(db, q["comando"], sin)                       # calentamiento
            corridas = [explicar(db, q["comando"], sin) for _ in range(a.repeticiones)]
            fila = dict(corridas[-1])
            fila["ms"] = statistics.median(c["ms"] for c in corridas)
            fila.update(consulta=ident, variante=variante, indice=q["indice"], descripcion=q["descripcion"])
            filas.append(fila)
            print(f"{ident} {variante:10s} {fila['etapa']:9s} docs={fila['docs']:>9,} claves={fila['claves']:>9,} "
                  f"devueltos={fila['devueltos']:>5} mediana={fila['ms']:.0f} ms")

    destino = RAIZ / "docs" / "resultados"
    destino.mkdir(parents=True, exist_ok=True)
    campos = ["consulta", "variante", "indice", "etapa", "docs", "claves", "devueltos", "ms", "descripcion"]
    with open(destino / "mongo_indices.csv", "w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=campos)
        w.writeheader()
        w.writerows({k: f[k] for k in campos} for f in filas)

    total = db.lectura.estimated_document_count()
    lineas = [
        "# Experimento de índices en MongoDB (B2.5 / E7)", "",
        f"Generado por `scripts/mongo_experimento_indices.py` el {datetime.now(timezone.utc):%Y-%m-%d %H:%M} UTC. "
        f"`lectura` = {num(total)} documentos. Mediana de {a.repeticiones} repeticiones tras 1 calentamiento; "
        "«sin índice» = `hint({$natural: 1})` (recorrido completo forzado).", "",
        "| Consulta | Variante | Índice esperado | Etapa | Docs examinados | Claves examinadas | Devueltos | Tiempo (ms) |",
        "|---|---|---|---|---:|---:|---:|---:|",
    ]
    for f in filas:
        lineas.append(f"| {f['consulta']} | {f['variante']} | {f['indice']} | {f['etapa']} | {num(f['docs'])} | "
                      f"{num(f['claves'])} | {num(f['devueltos'])} | {num(f['ms'])} |")
    lineas += ["", "## Lectura de resultados", ""]
    for ident in CONSULTAS:
        con = next(f for f in filas if f["consulta"] == ident and f["variante"] == "con_indice")
        sin = next(f for f in filas if f["consulta"] == ident and f["variante"] == "sin_indice")
        factor_docs = sin["docs"] / max(con["docs"], 1)
        factor_ms = sin["ms"] / max(con["ms"], 1)
        lineas.append(f"- **{ident}** ({CONSULTAS[ident]['descripcion']}): con índice examina {num(con['docs'])} documentos "
                      f"frente a {num(sin['docs'])} sin índice (×{num(factor_docs, 1)}); tiempo ×{num(factor_ms, 1)}.")
    (destino / "mongo_indices.md").write_text("\n".join(lineas) + "\n", encoding="utf-8")
    print(f"\nEscrito {destino / 'mongo_indices.md'}")


if __name__ == "__main__":
    main()
