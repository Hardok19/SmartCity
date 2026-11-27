#!/usr/bin/env python3
"""mongo_cargar.py — B2.1. Carga data/interim/mongo/*.jsonl (salida de generar_sinteticos.py) en MongoDB.

Orden recomendado (raíz del repo):
  1. python scripts/mongo_admin.py colecciones     (validadores; equivale a mongo/01_colecciones.js)
  2. python scripts/mongo_cargar.py --reiniciar
  3. python scripts/mongo_admin.py indices         (crear índices DESPUÉS de cargar es mucho más rápido)

Conexión: variable MONGO_URI (o .env). El valor por defecto coincide con docker/docker-compose.yml:
  usuario smartcity / contraseña smartcity123 (usuario root, creado en la base `admin`), base smartcity_db
  mongodb://smartcity:smartcity123@localhost:27017/smartcity_db?authSource=admin
Si tienes un .env antiguo con otra MONGO_URI, esa tiene prioridad: bórrala o actualízala.
Idempotente con --reiniciar (vacía las 4 colecciones con delete_many; conserva validadores e índices).
"""
import argparse
import os
import sys
import time
from datetime import datetime
from pathlib import Path

from bson import json_util
from pymongo import MongoClient
from pymongo.errors import BulkWriteError, OperationFailure, ServerSelectionTimeoutError

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

RAIZ = Path(__file__).resolve().parents[1]
COLECCIONES = ["lectura", "alerta", "evento", "incidente"]
URI_DEFECTO = "mongodb://smartcity:smartcity123@localhost:27017/smartcity_db?authSource=admin"
DB_DEFECTO = "smartcity_db"
# Campo fecha que el validador exige como BSON date en cada colección
CAMPO_FECHA = {"lectura": "timestamp", "alerta": "emitida", "evento": "inicio", "incidente": "reportado"}


def lotes(ruta, tam, limite):
    lote, n = [], 0
    with open(ruta, encoding="utf-8") as f:
        for linea in f:
            if not linea.strip():
                continue
            lote.append(json_util.loads(linea))
            n += 1
            if len(lote) == tam:
                yield lote
                lote = []
            if limite and n >= limite:
                break
    if lote:
        yield lote


def verificar_formato(col, ruta):
    """Revisa el primer documento: si las fechas vienen como texto el validador rechazaría TODO el archivo."""
    with open(ruta, encoding="utf-8") as f:
        primera = next((l for l in f if l.strip()), None)
    if primera is None:
        sys.exit(f"{ruta} está vacío.")
    doc = json_util.loads(primera)
    campo = CAMPO_FECHA[col]
    if not isinstance(doc.get(campo), datetime):
        sys.exit(
            f"[{col}] '{campo}' no es una fecha BSON (es {type(doc.get(campo)).__name__}). "
            f'generar_sinteticos.py debe escribir las fechas como {{"$date": "2025-01-31T12:00:00Z"}} (Extended JSON).'
        )
    if col == "lectura" and not (doc.get("ubicacion") or {}).get("geo"):
        print(f"[{col}] AVISO: ubicacion.geo ausente en el primer documento; $geoNear/2dsphere no lo verán. "
              "Formato esperado: {\"type\": \"Point\", \"coordinates\": [lon, lat]}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", default=str(RAIZ / "data/interim/mongo"))
    ap.add_argument("--uri", default=os.environ.get("MONGO_URI", URI_DEFECTO))
    ap.add_argument("--db", default=os.environ.get("MONGO_DB", DB_DEFECTO))
    ap.add_argument("--lote", type=int, default=10_000)
    ap.add_argument("--limite", type=int, help="máx. documentos por colección (pruebas)")
    ap.add_argument("--reiniciar", action="store_true", help="vaciar las colecciones antes de cargar")
    ap.add_argument("--colecciones", nargs="+", default=COLECCIONES, choices=COLECCIONES)
    a = ap.parse_args()

    db = MongoClient(a.uri, serverSelectionTimeoutMS=5000)[a.db]
    try:
        db.command("ping")
        db.list_collection_names()   # falla aquí (no al insertar) si las credenciales no sirven
    except ServerSelectionTimeoutError:
        sys.exit("No se pudo conectar a MongoDB. ¿Está arriba el contenedor? (docker compose up -d mongo)")
    except OperationFailure as e:
        sys.exit(f"MongoDB rechazó las credenciales: {e.details.get('errmsg', e)}\n"
                 "Compose: usuario smartcity / contraseña smartcity123, authSource=admin. Revisa MONGO_URI o tu .env.")

    total_t = time.time()
    for col in a.colecciones:
        ruta = Path(a.dir) / f"{col}.jsonl"
        if not ruta.exists():
            sys.exit(f"No existe {ruta}. Corran primero scripts/generar_sinteticos.py")
        verificar_formato(col, ruta)
        if a.reiniciar:
            borrados = db[col].delete_many({}).deleted_count
            print(f"[{col}] vaciada ({borrados} documentos)")
        t0, ok, rechazados = time.time(), 0, 0
        for lote in lotes(ruta, a.lote, a.limite):
            try:
                ok += len(db[col].insert_many(lote, ordered=False).inserted_ids)
            except BulkWriteError as e:
                ok += e.details["nInserted"]
                rechazados += len(e.details["writeErrors"])
                if rechazados <= 3:
                    print("\n  rechazado:", e.details["writeErrors"][0]["errmsg"][:200])
            print(f"\r[{col}] {ok:,} insertados", end="", flush=True)
        seg = time.time() - t0
        print(f"\r[{col}] {ok:,} insertados, {rechazados} rechazados, {seg:.1f} s ({ok / max(seg, 1e-9):,.0f} doc/s)")
    print("\nConteos en MongoDB:")
    for col in COLECCIONES:
        print(f"  {col:10s} {db[col].estimated_document_count():>10,}")
    if "lectura" in a.colecciones and not a.limite:
        n = db["lectura"].estimated_document_count()
        print(f"\nAceptación B2.1 (lectura ≥ 500.000): {'OK' if n >= 500_000 else 'FALTA'}  · tiempo total {time.time() - total_t:.0f} s")
    else:
        print(f"\nTiempo total {time.time() - total_t:.0f} s")


if __name__ == "__main__":
    main()
