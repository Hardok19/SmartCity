#!/usr/bin/env python3
"""Copia fuentes originales a /smartcity/raw en HDFS (no toca curated).

Ejemplo: python scripts/subir_crudos_hdfs.py --fuentes openaq openmeteo trafico
         python scripts/subir_crudos_hdfs.py --fuentes mongo --colecciones lectura

Para Mongo: MONGO_URI debe apuntar a localhost (Python del anfitrion).
No incluye credenciales en la linea de comandos ni en el log.
"""
import argparse
import hashlib
import os
import subprocess
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
CONTENEDOR = "smartcity_namenode"
HDFS_BASE = "/smartcity/raw"


def ejecutar(*args, comprobar=True):
    resultado = subprocess.run(args, text=True, capture_output=True)
    if comprobar and resultado.returncode:
        raise RuntimeError(f"Comando fallido ({resultado.returncode}): {args[0]} {args[1]} ...\n{resultado.stderr.strip()[:800]}")
    return resultado


def hdfs_existe(destino):
    return ejecutar("docker", "exec", CONTENEDOR, "hadoop", "fs", "-test", "-e", destino,
                   comprobar=False).returncode == 0


def hdfs_bytes(destino):
    salida = ejecutar("docker", "exec", CONTENEDOR, "hadoop", "fs", "-stat", "%b", destino)
    return int(salida.stdout.strip())


def copiar(local, destino, sobrescribir=False):
    if not destino.startswith(HDFS_BASE + "/"):
        raise ValueError("Solo se permite subir datos a /smartcity/raw")
    local = Path(local)
    if not local.is_file() or local.stat().st_size == 0:
        raise RuntimeError(f"Archivo inexistente o vacio: {local}")
    if hdfs_existe(destino) and not sobrescribir:
        tam_remoto = hdfs_bytes(destino)
        if tam_remoto != local.stat().st_size:
            raise RuntimeError(f"Ya existe {destino} con otro tamano; use --sobrescribir si es intencional")
        print(f"SMARTCITY_RAW_EXISTE={destino} bytes={tam_remoto}", flush=True)
        return
    temporal = f"/tmp/smartcity_raw_{uuid.uuid4().hex}"
    try:
        ejecutar("docker", "cp", str(local.resolve()), f"{CONTENEDOR}:{temporal}")
        padre = destino.rsplit("/", 1)[0]
        ejecutar("docker", "exec", CONTENEDOR, "hadoop", "fs", "-mkdir", "-p", padre)
        ejecutar("docker", "exec", CONTENEDOR, "hadoop", "fs", "-put", "-f", temporal, destino)
    finally:
        ejecutar("docker", "exec", CONTENEDOR, "rm", "-f", temporal, comprobar=False)
    actual = hdfs_bytes(destino)
    if actual != local.stat().st_size:
        raise RuntimeError(f"No coinciden los bytes en {destino}: local {local.stat().st_size}, HDFS {actual}")
    print(f"SMARTCITY_RAW_OK={destino} bytes={actual}", flush=True)


def exportar_mongo(coleccion, destino_local):
    try:
        from pymongo import MongoClient
        from bson import json_util
    except ImportError as error:
        raise RuntimeError("Faltan pymongo y bson: python -m pip install pymongo") from error
    uri = os.getenv("MONGO_URI")
    if not uri:
        raise RuntimeError("Falta MONGO_URI para exportar MongoDB; no usar --mongo-uri en la terminal")
    cliente = MongoClient(uri, serverSelectionTimeoutMS=10_000)
    try:
        db = cliente[os.getenv("MONGO_DB", "smartcity_db")]
        db.command("ping")
        total = db[coleccion].count_documents({})
        escritos = 0
        with open(destino_local, "w", encoding="utf-8", newline="\n") as archivo:
            for documento in db[coleccion].find({}, no_cursor_timeout=True).batch_size(2000):
                archivo.write(json_util.dumps(documento, ensure_ascii=False) + "\n")
                escritos += 1
        if total != escritos:
            raise RuntimeError(f"Mongo {coleccion}: conteo {total}, exportados {escritos}; base cambiada durante la exportacion")
        print(f"SMARTCITY_MONGO_EXPORT={coleccion} documentos={escritos}", flush=True)
        return escritos
    finally:
        cliente.close()


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--fuentes", nargs="+", choices=["openaq", "openmeteo", "trafico", "mongo"],
                    default=["openaq", "openmeteo", "trafico"])
    ap.add_argument("--colecciones", nargs="+", choices=["lectura", "evento", "alerta", "incidente"],
                    default=["lectura"])
    ap.add_argument("--fecha", default=datetime.now(timezone.utc).date().isoformat(),
                    help="Fecha del snapshot Mongo, AAAA-MM-DD")
    ap.add_argument("--sobrescribir", action="store_true")
    args = ap.parse_args()
    try:
        datetime.strptime(args.fecha, "%Y-%m-%d")
    except ValueError:
        ap.error("--fecha debe ser AAAA-MM-DD")
    fuentes = list(dict.fromkeys(args.fuentes))
    for origen in ["openaq", "openmeteo", "trafico"]:
        if origen not in fuentes:
            continue
        patron = {"openaq": "*/*.csv", "openmeteo": "*.json", "trafico": "trafico_*.csv"}[origen]
        base = RAIZ / "data" / "raw" / origen
        archivos = sorted(base.glob(patron))
        if not archivos:
            raise RuntimeError(f"No hay archivos {origen} en {base}; primero generarlos/descargarlos")
        for archivo in archivos:
            relativa = archivo.relative_to(base).as_posix()
            copiar(archivo, f"{HDFS_BASE}/{origen}/{relativa}", args.sobrescribir)
    if "mongo" in fuentes:
        for coleccion in list(dict.fromkeys(args.colecciones)):
            with tempfile.TemporaryDirectory(prefix="smartcity_mongo_export_") as tmp:
                local = Path(tmp) / f"{coleccion}.jsonl"
                exportar_mongo(coleccion, local)
                copiar(local, f"{HDFS_BASE}/mongo_exports/{coleccion}/fecha={args.fecha}/{coleccion}.jsonl",
                       args.sobrescribir)
    print("SMARTCITY_RAW_RESUMEN=OK", flush=True)


if __name__ == "__main__":
    main()
